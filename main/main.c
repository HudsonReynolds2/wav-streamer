// wav-streamer: AudioMoth USB -> HTTP chunked streamer with SD fallback and robust error handling
// ESP-IDF v5.4.3 compatible
//
// For AIs editing this file: if you are unsure about anything in this codebase or its
// dependencies (a struct layout, a field name, an SDK API signature, whether a symbol
// exists), do NOT guess. Write the grep/find command that would resolve it and hand it to
// me to run; I will paste back the output. Certainty over speed.
//
// ============================================================================
//  HARDWARE CONSTRAINT: SD CARD AND HaLow RADIO SHARE ONE SPI BUS.
//  THEY CAN NEVER BE ACTIVE AT THE SAME TIME.
// ----------------------------------------------------------------------------
//  The microSD card and the Morse HaLow module sit on the same SPI bus, so the
//  bus must be owned by exactly one of them at any instant. sd_card_mount()
//  suspends the radio (mmwlan_shutdown frees SPI2) before taking the bus, and
//  halow_resume() re-inits the radio transport after the SD card releases it.
//  Any TCP socket opened over the radio dies the moment the SD card is mounted.
//
//  This single fact dictates the whole catch-up design. We cannot read buffered
//  audio from SD and stream it to the network simultaneously. Catch-up therefore
//  runs as a strict ping-pong (see run_catchup_cycle):
//
//    Phase 1 (radio OFF, SD ON):  delete files confirmed sent last cycle, drain
//                                 the live incoming ring to fresh SD files so no
//                                 live audio is lost, then stage the oldest
//                                 buffered file into the PSRAM stage buffer.
//    Phase 2 (SD OFF, radio ON):  bring the radio up, open the stream, and push
//                                 the staged buffer out. Files are deleted only
//                                 AFTER their bytes are confirmed sent, so a
//                                 crash re-sends (in-order duplicate) but never
//                                 drops audio.
//
//  Ordering guarantee: SD filenames are a zero-padded monotonic counter
//  (NVS-backed, survives reboots), and we always stage the oldest file first, so
//  the server receives every byte exactly once, in chronological order, with no
//  gaps. Reconnect splits the server-side WAV into separate clips, which is
//  acceptable; no audio is lost across the seam.
// ============================================================================

#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include <dirent.h>
#include <sys/types.h>
#include <errno.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/queue.h"

#include "esp_log.h"
#include "esp_err.h"
#include "esp_check.h"
#include "esp_timer.h"
#include "esp_event.h"
#include "esp_system.h"
#include "esp_netif.h"
#include "esp_http_client.h"
#include "esp_heap_caps.h"

#include "mmhalow.h"   // pulls in mmwlan.h, mmosal.h, esp_netif.h, esp_wifi.h
#include "nvs_flash.h"
#include "nvs.h"

#include "usb/usb_host.h"
#include "usb/usb_types_stack.h"
#include "usb/usb_helpers.h"
#include "usb/usb_types_ch9.h"

#include "driver/gpio.h"
#include "hal/gpio_types.h"

#include <sys/unistd.h>
#include <sys/stat.h>
#include "esp_vfs_fat.h"
#include "sdmmc_cmd.h"
#include "driver/sdspi_host.h"
#include "driver/spi_common.h"

/* ========================== Configuration Defines ========================== */

#define PIN_SD_MISO 8
#define PIN_SD_MOSI 9
#define PIN_SD_CLK  7
#define PIN_SD_CS   21

#define MOUNT_POINT "/sdcard"

// Network configuration
#define SERVER_URL          "http://192.168.100.162:8000/recordings_stream"
#define LISTENER_ID         "esp32_01_outdoor_test"
#define WIFI_CONNECT_TIMEOUT_MS  10000  // 10 seconds timeout for WiFi connection

// HaLow STA credentials (set to match your Heltec H7608 AP)
#define WIFI_SSID           "HT-H7608-B81E"
#define WIFI_PSK            "blacksmith"
// Security: MMWLAN_SAE (WPA3), MMWLAN_OWE, or MMWLAN_OPEN
#define WIFI_SECURITY       MMWLAN_SAE

// Static IP configuration. The old v5.1.1 build used a static IP; the new esp_netif path
// defaults to a DHCP client, which gets no lease on this AP and leaves the interface at
// 0.0.0.0 (cause of "Host is unreachable"). Set these to the device's static addressing.
// !!! SET WIFI_STATIC_IP to the address this device used in the old build !!!
#define WIFI_STATIC_IP      "192.168.100.150"   // <-- CHANGE to the ESP's static IP
#define WIFI_STATIC_GW      "192.168.100.1"     // gateway (confirmed)
#define WIFI_STATIC_NETMASK "255.255.255.0"     // /24, adjust if your subnet differs

// Buffer configuration (easily changeable)
//
// Catch-up is link-limited: the in-phase send rate (~1.28 Mbps measured) is the ceiling, so
// the lever is amortizing the fixed per-cycle overhead (radio bring-up + SD mount/stage) over
// a larger batch. The stage buffer holds 5 MB (up to five 1 MB files), so each radio bring-up
// sends 5 MB instead of 3, raising net drain. SD files stay 1 MB so several stage together.
//
// Buffer configuration
// Rebalanced after measuring real link behavior: send rate ~311 KB/s (2.5 Mbps) when the
// link is healthy, but the HaLow link stalls intermittently. A larger incoming ring buys
// more seconds of dropout tolerance before the catch-up abort triggers; the stage shrinks
// to match so total PSRAM stays within budget. 4 MB ring + 3.5 MB stage = 7.5 MB (same as
// before), but the headroom now sits where the link needs it.
#define PSRAM_INCOMING_BUFFER_SIZE  (8 * 512 * 1024)   // 4MB incoming USB ring (~43s headroom)
#define PSRAM_STAGE_BUFFER_SIZE     (7 * 512 * 1024)   // 3.5MB catch-up stage (up to seven 512KB-equiv, ~3-4 files)
#define SD_BLOCK_SIZE               (32 * 1024)        // 32KB blocks for SD writes / frames
#define SD_MAX_WRITE_SIZE           (128 * 1024)       // Max 128KB per SD write operation

// Each buffered SD file is exactly this many bytes (except a short final file at the tail
// of a buffering burst). 1 MB keeps files small enough that several stage together into the
// 5 MB stage buffer, amortizing the per-cycle radio bring-up over more data. Must be a
// multiple of SD_BLOCK_SIZE (1MB / 32KB = 32) and of the 512-byte sector size.
#define SD_FILE_SIZE                (1 * 1024 * 1024)

// 80% of the 4 MB ring = 3.2 MB of headroom before abort. At the 96 KB/s capture rate that is
// ~34 s, comfortably longer than the ~11 s it takes to send the 3.5 MB stage at the measured
// 311 KB/s, so a healthy link clears a full stage without ever tripping the abort. The margin
// also absorbs multi-second link stalls that would previously have overflowed the ring.
#define CATCHUP_INCOMING_ABORT_NUM  80
#define CATCHUP_INCOMING_ABORT_DEN  100

// NVS-backed monotonic file counter. We reserve a block of IDs at a time and hand
// them out from RAM, so NVS sees one write per block (negligible flash wear) and a
// crash wastes at most (BLOCK-1) IDs without ever reusing one. This keeps filenames
// strictly increasing across reboots, which is what makes oldest-first replay sort
// correctly when there is no real-time clock.
#define NVS_COUNTER_NAMESPACE       "wavstream"
#define NVS_COUNTER_KEY             "filectr"
#define NVS_COUNTER_BLOCK           256
// Cached AP BSSID for fast directed reconnect (skips open scan during catch-up
// ping-pong). Stored as a 6-byte blob in the same namespace / handle as the file
// counter. Written ONLY when the live BSSID differs from the cached copy, so a stable
// deployment performs zero NVS writes after the first-ever connect.
#define NVS_BSSID_KEY               "ap_bssid"

// Timing configuration (easily changeable)
#define STREAM_RETRY_DELAY_MS       5000   // Delay between stream reconnection attempts
#define STREAM_MAX_RETRIES          2       // Number of stream reconnection attempts before SD mode
#define NETWORK_CHECK_INTERVAL_MS   20000  // Check network every 20 seconds when on SD
#define STREAM_HEALTH_CHECK_MS      10000  // Check stream health every 10 seconds

// Zero-loss stream failover: when the live stream fails, re-probe the link quickly instead
// of blocking, and abandon to SD the instant the incoming ring nears full so audio is never
// lost to overflow.
#define RECONNECT_PROBE_TIMEOUT_MS   1000  // short per-probe connect timeout (re-probe often)
#define RECONNECT_PROBE_GAP_MS        200  // brief pause between probes
// High-water on the incoming ring. Crossing it while the stream is down forces an immediate
// switch to SD. 60% of the 2.5 MB ring leaves ~1 MB (~11 s at 96 KB/s) of headroom, far more
// than one probe (<=1 s) plus the SD mount (~0.5 s), so the ring never reaches capacity.
#define STREAM_FAIL_SD_HIGHWATER_NUM   60
#define STREAM_FAIL_SD_HIGHWATER_DEN  100

// First-connect warm-up. After a fresh radio bring-up the HaLow rate controller sits at
// MCS0/1MHz and has not adapted, so the opening TCP SYN frequently times out for a second or
// two even with excellent RSSI (observed: a full connect timeout on the first attempt, then
// an instant success on the next try once the link rate-adapted). Retry the cheap TCP open a
// few times before tearing the radio back down and eating a NETWORK_CHECK_INTERVAL_MS wait.
// Reuses the live-reconnect probe timing (RECONNECT_PROBE_TIMEOUT_MS / _GAP_MS); only the
// attempt count is new. ~4 attempts at the 1 s probe timeout spans the few seconds the rate
// controller needs to climb off MCS0, while returning the instant a try connects. Each short
// attempt also re-sends SYNs, which is the very traffic that ramps the link (an idle settle
// delay would not, since rate control only adapts when frames flow).
#define STREAM_CONNECT_WARMUP_ATTEMPTS  4

// Audio data rate: 96 bytes/ms = 96KB/s = 768kbps
#define AUDIO_DATA_RATE_BPS         768000
#define AUDIO_BYTES_PER_MS          96

// USB configuration
#define ISO_MPS              96
#define ISO_PKTS_PER_URB     16
#define NUM_ISO_URBS         3

#define FRAME_HEADER_SIZE    6   // 3 bytes seq + 3 bytes length

// The server reserves sequence value 0xFFFFFF as a metadata marker and drops any
// frame carrying it. The 24-bit stream sequence must therefore skip 0xFFFFFF on
// wrap (see seq_advance) so we never silently lose a 32KB block once every ~64
// days of continuous streaming.
#define STREAM_SEQ_METADATA  0xFFFFFF
#define STREAM_SEQ_MASK      0xFFFFFF

// ---------------------------------------------------------------------------
// Diagnostic / log-output configuration (easily changeable)
// ---------------------------------------------------------------------------
// Backlog file-list verbosity. When too many files are buffered on the SD card the
// per-file printout is just noise: the IDs are a dense monotonic counter, so the list
// is almost always one long run. Set to 1 to print one line per file (old behavior);
// set to 0 to collapse each run of consecutive IDs into a single "[a..b] first ... last
// (N files)" line. Singletons and gaps still print on their own line.
#define SD_FILE_LIST_VERBOSE        0

// Include the negotiated HaLow TX rate / MCS in the link status line. This calls
// mmwlan_get_rc_stats(), which allocates a struct on the heap that must be freed. The struct
// layout has been verified against this SDK's mmwlan.h (see log_halow_link). Set to 0 to drop
// the TX-rate line and report only RSSI/state/IP.
#define HALOW_REPORT_TX_RATE        1

static const char *TAG = "wav-streamer";

static esp_err_t stream_disconnect(void);

/* ========================== Type Definitions ========================== */

typedef enum {
    MODE_STREAMING,      // Normal streaming to network
    MODE_SD_BUFFERING,   // Network down, buffering to SD
    MODE_CATCHING_UP     // Network restored, uploading SD while buffering new data
} stream_mode_t;

typedef struct {
    uint8_t *buffer;
    size_t capacity;
    size_t write_pos;
    size_t read_pos;
    size_t data_size;
    SemaphoreHandle_t mutex;
} ring_buffer_t;

typedef struct {
    char filename[256];
    int64_t timestamp;
} sd_file_info_t;

typedef struct {
    stream_mode_t mode;
    bool network_healthy;
    bool stream_healthy;
    bool sd_mounted;
    bool wifi_connected;
    int stream_retry_count;
    int64_t last_network_check_ms;
    int64_t last_stream_write_ms;
    int64_t last_catchup_pause_ms;
    uint32_t sequence_number;
    uint64_t sd_bytes_to_catch_up;  // bytes on the card not yet confirmed delivered; 64-bit
                                    // since a multi-day outage at 96 KB/s overflows 32-bit

    // SD file management
    char current_write_filename[128];
    char current_read_filename[128];
    FILE *sd_write_file;
    FILE *sd_read_file;
    size_t sd_write_file_bytes;     // bytes in the currently open write file (drives rotation)
    sd_file_info_t *sd_file_list;
    int sd_file_count;
    
    SemaphoreHandle_t state_mutex;
} system_state_t;

typedef struct {
    esp_http_client_handle_t client;
    bool is_connected;
    SemaphoreHandle_t mutex;
} streaming_context_t;

typedef struct {
    int      mps;
    uint8_t *batch_buf;
    size_t   batch_cap;
} iso_cb_ctx_t;

/* ========================== Global Variables ========================== */

// Ring buffers in PSRAM
static ring_buffer_t *g_incoming_buffer = NULL;
// Flat catch-up stage buffer (formerly the unused "outgoing" ring). Holds one whole
// SD file read off the card during the radio-off phase, then streamed out radio-on. (outdated comment)
static uint8_t *g_stage_buffer = NULL;

// Catch-up staging state. g_staged_files holds the SD files currently sitting in
// g_stage_buffer; once their bytes are confirmed sent (g_staged_uploaded), the next
// SD phase deletes them. A whole file fits the stage 1:1, but smaller tail files may
// let several fit, so this is a small list. (outdated comment)
#define CATCHUP_MAX_STAGED_FILES 8
static char   g_staged_files[CATCHUP_MAX_STAGED_FILES][128];
static int    g_staged_count = 0;
static size_t g_staged_bytes = 0;
static bool   g_staged_uploaded = false;
// Bytes of the current staged batch already confirmed sent. If a radio phase aborts early
// for ring pressure, this preserves how far we got: the next cycle drains the ring and
// re-stages (drained files always get higher counter IDs, so they sort newer and the staged
// prefix is reproduced byte-for-byte), then the radio phase resumes sending from this offset
// without re-sending or re-advancing the sequence. The server reassembles by frame sequence
// number, so a single byte offset is all that must survive an abort; file boundaries are
// irrelevant on the wire. Reset to 0 whenever a batch fully completes.
static size_t g_staged_sent_off = 0;

// System state
static system_state_t g_state = {
    .mode = MODE_STREAMING,
    .network_healthy = false,
    .stream_healthy = false,
    .sd_mounted = false,
    .wifi_connected = false,
    .stream_retry_count = 0,
    .sequence_number = 0,
    .sd_bytes_to_catch_up = 0,
    .sd_write_file = NULL,
    .sd_read_file = NULL,
    .sd_file_list = NULL,
    .sd_file_count = 0
};

// Streaming context
static streaming_context_t stream_ctx = {0};
// Connect timeout for the streaming HTTP client. Lowered transiently during the zero-loss
// failover probe loop so a dead endpoint is given up on fast and the link is re-probed often.
static int g_stream_connect_timeout_ms = 5000;

// USB variables
static usb_host_client_handle_t g_client;
static usb_device_handle_t      g_dev;
static SemaphoreHandle_t        ctrl_sem;
static usb_transfer_t          *s_iso_urbs[NUM_ISO_URBS] = {0};
static iso_cb_ctx_t            *s_iso_ctxs[NUM_ISO_URBS] = {0};

// HaLow WiFi state (mmhalow API)
static SemaphoreHandle_t        g_halow_connected_sem = NULL;
static bool                     g_halow_initialized = false;
static bool                     g_netif_started = false;
// STA args built once in halow_init_once(); reused by every halow_resume() so the
// suspend/resume cycle never re-enters the one-shot mmhalow_init().
static struct mmwlan_sta_args   g_sta_args = MMWLAN_STA_ARGS_INIT;
// RAM mirror of the BSSID persisted in NVS. g_cached_bssid_valid is true once we have a
// known-good BSSID (loaded from NVS at boot, or learned on a successful open connect).
// g_bssid_pinned tracks whether the CURRENT g_sta_args.bssid is non-zero, so halow_resume()
// knows whether a failure should clear the pin and fall back to an open scan.
static uint8_t                  g_cached_bssid[MMWLAN_MAC_ADDR_LEN] = {0};
static bool                     g_cached_bssid_valid = false;
static bool                     g_bssid_pinned = false;
static int                      g_pinned_fail_count = 0;

// SD card variables
static SemaphoreHandle_t spi_bus_mutex = NULL;
static sdmmc_card_t *sd_card = NULL;
static sdmmc_host_t sd_host = SDSPI_HOST_DEFAULT();
static sdspi_device_config_t sd_slot_config = SDSPI_DEVICE_CONFIG_DEFAULT();

// Task handles
static TaskHandle_t stream_manager_task_handle = NULL;
static TaskHandle_t network_monitor_task_handle = NULL;

// Statistics
static uint64_t g_total_bytes_received = 0;
static uint64_t g_total_bytes_sent = 0;
static uint64_t g_total_bytes_sd_written = 0;

// Lifetime fault counters (since boot). These quantify how rough the link has been:
//   g_total_stream_failures - live stream dropped and entered the reconnect-probe path
//   g_total_sd_fallbacks    - probing failed long enough that we fell back to SD buffering
//   g_total_catchup_aborts  - a catch-up radio phase was aborted early for ring pressure
static uint32_t g_total_stream_failures = 0;
static uint32_t g_total_sd_fallbacks = 0;
static uint32_t g_total_catchup_aborts = 0;

// Data-loss (incoming ring overflow) tracking, used for edge-triggered logging in isoc_in_cb
// so a sustained overflow logs once on entry and once on recovery rather than every callback.
// g_losing_data is the current state, g_lost_bytes_run accumulates bytes within the current
// loss run (reset when it ends), and g_total_bytes_lost is the lifetime total since boot.
static volatile bool     g_losing_data = false;
static volatile uint32_t g_lost_bytes_run = 0;
static uint64_t          g_total_bytes_lost = 0;

// Edge-triggered SD-buffering write-run tracking (see sd_write_run_note / sd_write_run_end).
// Replaces the per-block SD write log: one line when a buffering run begins, one when it ends.
static bool     g_sd_run_active = false;
static uint64_t g_sd_run_bytes = 0;
static int64_t  g_sd_run_start_ms = 0;

// NVS-backed monotonic file counter (see NVS_COUNTER_* defines). g_file_ctr_next is
// the next ID to hand out; g_file_ctr_block_end is the first ID we have NOT yet
// reserved from NVS. g_max_existing_ctr is the highest ID found on the card at boot,
// used to bump the counter forward if NVS was ever erased while files remained.
static nvs_handle_t g_ctr_nvs = 0;
static uint32_t     g_file_ctr_next = 0;
static uint32_t     g_file_ctr_block_end = 0;
static uint32_t     g_max_existing_ctr = 0;
static SemaphoreHandle_t g_file_ctr_mutex = NULL;

// Forward declarations
static esp_err_t sd_close_write_file(void);
static void run_catchup_cycle(uint8_t *work_buffer);
// run_catchup_cycle (in the SD section) drives the mode transitions and radio bring-up,
// which are defined later in the Mode Management / Network sections.
static esp_err_t switch_to_streaming_mode(void);
static esp_err_t switch_to_sd_mode(void);
static esp_err_t switch_to_catchup_mode(void);
static void handle_stream_failure(void);
static esp_err_t wifi_reconnect(void);

/* ========================== Ring Buffer Functions ========================== */

static void ring_buffer_destroy(ring_buffer_t *rb)
{
    if (rb) {
        if (rb->buffer) {
            heap_caps_free(rb->buffer);
        }
        if (rb->mutex) {
            vSemaphoreDelete(rb->mutex);
        }
        free(rb);
    }
}

static ring_buffer_t* ring_buffer_create(size_t capacity)
{
    ring_buffer_t *rb = (ring_buffer_t*)malloc(sizeof(ring_buffer_t));
    if (!rb) {
        ESP_LOGE(TAG, "Failed to allocate ring buffer structure");
        return NULL;
    }
    
    // Explicitly allocate in PSRAM using heap_caps_malloc
    rb->buffer = (uint8_t*)heap_caps_malloc(capacity, MALLOC_CAP_SPIRAM);
    if (!rb->buffer) {
        ESP_LOGE(TAG, "Failed to allocate %zu bytes in PSRAM", capacity);
        // Try internal RAM as fallback (though likely to fail for large sizes)
        rb->buffer = (uint8_t*)heap_caps_malloc(capacity, MALLOC_CAP_INTERNAL);
        if (!rb->buffer) {
            ESP_LOGE(TAG, "Failed to allocate %zu bytes in internal RAM either", capacity);
            free(rb);
            return NULL;
        }
        ESP_LOGW(TAG, "Allocated %zu bytes in internal RAM (fallback)", capacity);
    } else {
        ESP_LOGI(TAG, "Successfully allocated %zu bytes in PSRAM", capacity);
    }
    
    rb->capacity = capacity;
    rb->write_pos = 0;
    rb->read_pos = 0;
    rb->data_size = 0;
    rb->mutex = xSemaphoreCreateMutex();
    
    if (!rb->mutex) {
        heap_caps_free(rb->buffer);
        free(rb);
        ESP_LOGE(TAG, "Failed to create mutex for ring buffer");
        return NULL;
    }
    
    ESP_LOGI(TAG, "Created ring buffer with %zu bytes at address 0x%p", capacity, rb->buffer);
    return rb;
}

static size_t ring_buffer_write(ring_buffer_t *rb, const uint8_t *data, size_t len)
{
    if (!rb || !data || len == 0) return 0;
    
    xSemaphoreTake(rb->mutex, portMAX_DELAY);
    
    size_t space_available = rb->capacity - rb->data_size;
    size_t to_write = (len > space_available) ? space_available : len;
    
    if (to_write == 0) {
        xSemaphoreGive(rb->mutex);
        return 0;
    }
    
    // Write in up to two chunks (wrap around)
    size_t first_chunk = rb->capacity - rb->write_pos;
    if (first_chunk > to_write) first_chunk = to_write;
    
    memcpy(rb->buffer + rb->write_pos, data, first_chunk);
    
    if (to_write > first_chunk) {
        memcpy(rb->buffer, data + first_chunk, to_write - first_chunk);
    }
    
    rb->write_pos = (rb->write_pos + to_write) % rb->capacity;
    rb->data_size += to_write;
    
    xSemaphoreGive(rb->mutex);
    return to_write;
}

// Copy up to len bytes from the read position WITHOUT advancing it. Pairs with
// ring_buffer_consume: peek a block, try to send/write it, and only consume it on
// success. On failure the bytes stay at the head of the ring in their original order,
// so a retry re-sends exactly the same block. This replaces the old read-then-write-back
// pattern, which pushed a failed block back behind newer USB audio and scrambled order.
static size_t ring_buffer_peek(ring_buffer_t *rb, uint8_t *data, size_t len)
{
    if (!rb || !data || len == 0) return 0;
    
    xSemaphoreTake(rb->mutex, portMAX_DELAY);
    
    size_t to_read = (len > rb->data_size) ? rb->data_size : len;
    
    if (to_read == 0) {
        xSemaphoreGive(rb->mutex);
        return 0;
    }
    
    // Read in up to two chunks (wrap around), leaving read_pos/data_size untouched
    size_t first_chunk = rb->capacity - rb->read_pos;
    if (first_chunk > to_read) first_chunk = to_read;
    
    memcpy(data, rb->buffer + rb->read_pos, first_chunk);
    
    if (to_read > first_chunk) {
        memcpy(data + first_chunk, rb->buffer, to_read - first_chunk);
    }
    
    xSemaphoreGive(rb->mutex);
    return to_read;
}

// Advance the read pointer by len bytes, discarding them. Call only after the bytes
// returned by a preceding ring_buffer_peek have been successfully sent or written.
// Returns the number actually consumed (clamped to data_size for safety).
static size_t ring_buffer_consume(ring_buffer_t *rb, size_t len)
{
    if (!rb || len == 0) return 0;
    
    xSemaphoreTake(rb->mutex, portMAX_DELAY);
    
    size_t to_consume = (len > rb->data_size) ? rb->data_size : len;
    rb->read_pos = (rb->read_pos + to_consume) % rb->capacity;
    rb->data_size -= to_consume;
    
    xSemaphoreGive(rb->mutex);
    return to_consume;
}

static size_t ring_buffer_get_data_size(ring_buffer_t *rb)
{
    if (!rb) return 0;
    xSemaphoreTake(rb->mutex, portMAX_DELAY);
    size_t size = rb->data_size;
    xSemaphoreGive(rb->mutex);
    return size;
}

static size_t ring_buffer_get_free_space(ring_buffer_t *rb)
{
    if (!rb) return 0;
    xSemaphoreTake(rb->mutex, portMAX_DELAY);
    size_t space = rb->capacity - rb->data_size;
    xSemaphoreGive(rb->mutex);
    return space;
}

/* ========================== HTTP Streaming Functions ========================== */

static int write_chunked_data(esp_http_client_handle_t client, const uint8_t *data, size_t len)
{
    char size_str[12];
    int size_len = snprintf(size_str, sizeof(size_str), "%X\r\n", (unsigned int)len);
    if (esp_http_client_write(client, size_str, size_len) != size_len) return -1;
    if (len && esp_http_client_write(client, (const char*)data, len) != (int)len) return -1;
    if (esp_http_client_write(client, "\r\n", 2) != 2) return -1;
    return (int)len;
}

static int write_chunked_end(esp_http_client_handle_t client)
{
    return (esp_http_client_write(client, "0\r\n\r\n", 5) == 5) ? 0 : -1;
}

// Advance the 24-bit stream sequence, skipping STREAM_SEQ_METADATA (0xFFFFFF). The
// server reserves 0xFFFFFF as a metadata marker and drops any data frame carrying it,
// so a plain "& 0xFFFFFF" wrap would silently lose one 32KB block roughly every 64 days
// of continuous streaming and then split the file at the following zero. Skipping it
// lands the sequence on 0 at wrap, which is exactly what the server's expected_seq remap
// anticipates: a clean file split, no lost block.
static inline uint32_t seq_advance(uint32_t seq)
{
    seq = (seq + 1) & STREAM_SEQ_MASK;
    if (seq == STREAM_SEQ_METADATA) {
        seq = 0;
    }
    return seq;
}

static esp_err_t stream_connect(void)
{
    if (stream_ctx.client != NULL) {
        ESP_LOGW(TAG, "Stream client already exists - disconnecting first");
        stream_disconnect();
    }

    stream_ctx.mutex = xSemaphoreCreateMutex();
    if (!stream_ctx.mutex) {
        ESP_LOGE(TAG, "Failed to create stream mutex");
        return ESP_ERR_NO_MEM;
    }

    char url[256];
    snprintf(url, sizeof(url), "%s?listener_id=%s", SERVER_URL, LISTENER_ID);

    esp_http_client_config_t config = {
        .url = url,
        .method = HTTP_METHOD_POST,
        .timeout_ms = g_stream_connect_timeout_ms,
        .buffer_size = 1024,
        .buffer_size_tx = 1024,
    };

    stream_ctx.client = esp_http_client_init(&config);
    if (!stream_ctx.client) {
        ESP_LOGE(TAG, "Failed to create HTTP client");
        vSemaphoreDelete(stream_ctx.mutex);
        stream_ctx.mutex = NULL;
        return ESP_FAIL;
    }

    esp_http_client_set_header(stream_ctx.client, "Transfer-Encoding", "chunked");
    esp_http_client_set_header(stream_ctx.client, "Connection", "keep-alive");

    ESP_LOGI(TAG, "Opening streaming connection...");
    esp_err_t err = esp_http_client_open(stream_ctx.client, -1);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed to open connection: %s", esp_err_to_name(err));
        esp_http_client_cleanup(stream_ctx.client);
        stream_ctx.client = NULL;
        vSemaphoreDelete(stream_ctx.mutex);
        stream_ctx.mutex = NULL;
        return err;
    }

    stream_ctx.is_connected = true;
    
    // Reset sequence number on new connection
    g_state.sequence_number = 0;
    
    // Initialize stream health timestamp
    g_state.last_stream_write_ms = esp_timer_get_time() / 1000;
    
    ESP_LOGI(TAG, "Streaming connection established");
    return ESP_OK;
}

// Open the stream with a few quick retries, for use right after a radio bring-up where the
// link has not yet rate-adapted. Mirrors the probe loop in handle_stream_failure() but for
// the cold-link connect rather than the live-stream reconnect, and borrows the same probe
// timing. Returns ESP_OK on the first attempt that connects, or the last error after
// exhausting the attempts. stream_connect() itself stays single-shot so handle_stream_failure()
// (which has its own ring-pressure-gated loop) does not nest retries.
static esp_err_t stream_connect_resilient(void)
{
    const int saved_timeout = g_stream_connect_timeout_ms;
    g_stream_connect_timeout_ms = RECONNECT_PROBE_TIMEOUT_MS;

    esp_err_t err = ESP_FAIL;
    for (int attempt = 1; attempt <= STREAM_CONNECT_WARMUP_ATTEMPTS; attempt++) {
        err = stream_connect();
        if (err == ESP_OK) {
            if (attempt > 1) {
                ESP_LOGI(TAG, "Stream connect succeeded on attempt %d/%d",
                         attempt, STREAM_CONNECT_WARMUP_ATTEMPTS);
            }
            break;
        }
        if (attempt < STREAM_CONNECT_WARMUP_ATTEMPTS) {
            ESP_LOGW(TAG, "Stream connect attempt %d/%d failed (%s); link likely still ramping, retrying",
                     attempt, STREAM_CONNECT_WARMUP_ATTEMPTS, esp_err_to_name(err));
            vTaskDelay(pdMS_TO_TICKS(RECONNECT_PROBE_GAP_MS));
        }
    }

    g_stream_connect_timeout_ms = saved_timeout;
    return err;
}

static esp_err_t stream_send_with_header(const uint8_t *data, size_t data_len, uint32_t seq)
{
    if (!stream_ctx.is_connected) return ESP_FAIL;
    if (xSemaphoreTake(stream_ctx.mutex, pdMS_TO_TICKS(100)) != pdTRUE) return ESP_ERR_TIMEOUT;

    // Prepare frame with header
    uint8_t *frame = malloc(FRAME_HEADER_SIZE + data_len);
    if (!frame) {
        xSemaphoreGive(stream_ctx.mutex);
        return ESP_ERR_NO_MEM;
    }
    
    // Add header
    frame[0] = seq & 0xFF;
    frame[1] = (seq >> 8) & 0xFF;
    frame[2] = (seq >> 16) & 0xFF;
    frame[3] = data_len & 0xFF;
    frame[4] = (data_len >> 8) & 0xFF;
    frame[5] = (data_len >> 16) & 0xFF;
    
    // Copy data
    memcpy(frame + FRAME_HEADER_SIZE, data, data_len);
    
    int result = write_chunked_data(stream_ctx.client, frame, FRAME_HEADER_SIZE + data_len);
    free(frame);
    
    if (result < 0) {
        stream_ctx.is_connected = false;
        xSemaphoreGive(stream_ctx.mutex);
        
        ESP_LOGW(TAG, "Stream write failed");
        return ESP_FAIL;
    }

    xSemaphoreGive(stream_ctx.mutex);
    return ESP_OK;
}

static esp_err_t stream_disconnect(void)
{
    if (!stream_ctx.client) return ESP_OK;

    ESP_LOGI(TAG, "Closing stream...");

    if (stream_ctx.mutex && xSemaphoreTake(stream_ctx.mutex, pdMS_TO_TICKS(5000)) == pdTRUE) {
        if (stream_ctx.is_connected) {
            write_chunked_end(stream_ctx.client);
        }
        
        esp_http_client_close(stream_ctx.client);
        esp_http_client_cleanup(stream_ctx.client);
        stream_ctx.client = NULL;
        stream_ctx.is_connected = false;
        
        xSemaphoreGive(stream_ctx.mutex);
        vSemaphoreDelete(stream_ctx.mutex);
        stream_ctx.mutex = NULL;
    }
    
    return ESP_OK;
}

/* ========================== SD Card Functions ========================== */

static const spi_bus_config_t spi_bus_cfg = {
    .mosi_io_num = PIN_SD_MOSI,
    .miso_io_num = PIN_SD_MISO,
    .sclk_io_num = PIN_SD_CLK,
    .quadwp_io_num = -1,
    .quadhd_io_num = -1,
    .max_transfer_sz = 4000,
    .flags = 0,
    .intr_flags = ESP_INTR_FLAG_IRAM
};

static const esp_vfs_fat_sdmmc_mount_config_t sd_mount_config = {
    .format_if_mount_failed = false,
    .max_files = 5,
    .allocation_unit_size = 16 * 1024
};

static void force_halow_cs_high(void)
{
    ESP_LOGI(TAG, "Setting HaLow CS (GPIO_NUM_4) HIGH to prevent bus conflicts");
    gpio_set_direction(GPIO_NUM_4, GPIO_MODE_OUTPUT);
    gpio_set_level(GPIO_NUM_4, 1);
}

// Compare function for sorting files by timestamp
static int compare_files_by_timestamp(const void *a, const void *b)
{
    const sd_file_info_t *file_a = (const sd_file_info_t *)a;
    const sd_file_info_t *file_b = (const sd_file_info_t *)b;
    
    if (file_a->timestamp < file_b->timestamp) return -1;
    if (file_a->timestamp > file_b->timestamp) return 1;
    return 0;
}

// Buffered files are named XXXXXXXX.bin: exactly eight hex digits from the uint32
// counter plus the .bin extension, which is exactly 8.3 (no long-filename support on
// this FATFS build). Zero-padding makes lexical order equal numeric order, so the
// directory scan can sort chronologically without a real-time clock. is_backlog_name
// validates that an 8.3 entry is one of ours (8 hex chars + .bin), so stray files like
// the startup test.txt are ignored. parse_backlog_id reads the counter back out.
static bool is_backlog_name(const char *name, uint32_t *id_out)
{
    // Expect exactly "XXXXXXXX.bin": 8 hex digits, a dot, then bin.
    if (strlen(name) != 12) return false;
    if (name[8] != '.' ||
        (name[9] != 'b' && name[9] != 'B') ||
        (name[10] != 'i' && name[10] != 'I') ||
        (name[11] != 'n' && name[11] != 'N')) {
        return false;
    }
    uint32_t id = 0;
    for (int i = 0; i < 8; i++) {
        char ch = name[i];
        uint32_t nyb;
        if      (ch >= '0' && ch <= '9') nyb = (uint32_t)(ch - '0');
        else if (ch >= 'a' && ch <= 'f') nyb = (uint32_t)(ch - 'a' + 10);
        else if (ch >= 'A' && ch <= 'F') nyb = (uint32_t)(ch - 'A' + 10);
        else return false;
        id = (id << 4) | nyb;
    }
    if (id_out) *id_out = id;
    return true;
}

// NVS-backed monotonic counter with block reservation. On boot we read the stored
// high-water mark and immediately reserve a block of NVS_COUNTER_BLOCK IDs by writing
// (highwater + BLOCK) back once, then hand out IDs from RAM until the block is exhausted
// and reserve again. NVS therefore sees one write per block (negligible flash wear), and
// a crash wastes at most (BLOCK-1) unused IDs but never reuses one, so files always sort
// after older pending files. g_max_existing_ctr (the highest ID found on the card at
// boot) bumps the start forward if NVS was ever erased while files remained.
static esp_err_t file_counter_init(void)
{
    g_file_ctr_mutex = xSemaphoreCreateMutex();
    if (!g_file_ctr_mutex) {
        ESP_LOGE(TAG, "Failed to create file counter mutex");
        return ESP_ERR_NO_MEM;
    }

    esp_err_t err = nvs_open(NVS_COUNTER_NAMESPACE, NVS_READWRITE, &g_ctr_nvs);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "nvs_open(%s) failed: %s", NVS_COUNTER_NAMESPACE, esp_err_to_name(err));
        return err;
    }

    uint32_t highwater = 0;
    err = nvs_get_u32(g_ctr_nvs, NVS_COUNTER_KEY, &highwater);
    if (err == ESP_ERR_NVS_NOT_FOUND) {
        highwater = 0;          // first ever boot
    } else if (err != ESP_OK) {
        ESP_LOGE(TAG, "nvs_get_u32 failed: %s", esp_err_to_name(err));
        return err;
    }

    // If files already on the card number at or above the stored high-water (e.g. NVS was
    // erased), start above them so new files still sort last.
    if (g_max_existing_ctr + 1 > highwater) {
        highwater = g_max_existing_ctr + 1;
    }

    // Reserve a block: persist highwater + BLOCK now, hand out [highwater, highwater+BLOCK).
    uint32_t new_highwater = highwater + NVS_COUNTER_BLOCK;
    err = nvs_set_u32(g_ctr_nvs, NVS_COUNTER_KEY, new_highwater);
    if (err == ESP_OK) err = nvs_commit(g_ctr_nvs);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "Reserving counter block failed: %s", esp_err_to_name(err));
        return err;
    }

    g_file_ctr_next = highwater;
    g_file_ctr_block_end = new_highwater;
    ESP_LOGI(TAG, "File counter initialized: next=%08" PRIx32 ", block_end=%08" PRIx32,
             g_file_ctr_next, g_file_ctr_block_end);
    return ESP_OK;
}

// Hand out the next monotonic file ID, reserving a fresh NVS block when the current one
// is exhausted. Safe to call from any task (guarded by g_file_ctr_mutex).
static esp_err_t file_counter_next(uint32_t *id_out)
{
    if (!g_file_ctr_mutex || !id_out) return ESP_FAIL;
    xSemaphoreTake(g_file_ctr_mutex, portMAX_DELAY);

    if (g_file_ctr_next >= g_file_ctr_block_end) {
        uint32_t new_highwater = g_file_ctr_block_end + NVS_COUNTER_BLOCK;
        esp_err_t err = nvs_set_u32(g_ctr_nvs, NVS_COUNTER_KEY, new_highwater);
        if (err == ESP_OK) err = nvs_commit(g_ctr_nvs);
        if (err != ESP_OK) {
            xSemaphoreGive(g_file_ctr_mutex);
            ESP_LOGE(TAG, "Reserving next counter block failed: %s", esp_err_to_name(err));
            return err;
        }
        g_file_ctr_block_end = new_highwater;
    }

    *id_out = g_file_ctr_next++;
    xSemaphoreGive(g_file_ctr_mutex);
    return ESP_OK;
}

// Load the cached AP BSSID from NVS into g_cached_bssid. Called once after the counter
// NVS handle is open. Absence of the key (first ever boot) is not an error; we simply
// leave g_cached_bssid_valid false and the first connect runs an open scan.
static void bssid_cache_load(void)
{
    if (!g_ctr_nvs) {
        return;
    }
    size_t len = MMWLAN_MAC_ADDR_LEN;
    esp_err_t err = nvs_get_blob(g_ctr_nvs, NVS_BSSID_KEY, g_cached_bssid, &len);
    if (err == ESP_OK && len == MMWLAN_MAC_ADDR_LEN) {
        g_cached_bssid_valid = true;
        ESP_LOGI(TAG, "Loaded cached BSSID %02x:%02x:%02x:%02x:%02x:%02x",
                 g_cached_bssid[0], g_cached_bssid[1], g_cached_bssid[2],
                 g_cached_bssid[3], g_cached_bssid[4], g_cached_bssid[5]);
    } else if (err == ESP_ERR_NVS_NOT_FOUND) {
        ESP_LOGI(TAG, "No cached BSSID yet; first connect will scan");
    } else {
        ESP_LOGW(TAG, "BSSID cache load failed (%s); will scan", esp_err_to_name(err));
    }
}

// Persist a freshly observed BSSID to NVS, but ONLY if it differs from the cached copy.
// A stable AP therefore costs zero writes for the life of the deployment. Updates the RAM
// mirror unconditionally so g_sta_args can be pinned from it next resume.
static void bssid_cache_store(const uint8_t *bssid)
{
    if (g_cached_bssid_valid && memcmp(g_cached_bssid, bssid, MMWLAN_MAC_ADDR_LEN) == 0) {
        return;   // unchanged: no flash write
    }
    memcpy(g_cached_bssid, bssid, MMWLAN_MAC_ADDR_LEN);
    g_cached_bssid_valid = true;
    if (!g_ctr_nvs) {
        return;
    }
    esp_err_t err = nvs_set_blob(g_ctr_nvs, NVS_BSSID_KEY, g_cached_bssid, MMWLAN_MAC_ADDR_LEN);
    if (err == ESP_OK) err = nvs_commit(g_ctr_nvs);
    if (err == ESP_OK) {
        ESP_LOGI(TAG, "Cached new BSSID %02x:%02x:%02x:%02x:%02x:%02x",
                 bssid[0], bssid[1], bssid[2], bssid[3], bssid[4], bssid[5]);
    } else {
        ESP_LOGW(TAG, "BSSID cache store failed: %s", esp_err_to_name(err));
    }
}

// Drop a stale cached BSSID after a directed connect has repeatedly failed (e.g. the AP
// moved or the device relocated). Clears RAM mirror, the live pin, and the NVS key so a
// cold reboot does not keep chasing a dead BSSID.
static void bssid_cache_invalidate(void)
{
    g_cached_bssid_valid = false;
    memset(g_cached_bssid, 0, MMWLAN_MAC_ADDR_LEN);
    memset(g_sta_args.bssid, 0, MMWLAN_MAC_ADDR_LEN);
    g_bssid_pinned = false;
    if (g_ctr_nvs) {
        esp_err_t err = nvs_erase_key(g_ctr_nvs, NVS_BSSID_KEY);
        if (err == ESP_OK) {
            nvs_commit(g_ctr_nvs);
            ESP_LOGW(TAG, "Invalidated cached BSSID; reverting to open scan");
        } else if (err != ESP_ERR_NVS_NOT_FOUND) {
            ESP_LOGW(TAG, "BSSID cache erase failed: %s", esp_err_to_name(err));
        }
    }
}


static void backlog_path_from_id(uint32_t id, char *out, size_t out_sz)
{
    snprintf(out, out_sz, MOUNT_POINT "/%08" PRIx32 ".bin", id);
}

// Scan SD card for buffered files
static esp_err_t sd_scan_buffered_files(void)
{
    DIR *dir;
    struct dirent *entry;
    struct stat file_stat;
    char full_path[512];  // Increased buffer size to 512 bytes
    
    // Check if SD is mounted
    if (!g_state.sd_mounted) {
        ESP_LOGW(TAG, "SD card not mounted, cannot scan for files");
        return ESP_FAIL;
    }
    
    // Free previous list if exists
    if (g_state.sd_file_list) {
        free(g_state.sd_file_list);
        g_state.sd_file_list = NULL;
    }
    g_state.sd_file_count = 0;
    
    dir = opendir(MOUNT_POINT);
    if (dir == NULL) {
        ESP_LOGE(TAG, "Failed to open SD card directory");
        return ESP_FAIL;
    }
    
    // Count backlog files (XXXXXXXX.bin) and track the highest ID seen, which seeds the
    // NVS counter on first boot / after an NVS erase so new files still sort last.
    int file_count = 0;
    uint32_t id;
    while ((entry = readdir(dir)) != NULL) {
        if (is_backlog_name(entry->d_name, &id)) {
            file_count++;
            if (id > g_max_existing_ctr) g_max_existing_ctr = id;
        }
    }
    
    if (file_count == 0) {
        closedir(dir);
        // Authoritative: no backlog files on the card means zero bytes to catch up. This also
        // corrects the counter if pre-existing files were cleared outside the live accounting.
        g_state.sd_bytes_to_catch_up = 0;
        ESP_LOGI(TAG, "No buffered audio files found on SD card");
        return ESP_OK;
    }
    
    ESP_LOGI(TAG, "Found %d buffered audio files on SD card", file_count);
    
    // Allocate list
    g_state.sd_file_list = (sd_file_info_t *)malloc(sizeof(sd_file_info_t) * file_count);
    if (!g_state.sd_file_list) {
        closedir(dir);
        ESP_LOGE(TAG, "Failed to allocate memory for file list");
        return ESP_ERR_NO_MEM;
    }
    
    // Rewind directory and populate list
    rewinddir(dir);
    int index = 0;
    uint64_t total_backlog_bytes = 0;   // sum of on-card backlog file sizes (authoritative)
    while ((entry = readdir(dir)) != NULL && index < file_count) {
        if (is_backlog_name(entry->d_name, &id)) {
            int ret = snprintf(full_path, sizeof(full_path), "%s/%s", MOUNT_POINT, entry->d_name);
            if (ret >= sizeof(full_path)) {
                ESP_LOGW(TAG, "Path truncated, skipping file: %s", entry->d_name);
                continue;
            }
            
            if (stat(full_path, &file_stat) == 0) {
                strncpy(g_state.sd_file_list[index].filename, full_path, sizeof(g_state.sd_file_list[index].filename) - 1);
                g_state.sd_file_list[index].filename[sizeof(g_state.sd_file_list[index].filename) - 1] = '\0';  // Ensure null termination
                
                // The 8-hex-digit base is the monotonic counter; it sorts chronologically.
                g_state.sd_file_list[index].timestamp = (int64_t)id;
                
                total_backlog_bytes += (uint64_t)file_stat.st_size;
                index++;
            }
        }
    }
    
    closedir(dir);
    g_state.sd_file_count = index;

    // The scan is the single source of truth for the backlog size: set the catch-up byte total
    // to the exact sum of on-card backlog file sizes. This fixes the boot case (pre-existing
    // files written by a prior run were never in the live += accounting) and prevents any drift
    // between the incremental write/delete bookkeeping and what is actually on the card.
    g_state.sd_bytes_to_catch_up = total_backlog_bytes;
    
    // Sort files by timestamp
    qsort(g_state.sd_file_list, g_state.sd_file_count, sizeof(sd_file_info_t), compare_files_by_timestamp);
    
    ESP_LOGI(TAG, "Sorted %d buffered files for upload", g_state.sd_file_count);
#if SD_FILE_LIST_VERBOSE
    // Verbose: one line per file (original behavior).
    for (int i = 0; i < g_state.sd_file_count; i++) {
        ESP_LOGI(TAG, "  [%d] %s (timestamp: %lld)", i, g_state.sd_file_list[i].filename, g_state.sd_file_list[i].timestamp);
    }
#else
    // Compact: collapse runs of consecutive IDs (the monotonic counter, stored in .timestamp)
    // into a single line per run. A run breaks whenever the next ID is not exactly one greater
    // than the previous. Singletons print as a plain "[i]" line; runs print first ... last.
    // The list is already sorted ascending by timestamp at this point.
    int i = 0;
    while (i < g_state.sd_file_count) {
        int j = i;
        // Extend the run while IDs stay strictly consecutive.
        while (j + 1 < g_state.sd_file_count &&
               g_state.sd_file_list[j + 1].timestamp == g_state.sd_file_list[j].timestamp + 1) {
            j++;
        }
        if (j == i) {
            ESP_LOGI(TAG, "  [%d] %s (id: %lld)",
                     i, g_state.sd_file_list[i].filename, g_state.sd_file_list[i].timestamp);
        } else {
            ESP_LOGI(TAG, "  [%d..%d] %s ... %s (%d files, ids %lld..%lld)",
                     i, j,
                     g_state.sd_file_list[i].filename, g_state.sd_file_list[j].filename,
                     j - i + 1,
                     g_state.sd_file_list[i].timestamp, g_state.sd_file_list[j].timestamp);
        }
        i = j + 1;
    }
#endif
    
    return ESP_OK;
}

static esp_err_t sd_card_mount(void)
{
    esp_err_t ret;
    
    if (g_state.sd_mounted) {
        return ESP_OK;
    }
    
    ESP_LOGI(TAG, "Mounting SD card...");
    
    // Suspend HaLow to release the shared SPI bus for the SD card.
    // We must NOT call mmhalow_deinit()/mmhalow_init() per cycle: mmhalow_init() asserts
    // halow_netif == NULL (it is one-shot, created once in halow_init_once()). Instead we
    // power the radio down here and bring it back with mmwlan_sta_enable() in halow_resume().
    // mmwlan_shutdown() runs the shim's mmhal_wlan_deinit(), which does spi_bus_remove_device()
    // + spi_bus_free(SPI2_HOST), fully releasing the bus. The netif and rx/link callbacks
    // registered in mmhalow_init() survive this and are reused on resume.
    ESP_LOGI(TAG, "Suspending HaLow for SD access...");
    // Tear down any open HTTP stream first. Suspending the radio (mmwlan_shutdown) kills the
    // underlying socket, so leaving stream_ctx.client open would orphan it: the next
    // stream_connect() would see a stale client ("already exists") and the server would log a
    // connection abort. Closing it here keeps each radio phase's connect clean.
    stream_disconnect();
    if (g_halow_initialized) {
        mmwlan_sta_disable();   // disconnect STA
        mmwlan_shutdown();      // power down radio + free SPI2 (via shim mmhal_wlan_deinit)
    }
    g_state.wifi_connected = false;
    vTaskDelay(pdMS_TO_TICKS(200));
    
    force_halow_cs_high();
    
    // Initialize SPI bus
    spi_bus_free(sd_host.slot);
    ret = spi_bus_initialize(sd_host.slot, &spi_bus_cfg, SPI_DMA_CH_AUTO);
    if (ret != ESP_OK && ret != ESP_ERR_INVALID_STATE) {
        ESP_LOGE(TAG, "Failed to initialize SPI bus: %s", esp_err_to_name(ret));
        return ret;
    }
    
    // Mount SD card
    sd_slot_config.gpio_cs = PIN_SD_CS;
    sd_slot_config.host_id = sd_host.slot;
    
    ret = esp_vfs_fat_sdspi_mount(MOUNT_POINT, &sd_host, &sd_slot_config, 
                                   &sd_mount_config, &sd_card);
    if (ret == ESP_OK) {
        g_state.sd_mounted = true;
        ESP_LOGI(TAG, "SD card mounted successfully");
        
        // Scan for existing buffered files
        sd_scan_buffered_files();
    } else {
        ESP_LOGE(TAG, "Failed to mount SD card: %s", esp_err_to_name(ret));
        spi_bus_free(sd_host.slot);
    }
    
    return ret;
}

static esp_err_t sd_card_unmount(void)
{
    if (!g_state.sd_mounted) {
        return ESP_OK;
    }
    
    ESP_LOGI(TAG, "Unmounting SD card...");
    
    // Close any open files - CRITICAL to do this before unmounting
    if (g_state.sd_write_file) {
        fclose(g_state.sd_write_file);
        g_state.sd_write_file = NULL;
        ESP_LOGI(TAG, "Closed write file");
    }
    if (g_state.sd_read_file) {
        fclose(g_state.sd_read_file);
        g_state.sd_read_file = NULL;
        ESP_LOGI(TAG, "Closed read file");
    }
    
    // Clear filename buffers
    memset(g_state.current_write_filename, 0, sizeof(g_state.current_write_filename));
    memset(g_state.current_read_filename, 0, sizeof(g_state.current_read_filename));
    
    esp_err_t ret = esp_vfs_fat_sdcard_unmount(MOUNT_POINT, sd_card);
    if (ret == ESP_OK) {
        g_state.sd_mounted = false;
        sd_card = NULL;
        ESP_LOGI(TAG, "SD card unmounted");
    } else {
        ESP_LOGW(TAG, "Failed to unmount SD card: %s", esp_err_to_name(ret));
    }
    
    // Free SPI bus
    spi_bus_free(sd_host.slot);
    
    return ret;
}

// Close the current write file (if any) and clear the per-file byte counter. Defined
// here (forward-declared at top) so both the buffering path and catch-up can rotate or
// finalize the open file through one place. Returns ESP_OK even if no file was open.
static esp_err_t sd_close_write_file(void)
{
    if (g_state.sd_write_file) {
        fclose(g_state.sd_write_file);
        g_state.sd_write_file = NULL;
    }
    g_state.sd_write_file_bytes = 0;
    return ESP_OK;
}

static esp_err_t sd_write_audio_data(const uint8_t *data, size_t len)
{
    if (!g_state.sd_mounted || !data || len == 0) {
        return ESP_FAIL;
    }
    
    // Open a new counter-named file if none is open. Files are XXXXXXXX.bin (8.3, eight
    // hex digits from the monotonic NVS counter), so they sort chronologically and a
    // whole file fits the stage buffer 1:1.
    if (!g_state.sd_write_file) {
        uint32_t id;
        if (file_counter_next(&id) != ESP_OK) {
            ESP_LOGE(TAG, "Could not obtain next file counter");
            return ESP_FAIL;
        }
        backlog_path_from_id(id, g_state.current_write_filename, sizeof(g_state.current_write_filename));
        
        g_state.sd_write_file = fopen(g_state.current_write_filename, "wb");
        if (!g_state.sd_write_file) {
            ESP_LOGE(TAG, "Failed to open SD file for writing: %s", g_state.current_write_filename);
            ESP_LOGE(TAG, "errno: %d (%s)", errno, strerror(errno));
            return ESP_FAIL;
        }
        g_state.sd_write_file_bytes = 0;
        ESP_LOGI(TAG, "Opened SD file for writing: %s", g_state.current_write_filename);
    }
    
    // Write data in chunks up to SD_MAX_WRITE_SIZE
    size_t written = 0;
    while (written < len) {
        size_t chunk_size = (len - written) > SD_MAX_WRITE_SIZE ? SD_MAX_WRITE_SIZE : (len - written);
        size_t result = fwrite(data + written, 1, chunk_size, g_state.sd_write_file);
        if (result != chunk_size) {
            ESP_LOGE(TAG, "SD write failed: wrote %zu of %zu bytes, errno: %d (%s)", 
                     result, chunk_size, errno, strerror(errno));
            // Close the failed file
            sd_close_write_file();
            return ESP_FAIL;
        }
        written += result;
    }
    
    // Flush to ensure data is written
    if (fflush(g_state.sd_write_file) != 0) {
        ESP_LOGE(TAG, "SD flush failed: errno: %d (%s)", errno, strerror(errno));
        // Close the failed file
        sd_close_write_file();
        return ESP_FAIL;
    }
    
    // Track bytes of the currently-open (not yet scannable) file for mid-burst responsiveness.
    // sd_scan_buffered_files() resets this total to the sum of closed files on its next run, so
    // this increment only ever represents the in-progress file and never double-counts.
    g_state.sd_bytes_to_catch_up += written;
    g_total_bytes_sd_written += written;
    g_state.sd_write_file_bytes += written;
    
    // Rotate at SD_FILE_SIZE so each file fits the catch-up stage buffer 1:1. Callers
    // feed SD_BLOCK_SIZE-aligned blocks and SD_FILE_SIZE is a multiple of SD_BLOCK_SIZE,
    // so files land exactly on the cap (a short final file only occurs when buffering
    // stops mid-file, which the stage buffer still accommodates).
    if (g_state.sd_write_file_bytes >= SD_FILE_SIZE) {
        ESP_LOGI(TAG, "Rotating SD file at %d KB: %s",
                 SD_FILE_SIZE / 1024, g_state.current_write_filename);
        sd_close_write_file();
    }
    
    return ESP_OK;
}

// Delete the files staged-and-confirmed in the previous radio phase. Called at the start
// of an SD phase (bus owned by SD). Clears the staging record afterward. This is the
// deferred half of delete-on-confirmed-send: a file's bytes are known to have reached the
// server before we remove it, so a crash mid-cycle re-sends an in-order duplicate but
// never drops audio.
static void catchup_delete_confirmed(void)
{
    if (!g_staged_uploaded || g_staged_count == 0) {
        return;
    }
    // Note on sd_bytes_to_catch_up: no adjustment here. catchup_sd_phase() always rescans
    // immediately after this delete pass, and sd_scan_buffered_files() recomputes the backlog
    // total authoritatively from the files actually left on the card.
#if SD_FILE_LIST_VERBOSE
    // Verbose: one line per deleted file (original behavior).
    for (int i = 0; i < g_staged_count; i++) {
        ESP_LOGI(TAG, "Deleting confirmed-sent file: %s", g_staged_files[i]);
        unlink(g_staged_files[i]);
    }
#else
    // Compact: collapse consecutive IDs into "[a..b] first ... last (N files)" lines, mirroring
    // the backlog listing. g_staged_files is built oldest-first from the sorted scan, so it is
    // already in ascending ID order. Parse each name's monotonic ID (from the basename) to find
    // the runs; unlink within each run so deletion and logging stay together.
    int i = 0;
    while (i < g_staged_count) {
        // Resolve the ID of run-start file i.
        const char *base_i = strrchr(g_staged_files[i], '/');
        base_i = base_i ? base_i + 1 : g_staged_files[i];
        uint32_t id_i = 0;
        bool have_i = is_backlog_name(base_i, &id_i);

        int j = i;
        // Extend the run while the next file's ID is exactly one greater. If any name fails to
        // parse, we cannot prove contiguity, so the run stops there.
        while (have_i && j + 1 < g_staged_count) {
            const char *base_n = strrchr(g_staged_files[j + 1], '/');
            base_n = base_n ? base_n + 1 : g_staged_files[j + 1];
            uint32_t id_n = 0;
            if (!is_backlog_name(base_n, &id_n)) break;
            // id of file j is id_i + (j - i); next must be one past that.
            if (id_n != id_i + (uint32_t)(j + 1 - i)) break;
            j++;
        }

        if (j == i) {
            ESP_LOGI(TAG, "Deleting confirmed-sent file: %s", g_staged_files[i]);
        } else {
            ESP_LOGI(TAG, "Deleting confirmed-sent files [%d..%d]: %s ... %s (%d files)",
                     i, j, g_staged_files[i], g_staged_files[j], j - i + 1);
        }
        // Unlink every file in this run.
        for (int k = i; k <= j; k++) {
            unlink(g_staged_files[k]);
        }
        i = j + 1;
    }
#endif
    g_staged_count = 0;
    g_staged_bytes = 0;
    g_staged_uploaded = false;
}

// SD phase of a catch-up cycle (radio OFF, SD ON). Returns the sd_file_list index of the
// last file FULLY staged this cycle, or -1 if nothing was staged (backlog empty; caller
// then hands off to live streaming). The bus must be owned by SD on entry (caller mounts).
//   1. delete files confirmed sent last cycle
//   2. drain the live incoming ring to fresh counter-named SD files (preserve live audio
//      captured during the previous radio phase, in order)
//   3. rescan oldest-first
//   4. stage whole oldest files into g_stage_buffer up to its cap
// Returning the last fully-staged index (rather than a count) lets the caller detect "we
// staged through the final backlog file" robustly even if an earlier file was skipped.
static int catchup_sd_phase(uint8_t *work_buffer)
{
    // 1. Delete last cycle's confirmed files.
    catchup_delete_confirmed();

    // 2. Drain the incoming ring to SD so nothing captured during the last radio phase is
    //    lost. Whole blocks first; a final short block flushes the remainder.
    size_t avail = ring_buffer_get_data_size(g_incoming_buffer);
    while (avail >= SD_BLOCK_SIZE) {
        size_t got = ring_buffer_peek(g_incoming_buffer, work_buffer, SD_BLOCK_SIZE);
        if (got == 0) break;
        if (sd_write_audio_data(work_buffer, got) != ESP_OK) {
            ESP_LOGE(TAG, "SD write failed draining ring during catch-up");
            break;
        }
        ring_buffer_consume(g_incoming_buffer, got);
        avail = ring_buffer_get_data_size(g_incoming_buffer);
    }
    if (avail > 0) {
        size_t got = ring_buffer_peek(g_incoming_buffer, work_buffer, avail);
        if (got > 0 && sd_write_audio_data(work_buffer, got) == ESP_OK) {
            ring_buffer_consume(g_incoming_buffer, got);
        }
    }
    // Close the write file so the drained data is a complete, scannable backlog file.
    sd_close_write_file();

    // 3. Rescan oldest-first (includes whatever we just drained).
    sd_scan_buffered_files();
    if (g_state.sd_file_count == 0) {
        return -1;   // nothing to catch up
    }

    // 4. Stage whole oldest files into the flat stage buffer, up to its cap. A whole file
    //    fits 1:1, but short tail files may let several fit; remember each so we can delete
    //    them once their bytes are confirmed sent.
    g_staged_count = 0;
    g_staged_bytes = 0;
    g_staged_uploaded = false;
    int last_full_idx = -1;

    for (int idx = 0; idx < g_state.sd_file_count && g_staged_count < CATCHUP_MAX_STAGED_FILES; idx++) {
        const char *path = g_state.sd_file_list[idx].filename;
        struct stat st;
        if (stat(path, &st) != 0) {
            ESP_LOGW(TAG, "Cannot stat staged candidate %s, skipping", path);
            continue;
        }
        size_t fsize = (size_t)st.st_size;
        // Stop if this file would not fit alongside what is already staged. Always stage at
        // least one file even if (pathologically) it exceeds the cap, reading only what fits.
        if (g_staged_bytes > 0 && (g_staged_bytes + fsize) > PSRAM_STAGE_BUFFER_SIZE) {
            break;
        }
        size_t room = PSRAM_STAGE_BUFFER_SIZE - g_staged_bytes;
        size_t to_read = (fsize > room) ? room : fsize;

        FILE *f = fopen(path, "rb");
        if (!f) {
            ESP_LOGE(TAG, "Failed to open %s for staging", path);
            continue;
        }
        size_t rd = fread(g_stage_buffer + g_staged_bytes, 1, to_read, f);
        fclose(f);
        if (rd == 0) {
            continue;
        }
        g_staged_bytes += rd;
        strncpy(g_staged_files[g_staged_count], path, sizeof(g_staged_files[0]) - 1);
        g_staged_files[g_staged_count][sizeof(g_staged_files[0]) - 1] = '\0';
        g_staged_count++;

        // Only a fully-read file counts toward "staged through this index". A truncated
        // read (file larger than remaining room) means there is more of it still to send,
        // so it must not be treated as completing the backlog.
        if (rd == fsize) {
            last_full_idx = idx;
        }

        if (g_staged_bytes >= PSRAM_STAGE_BUFFER_SIZE) {
            break;   // stage full
        }
    }

    ESP_LOGI(TAG, "Staged %d file(s), %zu bytes for upload", g_staged_count, g_staged_bytes);
    return (g_staged_bytes > 0) ? last_full_idx : -1;
}

// Radio phase of a catch-up cycle (SD OFF, radio ON). The staged buffer is pushed out as
// SD_BLOCK_SIZE frames, resuming from g_staged_sent_off (nonzero only after a prior abort).
// The bus is owned by the radio (caller brings it up), so the incoming USB ring is NOT being
// drained during this phase; it fills at 96 KB/s. Between frames we watch the ring and, if it
// climbs past CATCHUP_INCOMING_ABORT_NUM/DEN of capacity, abort and return ESP_ERR_TIMEOUT so
// the caller flips back to an SD phase to drain it before isoc_in_cb drops live audio. The
// confirmed offset is saved so the next cycle resumes rather than re-sending. Returns:
//   ESP_OK          - whole staged buffer confirmed sent (marked for deletion)
//   ESP_ERR_TIMEOUT - aborted early for ring pressure; progress saved, stay in catch-up
//   ESP_FAIL        - send error; staged files intact, fall back to SD buffering
static esp_err_t catchup_radio_phase(void)
{
    const size_t abort_threshold =
        (size_t)((uint64_t)PSRAM_INCOMING_BUFFER_SIZE * CATCHUP_INCOMING_ABORT_NUM
                 / CATCHUP_INCOMING_ABORT_DEN);

    // Resume from wherever a prior abort left off. The re-staged prefix is identical (drained
    // files sort newer), so this offset still points at the next unsent byte.
    size_t off = (g_staged_sent_off < g_staged_bytes) ? g_staged_sent_off : 0;

    while (off < g_staged_bytes) {
        // If the incoming ring is filling while we hold the bus, stop sending and let the
        // caller drain it to SD. Bytes already sent stay confirmed (saved in g_staged_sent_off
        // and not re-sent), so nothing is lost or duplicated on the wire; the sequence number
        // is not rewound.
        if (ring_buffer_get_data_size(g_incoming_buffer) >= abort_threshold) {
            ESP_LOGW(TAG, "Incoming ring above %d%%, aborting catch-up send at %zu/%zu to drain",
                     (CATCHUP_INCOMING_ABORT_NUM * 100) / CATCHUP_INCOMING_ABORT_DEN,
                     off, g_staged_bytes);
            g_staged_sent_off = off;
            return ESP_ERR_TIMEOUT;
        }

        size_t chunk = g_staged_bytes - off;
        if (chunk > SD_BLOCK_SIZE) chunk = SD_BLOCK_SIZE;

        esp_err_t err = stream_send_with_header(g_stage_buffer + off, chunk, g_state.sequence_number);
        if (err != ESP_OK) {
            ESP_LOGE(TAG, "Catch-up send failed at offset %zu/%zu", off, g_staged_bytes);
            g_staged_sent_off = off;   // preserve progress; SD-buffering fallback re-stages
            return ESP_FAIL;
        }
        g_state.sequence_number = seq_advance(g_state.sequence_number);
        g_state.last_stream_write_ms = esp_timer_get_time() / 1000;
        g_total_bytes_sent += chunk;
        off += chunk;
    }

    // Whole staged buffer confirmed sent: mark for deletion at the next SD phase, reset the
    // resume offset for the next batch.
    g_staged_uploaded = true;
    g_staged_sent_off = 0;
    return ESP_OK;
}

// One full catch-up cycle, a strict SD<->radio ping-pong (SD and radio share one SPI bus
// and can never be active together). Owns the bus end to end and chooses the next mode:
//   - send phase fails           -> fall back to SD buffering (audio preserved on card)
//   - backlog fully drained      -> hand off to live streaming
//   - more backlog remains       -> stay in CATCHING_UP for another cycle
// state_mutex is held only around the short state mutations, not across the long blocking
// mount / radio-resume / network I/O, matching the locking granularity used elsewhere.
static void run_catchup_cycle(uint8_t *work_buffer)
{
    // ---- SD phase: bus to SD, stage the oldest backlog ----
    if (sd_card_mount() != ESP_OK) {
        ESP_LOGE(TAG, "Catch-up: SD mount failed, falling back to SD buffering");
        switch_to_sd_mode();
        return;
    }

    int last_staged_idx = catchup_sd_phase(work_buffer);

    if (last_staged_idx < 0) {
        // Backlog drained. Release the bus, bring the radio up, and go live.
        ESP_LOGI(TAG, "Catch-up complete, no backlog remains; handing off to streaming");
        sd_card_unmount();
        if (switch_to_streaming_mode() != ESP_OK) {
            ESP_LOGW(TAG, "Handoff to streaming failed; falling back to SD buffering");
            switch_to_sd_mode();
        }
        return;
    }

    // Whether this batch reached the last backlog file we scanned. Captured now, before the
    // radio phase, because nothing changes sd_file_count until the next SD-phase rescan.
    bool staged_through_end = (last_staged_idx == g_state.sd_file_count - 1);

    // ---- Radio phase: bus to radio, push the staged buffer ----
    sd_card_unmount();
    if (wifi_reconnect() != ESP_OK || stream_connect_resilient() != ESP_OK) {
        ESP_LOGW(TAG, "Catch-up: radio/endpoint unavailable, falling back to SD buffering");
        switch_to_sd_mode();
        return;
    }

    esp_err_t sent = catchup_radio_phase();
    if (sent == ESP_ERR_TIMEOUT) {
        // Aborted early because the incoming ring was filling. The staged batch was NOT
        // marked sent (g_staged_uploaded stays false), so the next cycle's SD phase will
        // drain the ring and re-stage the same files; nothing is lost or double-deleted.
        // Stay in catch-up.
        ESP_LOGI(TAG, "Catch-up send aborted for ring pressure; cycling to drain incoming");
        g_total_catchup_aborts++;
        return;
    }
    if (sent != ESP_OK) {
        // Send failed mid-batch. Nothing was deleted; fall back to buffering and retry later.
        switch_to_sd_mode();
        return;
    }

    // Batch confirmed sent. If it reached the last backlog file, this is the final batch:
    // delete it now (delete-on-confirmed-send for the final batch) and go live. Any audio
    // captured during this radio phase still sits in the incoming ring; the next SD-phase
    // rescan picks it up, so we only truly go live if the rescan finds nothing.
    if (staged_through_end) {
        ESP_LOGI(TAG, "Final backlog batch confirmed sent; deleting and checking for new audio");
        if (sd_card_mount() == ESP_OK) {
            catchup_delete_confirmed();
            sd_close_write_file();
            sd_scan_buffered_files();   // refresh count before unmounting
        }
        bool drained = (g_state.sd_file_count == 0);
        sd_card_unmount();
        if (drained && switch_to_streaming_mode() == ESP_OK) {
            return;
        }
        // New audio was buffered while we sent, or handoff failed: another cycle.
        switch_to_catchup_mode();
        return;
    }

    // More backlog remains: stay in catch-up for another ping-pong cycle. The confirmed
    // batch is deleted at the start of the next cycle's SD phase.
    ESP_LOGI(TAG, "Backlog remains beyond staged batch, continuing catch-up");
}

/* ========================== Network Management ========================== */

// STA status callback: give the semaphore when the link comes up
static void halow_sta_status_cb(enum mmwlan_sta_state sta_state)
{
    switch (sta_state) {
        case MMWLAN_STA_DISABLED:
            ESP_LOGI(TAG, "HaLow STA disabled");
            break;
        case MMWLAN_STA_CONNECTING:
            ESP_LOGI(TAG, "HaLow STA connecting");
            break;
        case MMWLAN_STA_CONNECTED:
            ESP_LOGI(TAG, "HaLow STA connected");
            {
                // Learn the BSSID we actually associated with and persist it if changed.
                // This is the only place the cache is updated; bssid_cache_store writes to
                // NVS only on an actual change, so a stable AP never re-writes flash.
                uint8_t bssid[MMWLAN_MAC_ADDR_LEN];
                if (mmwlan_get_bssid(bssid) == MMWLAN_SUCCESS) {
                    bssid_cache_store(bssid);
                }
            }
            if (g_halow_connected_sem) {
                xSemaphoreGive(g_halow_connected_sem);
            }
            break;
    }
}

// Apply static IP to the HaLow STA netif. The Morse component creates its netif with
// ESP_NETIF_DEFAULT_WIFI_STA(), whose ifkey is "WIFI_STA_DEF". By default that netif runs
// a DHCP client; on this AP there is no DHCP server, so we stop the client and set the
// static address the old build used. This is applied once after mmhalow_init(); the netif
// survives the suspend/resume cycle so it does not need re-applying each connect.
static esp_err_t halow_set_static_ip(void)
{
    esp_netif_t *nif = esp_netif_get_handle_from_ifkey("WIFI_STA_DEF");
    if (!nif) {
        ESP_LOGE(TAG, "Could not find HaLow STA netif (WIFI_STA_DEF)");
        return ESP_FAIL;
    }

    // Stop the DHCP client before assigning a static address (ignore "already stopped").
    esp_err_t err = esp_netif_dhcpc_stop(nif);
    if (err != ESP_OK && err != ESP_ERR_ESP_NETIF_DHCP_ALREADY_STOPPED) {
        ESP_LOGW(TAG, "dhcpc_stop: %s", esp_err_to_name(err));
    }

    esp_netif_ip_info_t ip = {0};
    ip.ip.addr      = esp_ip4addr_aton(WIFI_STATIC_IP);
    ip.gw.addr      = esp_ip4addr_aton(WIFI_STATIC_GW);
    ip.netmask.addr = esp_ip4addr_aton(WIFI_STATIC_NETMASK);

    ESP_RETURN_ON_ERROR(esp_netif_set_ip_info(nif, &ip), TAG, "set_ip_info failed");

    ESP_LOGI(TAG, "Static IP set: %s gw %s mask %s",
             WIFI_STATIC_IP, WIFI_STATIC_GW, WIFI_STATIC_NETMASK);
    return ESP_OK;
}

// One-time HaLow bring-up for the whole program lifetime. mmhalow_init() creates the
// esp_netif, registers the rx/link-state callbacks, runs mmwlan_init(), sets the channel
// list, and boots the chip once. It asserts halow_netif == NULL internally, so it must
// never be called twice. The suspend/resume cycle (mmwlan_shutdown / mmwlan_sta_enable)
// does NOT re-enter this function; the netif and callbacks set up here are reused.
static esp_err_t halow_init_once(void)
{
    if (g_halow_initialized) {
        return ESP_OK;
    }

    // Truly-once: netif + default event loop (calling esp_netif_init twice can fault)
    if (!g_netif_started) {
        ESP_ERROR_CHECK(esp_netif_init());
        esp_err_t eret = esp_event_loop_create_default();
        if (eret != ESP_OK && eret != ESP_ERR_INVALID_STATE) {
            ESP_ERROR_CHECK(eret);
        }
        g_netif_started = true;
    }

    // One-shot: driver init, netif creation, callback registration, channel list, boot
    ESP_RETURN_ON_ERROR(mmhalow_init(NULL), TAG, "mmhalow_init failed");
    mmhalow_print_version_info();

    // Assign the static IP (no DHCP server on this AP). Done once; survives reconnects.
    ESP_RETURN_ON_ERROR(halow_set_static_ip(), TAG, "static IP config failed");

    // Build the STA args once. These are stored in g_sta_args and reused on every resume.
    memcpy(g_sta_args.ssid, WIFI_SSID, strlen(WIFI_SSID));
    g_sta_args.ssid_len = strlen(WIFI_SSID);
    memcpy(g_sta_args.passphrase, WIFI_PSK, strlen(WIFI_PSK));
    g_sta_args.passphrase_len = strlen(WIFI_PSK);
    g_sta_args.security_type = WIFI_SECURITY;

    // Reduce the driver-internal connect-scan dwell to the SDK floor. With a pinned BSSID
    // on a strong, fixed link this trims scan time off every catch-up resume; the open-scan
    // fallback (see halow_resume) covers the rare case a short dwell misses the AP.
    struct mmwlan_scan_config scan_cfg = MMWLAN_SCAN_CONFIG_INIT;
    scan_cfg.dwell_time_ms = MMWLAN_SCAN_MIN_DWELL_TIME_MS;
    enum mmwlan_status scfg = mmwlan_set_scan_config(&scan_cfg);
    if (scfg != MMWLAN_SUCCESS) {
        ESP_LOGW(TAG, "mmwlan_set_scan_config failed: %d (using default dwell)", scfg);
    }

    // Also push the config into the driver/wrapper (keeps mmhalow_get_config consistent).
    mmhalow_wifi_config_t conf = { .sta = g_sta_args };
    ESP_RETURN_ON_ERROR(mmhalow_set_config(WIFI_IF_STA, &conf), TAG, "mmhalow_set_config failed");

    if (!g_halow_connected_sem) {
        g_halow_connected_sem = xSemaphoreCreateBinary();
    }

    g_halow_initialized = true;
    return ESP_OK;
}

// Format a byte count into a fixed caller-supplied buffer, auto-scaling the unit so the
// numeric part always stays under 1000 (e.g. 1000 -> "1.0 KB", 1048576 -> "1.0 MB"). Raw
// bytes print with no decimal; scaled units print one decimal place. Returns buf so the
// call can be used inline as a printf %s argument. The buffer is caller-owned because a
// single log line often needs two formatted values at once, which a shared static could
// not provide. buf should be >= 16 bytes.
static const char *human_bytes(uint64_t bytes, char *buf, size_t buf_sz)
{
    static const char *units[] = { "B", "KB", "MB", "GB", "TB", "PB" };
    int unit = 0;
    double val = (double)bytes;
    // Step up a unit while the magnitude is >= 1000 so the most significant group is < 1000.
    while (val >= 1000.0 && unit < (int)(sizeof(units) / sizeof(units[0])) - 1) {
        val /= 1024.0;
        unit++;
    }
    if (unit == 0) {
        snprintf(buf, buf_sz, "%llu %s", (unsigned long long)bytes, units[0]);
    } else {
        snprintf(buf, buf_sz, "%.1f %s", val, units[unit]);
    }
    return buf;
}

// Map an RSSI in dBm to a short qualitative label. Thresholds are typical for sub-GHz
// HaLow links: stronger (less negative) is better; below about -90 dBm the rate control
// is forced to the lowest MCS and goodput collapses.
static const char *rssi_quality(int32_t rssi)
{
    if (rssi >= -55) return "excellent";
    if (rssi >= -67) return "good";
    if (rssi >= -78) return "fair";
    if (rssi >= -90) return "weak";
    return "poor";
}

// Log the HaLow link status. RSSI (dBm) is the cheapest, most diagnostic number: a low RSSI
// forces the rate-control algorithm onto a low MCS, which caps goodput and is the likely
// cause if catch-up throughput is far below the PHY ceiling. We also report the STA state,
// the current IP (if the netif is up), and, when HALOW_REPORT_TX_RATE is set, the negotiated
// TX rate / MCS from mmwlan_get_rc_stats(). Only meaningful while the STA is connected, so
// the whole line is suppressed when the radio is suspended (SD phase).
static void log_halow_link(const char *context)
{
    if (mmwlan_get_sta_state() != MMWLAN_STA_CONNECTED) {
        return;
    }
    int32_t rssi = mmwlan_get_rssi();

    // IP, if the netif is up (static IP, so present as soon as the link is up).
    char ipbuf[20] = "0.0.0.0";
    esp_netif_t *nif = esp_netif_get_handle_from_ifkey("WIFI_STA_DEF");
    esp_netif_ip_info_t ip = {0};
    if (nif && esp_netif_is_netif_up(nif) &&
        esp_netif_get_ip_info(nif, &ip) == ESP_OK && ip.ip.addr != 0) {
        snprintf(ipbuf, sizeof(ipbuf), IPSTR, IP2STR(&ip.ip));
    }

    ESP_LOGI(TAG, "HaLow link [%s]: CONNECTED, RSSI %ld dBm (%s), IP %s",
             context, (long)rssi, rssi_quality(rssi), ipbuf);

#if HALOW_REPORT_TX_RATE
    // Negotiated TX rate / MCS. mmwlan_get_rc_stats() returns a heap struct that must be freed
    // with mmwlan_free_rc_stats(). Its layout (verified against mmwlan.h): n_entries plus three
    // parallel arrays indexed 0..n_entries-1: rate_info[], total_sent[], total_success[].
    // rate_info is a packed bitfield: bits 0-3 = bandwidth (0=1MHz,1=2MHz,2=4MHz), bits 4-7 =
    // MCS rate, bit 8 = guard interval (0=long,1=short). We report the entry currently carrying
    // the most traffic (max total_sent), which is the rate rate-control has settled on, along
    // with its success rate. (This is the SDK-specific block; toggle HALOW_REPORT_TX_RATE off
    // if the struct ever changes.)
    struct mmwlan_rc_stats *rc = mmwlan_get_rc_stats();
    if (rc != NULL) {
        if (rc->n_entries > 0 && rc->rate_info && rc->total_sent && rc->total_success) {
            // Pick the most-used rate entry (highest total_sent).
            uint32_t best = 0;
            for (uint32_t i = 1; i < rc->n_entries; i++) {
                if (rc->total_sent[i] > rc->total_sent[best]) {
                    best = i;
                }
            }
            uint32_t ri = rc->rate_info[best];
            // Field widths from the mmwlan.h bitfield diagram: BW occupies bits 0-3 but only
            // uses values 0-2 (1/2/4 MHz), Rate (MCS) is bits 4-7, Guard is bit 8. Mask each
            // to its own width so adjacent fields never leak in. BW masked to 2 bits is enough
            // for 0-2; MCS masked to 4 bits.
            uint32_t bw_field = (ri >> MMWLAN_RC_STATS_RATE_INFO_BW_OFFSET)    & 0x3;
            uint32_t mcs      = (ri >> MMWLAN_RC_STATS_RATE_INFO_RATE_OFFSET)  & 0xF;
            uint32_t sgi      = (ri >> MMWLAN_RC_STATS_RATE_INFO_GUARD_OFFSET) & 0x1;
            // BW codes: 0=1MHz, 1=2MHz, 2=4MHz, 3=8MHz. The header enum only documents 0-2,
            // but the MM8108 (chip 0x0306) adds 8 MHz, which reports as code 3. Anything else is
            // surfaced raw below.
            const char *bw_str = (bw_field == 0) ? "1MHz" :
                                 (bw_field == 1) ? "2MHz" :
                                 (bw_field == 2) ? "4MHz" :
                                 (bw_field == 3) ? "8MHz" : NULL;
            uint32_t sent = rc->total_sent[best];
            uint32_t succ = rc->total_success[best];
            unsigned succ_pct = (sent > 0) ? (unsigned)((uint64_t)succ * 100 / sent) : 0;
            if (bw_str) {
                ESP_LOGI(TAG, "HaLow rate [%s]: MCS %lu, %s, %s GI, success %u%% (%lu/%lu pkts)",
                         context,
                         (unsigned long)mcs, bw_str, sgi ? "short" : "long",
                         succ_pct, (unsigned long)succ, (unsigned long)sent);
            } else {
                // BW code outside the documented 0-2 range; surface it raw rather than hide it.
                ESP_LOGI(TAG, "HaLow rate [%s]: MCS %lu, BW code %lu, %s GI, success %u%% (%lu/%lu pkts)",
                         context,
                         (unsigned long)mcs, (unsigned long)bw_field, sgi ? "short" : "long",
                         succ_pct, (unsigned long)succ, (unsigned long)sent);
            }
        }
        mmwlan_free_rc_stats(rc);
    }
#endif
}

// Re-boot the radio and (re)connect after a suspend. mmwlan_sta_enable() auto-boots the
// chip if powered down (re-initializing the SPI transport via the shim) and initiates the
// association. Link-up arrives asynchronously via halow_sta_status_cb. No netif recreation.
static esp_err_t halow_resume(void)
{
    // Drain any stale connect signal before issuing a fresh connect
    xSemaphoreTake(g_halow_connected_sem, 0);

    // If we have a known-good BSSID, pin it so mmwlan_sta_enable connects directed instead
    // of accepting any AP found in the scan. The scan still runs internally, but a pinned
    // BSSID lets it settle on the right AP fast. Cleared below if the directed connect fails.
    if (g_cached_bssid_valid) {
        memcpy(g_sta_args.bssid, g_cached_bssid, MMWLAN_MAC_ADDR_LEN);
        g_bssid_pinned = true;
    } else {
        memset(g_sta_args.bssid, 0, MMWLAN_MAC_ADDR_LEN);
        g_bssid_pinned = false;
    }

    int64_t t_enable_start = esp_timer_get_time();
    enum mmwlan_status st = mmwlan_sta_enable(&g_sta_args, halow_sta_status_cb);
    if (st != MMWLAN_SUCCESS) {
        ESP_LOGE(TAG, "mmwlan_sta_enable failed: %d", st);
        return ESP_FAIL;
    }

    // Wait for connected callback with timeout
    if (xSemaphoreTake(g_halow_connected_sem, pdMS_TO_TICKS(WIFI_CONNECT_TIMEOUT_MS)) != pdTRUE) {
        ESP_LOGE(TAG, "WiFi connection timeout");
        mmwlan_sta_disable();
        // A directed (pinned) connect timed out. Drop the pin so the caller's retry runs an
        // open scan, and after two consecutive pinned failures invalidate the cached BSSID
        // entirely (AP likely moved/changed) so we stop chasing it across reboots.
        if (g_bssid_pinned) {
            g_pinned_fail_count++;
            memset(g_sta_args.bssid, 0, MMWLAN_MAC_ADDR_LEN);
            g_bssid_pinned = false;
            if (g_pinned_fail_count >= 2) {
                bssid_cache_invalidate();
                g_pinned_fail_count = 0;
            } else {
                ESP_LOGW(TAG, "Directed connect timed out; retry will use open scan");
            }
        }
        return ESP_FAIL;
    }
    ESP_LOGI(TAG, "sta_enable->connected in %lld ms (%s)",
             (esp_timer_get_time() - t_enable_start) / 1000,
             g_bssid_pinned ? "directed" : "open scan");
    g_pinned_fail_count = 0;   // any successful connect clears the directed-failure tally

    // Disable power save (these survive from morselib via mmhalow.h)
    mmwlan_set_power_save_mode(MMWLAN_PS_DISABLED);
    mmwlan_set_wnm_sleep_enabled(false);

    // With a static IP the route is ready as soon as the link is up. The Morse link-state
    // callback drives esp_netif_action_connected asynchronously, so give it a brief moment
    // to mark the netif up, then confirm/log the address before we let the stream open.
    esp_netif_t *nif = esp_netif_get_handle_from_ifkey("WIFI_STA_DEF");
    esp_netif_ip_info_t ip = {0};
    for (int i = 0; i < 20; i++) {                 // up to ~1s
        if (nif && esp_netif_is_netif_up(nif) &&
            esp_netif_get_ip_info(nif, &ip) == ESP_OK && ip.ip.addr != 0) {
            break;
        }
        vTaskDelay(pdMS_TO_TICKS(50));
    }
    if (ip.ip.addr == 0) {
        ESP_LOGW(TAG, "Netif up but no IP set; stream may be unreachable");
    } else {
        ESP_LOGI(TAG, "Netif up, IP " IPSTR " gw " IPSTR,
                 IP2STR(&ip.ip), IP2STR(&ip.gw));
    }

    g_state.wifi_connected = true;
    g_state.network_healthy = true;
    log_halow_link("connect");
    return ESP_OK;
}

static esp_err_t wifi_reconnect(void)
{
    ESP_LOGI(TAG, "Attempting WiFi reconnection...");

    // Make sure SD is unmounted first
    if (g_state.sd_mounted) {
        sd_card_unmount();
    }

    // Ensure HaLow stack is initialized (one-shot; no-op after the first call)
    if (halow_init_once() != ESP_OK) {
        ESP_LOGE(TAG, "HaLow init failed");
        return ESP_FAIL;
    }

    // Boot/reconnect the radio. Works on first connect and after every suspend.
    if (halow_resume() != ESP_OK) {
        return ESP_FAIL;
    }

    ESP_LOGI(TAG, "WiFi connected successfully");
    return ESP_OK;
}

static bool check_network_health(void)
{
    // Check WiFi connection status
    if (!g_state.wifi_connected) {
        return false;
    }
    
    // Check if we can write to stream
    if (stream_ctx.is_connected) {
        int64_t now = esp_timer_get_time() / 1000;
        int64_t time_since_last_write = now - g_state.last_stream_write_ms;
        
        // Give grace period of 30 seconds after connection, then check if stalled
        if (time_since_last_write > 30000) {  // 30 seconds grace period
            // Check if incoming buffer has data that should be streaming
            size_t incoming_data = ring_buffer_get_data_size(g_incoming_buffer);
            if (incoming_data > (PSRAM_INCOMING_BUFFER_SIZE / 4)) {  // If buffer is 25% full
                ESP_LOGW(TAG, "Stream appears stalled: %lld ms since last write, %zu bytes waiting", 
                         time_since_last_write, incoming_data);
                return false;
            }
        }
    }
    
    return true;
}

/* ========================== Mode Management ========================== */

static esp_err_t switch_to_streaming_mode(void)
{
    ESP_LOGI(TAG, "Switching to STREAMING mode");
    
    xSemaphoreTake(g_state.state_mutex, portMAX_DELAY);
    
    // Ensure WiFi is connected
    if (!g_state.wifi_connected) {
        if (wifi_reconnect() != ESP_OK) {
            xSemaphoreGive(g_state.state_mutex);
            return ESP_FAIL;
        }
    }
    
    // Connect to streaming endpoint
    if (!stream_ctx.is_connected) {
        if (stream_connect() != ESP_OK) {
            xSemaphoreGive(g_state.state_mutex);
            return ESP_FAIL;
        }
    }
    
    // Close SD write file if open (keep read file for catch-up if needed)
    if (g_state.sd_write_file) {
        fclose(g_state.sd_write_file);
        g_state.sd_write_file = NULL;
    }
    
    // Unmount SD if mounted and no files to catch up
    if (g_state.sd_mounted && g_state.sd_file_count == 0) {
        sd_card_unmount();
    }
    
    g_state.mode = MODE_STREAMING;
    g_state.stream_healthy = true;
    g_state.network_healthy = true;
    g_state.stream_retry_count = 0;
    
    // Reset health check timestamp
    g_state.last_stream_write_ms = esp_timer_get_time() / 1000;
    
    xSemaphoreGive(g_state.state_mutex);
    
    ESP_LOGI(TAG, "Switched to STREAMING mode");
    return ESP_OK;
}

static esp_err_t switch_to_sd_mode(void)
{
    ESP_LOGI(TAG, "Switching to SD_BUFFERING mode");
    
    xSemaphoreTake(g_state.state_mutex, portMAX_DELAY);
    
    // Disconnect stream if connected
    if (stream_ctx.is_connected) {
        stream_disconnect();
    }
    
    // Mount SD card
    if (sd_card_mount() != ESP_OK) {
        ESP_LOGE(TAG, "Failed to mount SD card!");
        xSemaphoreGive(g_state.state_mutex);
        return ESP_FAIL;
    }
    
    g_state.mode = MODE_SD_BUFFERING;
    g_state.last_network_check_ms = esp_timer_get_time() / 1000;
    g_state.stream_retry_count = 0;
    g_total_sd_fallbacks++;
    
    xSemaphoreGive(g_state.state_mutex);
    
    ESP_LOGI(TAG, "Switched to SD_BUFFERING mode");
    return ESP_OK;
}

// Stream just failed. Re-probe the link quickly and repeatedly while watching the incoming
// ring. The instant the ring crosses the high-water mark we stop probing and switch to SD,
// which drains the ring far faster than USB fills it. With ~11 s of headroom above the mark
// and a probe+switch costing a few seconds at most, the ring can never reach capacity, so no
// audio is lost to a network outage (assumes SD is healthy and writes faster than inflow,
// which it does by ~8x). The unsent block stays at the head of the ring and resends on resume.
static void handle_stream_failure(void)
{
    const size_t highwater =
        (size_t)((uint64_t)PSRAM_INCOMING_BUFFER_SIZE * STREAM_FAIL_SD_HIGHWATER_NUM
                 / STREAM_FAIL_SD_HIGHWATER_DEN);

    g_state.stream_healthy = false;
    g_total_stream_failures++;
    stream_disconnect();

    const int saved_timeout = g_stream_connect_timeout_ms;
    g_stream_connect_timeout_ms = RECONNECT_PROBE_TIMEOUT_MS;

    int probe = 0;
    while (1) {
        const size_t fill = ring_buffer_get_data_size(g_incoming_buffer);
        if (fill >= highwater) {
            ESP_LOGW(TAG, "Stream down, ring %zu/%zu (>=%d%%); switching to SD to avoid loss",
                     fill, (size_t)PSRAM_INCOMING_BUFFER_SIZE,
                     (STREAM_FAIL_SD_HIGHWATER_NUM * 100) / STREAM_FAIL_SD_HIGHWATER_DEN);
            break;
        }

        probe++;
        ESP_LOGI(TAG, "Stream reconnect probe %d (ring %zu/%zu)",
                 probe, fill, (size_t)PSRAM_INCOMING_BUFFER_SIZE);
        if (stream_connect() == ESP_OK) {
            ESP_LOGI(TAG, "Stream recovered after %d probe(s); resuming STREAMING", probe);
            g_state.stream_healthy = true;
            g_state.stream_retry_count = 0;
            g_stream_connect_timeout_ms = saved_timeout;
            return;  // back to the STREAMING loop; the unsent block resends
        }
        vTaskDelay(pdMS_TO_TICKS(RECONNECT_PROBE_GAP_MS));
    }

    g_stream_connect_timeout_ms = saved_timeout;
    switch_to_sd_mode();
}

static esp_err_t switch_to_catchup_mode(void)
{
    ESP_LOGI(TAG, "Switching to CATCHING_UP mode");
    
    xSemaphoreTake(g_state.state_mutex, portMAX_DELAY);
    
    // Need a backlog to catch up from; otherwise go straight to live streaming.
    if (g_state.sd_file_count == 0) {
        ESP_LOGI(TAG, "No SD data to catch up, going directly to streaming");
        xSemaphoreGive(g_state.state_mutex);
        return switch_to_streaming_mode();
    }
    
    // Just set the mode. run_catchup_cycle() owns the SD<->radio ping-pong, including all
    // mounting, staging, and bus handoff; there is nothing to pre-mount or pre-open here.
    g_state.mode = MODE_CATCHING_UP;
    g_state.last_catchup_pause_ms = esp_timer_get_time() / 1000;
    
    xSemaphoreGive(g_state.state_mutex);
    
    ESP_LOGI(TAG, "Switched to CATCHING_UP mode");
    return ESP_OK;
}

// Edge-triggered SD-buffering write run. The old per-block "Writing 32 KB to SD" line printed
// ~3x/s and buried the log. Instead we announce once when a buffering run starts and once when
// it ends, reporting how much audio landed on the card and the effective write rate, the same
// shape as the data-loss run logging. Counts only the live SD_BUFFERING path, not catch-up
// drains (which are already quiet).
static void sd_write_run_note(size_t n)
{
    if (!g_sd_run_active) {
        g_sd_run_active = true;
        g_sd_run_start_ms = esp_timer_get_time() / 1000;
        g_sd_run_bytes = 0;
        ESP_LOGI(TAG, "SD buffering: network down, writing audio to card");
    }
    g_sd_run_bytes += n;
}

static void sd_write_run_end(void)
{
    if (!g_sd_run_active) return;
    int64_t dur_ms = (esp_timer_get_time() / 1000) - g_sd_run_start_ms;
    char b1[24];
    if (dur_ms > 0) {
        double kbps = (double)g_sd_run_bytes / 1024.0 / ((double)dur_ms / 1000.0);
        ESP_LOGI(TAG, "SD buffering ended: wrote %s in %lld.%01lld s (%.0f KB/s avg)",
                 human_bytes(g_sd_run_bytes, b1, sizeof(b1)),
                 (long long)(dur_ms / 1000), (long long)((dur_ms % 1000) / 100),
                 kbps);
    } else {
        ESP_LOGI(TAG, "SD buffering ended: wrote %s",
                 human_bytes(g_sd_run_bytes, b1, sizeof(b1)));
    }
    g_sd_run_active = false;
    g_sd_run_bytes = 0;
}

/* ========================== Stream Manager Task ========================== */
static void stream_manager_task(void *arg)
{
    ESP_LOGI(TAG, "Stream manager task started");
    
    // Allocate work buffer in internal RAM for faster access
    uint8_t *work_buffer = (uint8_t*)heap_caps_malloc(SD_BLOCK_SIZE, MALLOC_CAP_INTERNAL);
    if (!work_buffer) {
        ESP_LOGE(TAG, "Failed to allocate work buffer");
        vTaskDelete(NULL);
        return;
    }
    ESP_LOGI(TAG, "Work buffer allocated: %d KB in internal RAM", SD_BLOCK_SIZE / 1024);
    
    // Boot mode was already chosen in app_main (STREAMING / CATCHING_UP / SD_BUFFERING)
    // based on whether the radio, endpoint, and backlog were present. The loop below acts
    // on g_state.mode; no override here.
    
    while (1) {
        xSemaphoreTake(g_state.state_mutex, portMAX_DELAY);
        stream_mode_t current_mode = g_state.mode;
        xSemaphoreGive(g_state.state_mutex);
        
        // Close any open SD-buffering write run when we leave SD_BUFFERING, emitting the
        // edge-triggered summary (bytes written + effective rate) in place of the old
        // per-block line.
        if (current_mode != MODE_SD_BUFFERING && g_sd_run_active) {
            sd_write_run_end();
        }

        switch (current_mode) {
            case MODE_STREAMING: {
                // Peek a block from the incoming ring and send it. The read pointer only
                // advances (ring_buffer_consume) after a confirmed send, so a failed block
                // stays at the head in order and is retried; nothing is reordered.
                size_t available = ring_buffer_get_data_size(g_incoming_buffer);
                if (available >= SD_BLOCK_SIZE) {
                    size_t bytes_read = ring_buffer_peek(g_incoming_buffer, work_buffer, SD_BLOCK_SIZE);
                    if (bytes_read > 0) {
                        esp_err_t err = stream_send_with_header(work_buffer, bytes_read, g_state.sequence_number);
                        if (err == ESP_OK) {
                            ring_buffer_consume(g_incoming_buffer, bytes_read);
                            g_state.sequence_number = seq_advance(g_state.sequence_number);
                            g_state.last_stream_write_ms = esp_timer_get_time() / 1000;
                            g_total_bytes_sent += bytes_read;
                            
                            // Reset retry count on successful send
                            g_state.stream_retry_count = 0;
                        } else {
                            // Stream failed. The unsent block stays at the head of the ring.
                            // Hand off to zero-loss failover: re-probe fast, and bail to SD the
                            // instant the ring nears full so nothing is lost to overflow.
                            ESP_LOGW(TAG, "Stream write failed; entering zero-loss failover");
                            handle_stream_failure();
                        }
                    }
                } else {
                    vTaskDelay(pdMS_TO_TICKS(10));
                }
                break;
            }
            
            case MODE_SD_BUFFERING: {
                // Peek from the incoming ring and write to SD, consuming only on a confirmed
                // write so a failed write leaves the block in the ring in order.
                size_t available = ring_buffer_get_data_size(g_incoming_buffer);
                size_t free_space = ring_buffer_get_free_space(g_incoming_buffer);
                bool did_work = false;
                
                if (available >= SD_BLOCK_SIZE) {
                    // Write full blocks
                    size_t bytes_read = ring_buffer_peek(g_incoming_buffer, work_buffer, SD_BLOCK_SIZE);
                    if (bytes_read > 0) {
                        esp_err_t err = sd_write_audio_data(work_buffer, bytes_read);
                        if (err != ESP_OK) {
                            ESP_LOGE(TAG, "SD write failed!");
                            // SD failure is critical - only try network if buffer has space.
                            // The block was NOT consumed, so it is preserved either way.
                            if (free_space > (PSRAM_INCOMING_BUFFER_SIZE / 2)) {
                                ESP_LOGW(TAG, "Attempting emergency network fallback due to SD failure");
                                
                                // Properly unmount SD before trying network
                                sd_card_unmount();
                                
                                if (wifi_reconnect() == ESP_OK && stream_connect_resilient() == ESP_OK) {
                                    switch_to_streaming_mode();
                                } else {
                                    // Network failed too, go back to SD
                                    switch_to_sd_mode();
                                }
                            } else {
                                // Buffer too full, keep trying SD
                                ESP_LOGE(TAG, "Buffer nearly full, retrying SD write");
                            }
                        } else {
                            ring_buffer_consume(g_incoming_buffer, bytes_read);
                            sd_write_run_note(bytes_read);
                            did_work = true;
                        }
                    }
                } else if (available > 0 && free_space < SD_BLOCK_SIZE) {
                    // Buffer is getting full, flush to prevent overflow
                    size_t bytes_read = ring_buffer_peek(g_incoming_buffer, work_buffer, available);
                    if (bytes_read > 0) {
                        esp_err_t err = sd_write_audio_data(work_buffer, bytes_read);
                        if (err != ESP_OK) {
                            ESP_LOGE(TAG, "Emergency flush failed!");
                            // Not consumed; block stays in the ring for the next attempt.
                        } else {
                            ring_buffer_consume(g_incoming_buffer, bytes_read);
                            sd_write_run_note(bytes_read);
                            did_work = true;
                        }
                    }
                }
                
                // Periodically probe whether the network is genuinely back. We only leave
                // SD buffering if the radio AND the HTTP endpoint both come up. If files are
                // waiting we enter catch-up (which owns the bus and ping-pongs them out); if
                // none are waiting we go straight to live streaming. A failed probe leaves us
                // buffering, with SD remounted to keep capturing audio.
                int64_t now_ms = esp_timer_get_time() / 1000;
                if ((now_ms - g_state.last_network_check_ms) > NETWORK_CHECK_INTERVAL_MS) {
                    ESP_LOGI(TAG, "Probing network from SD buffering...");
                    g_state.last_network_check_ms = now_ms;
                    
                    // Finalize and scan the backlog, then release the bus for the radio probe.
                    // We do NOT stage here: during SD buffering the incoming ring is kept near
                    // empty (it is drained to SD continuously), so there is no useful batch in
                    // RAM to send, and the real backlog lives on the card. Staging from SD on
                    // every probe would burn a multi-MB read on each failed attempt during a
                    // long outage. Instead, if the probe connects, the catch-up cycle stages
                    // from SD itself (radio down) on its first pass.
                    sd_close_write_file();
                    sd_scan_buffered_files();
                    sd_card_unmount();
                    
                    if (wifi_reconnect() == ESP_OK && stream_connect_resilient() == ESP_OK) {
                        if (g_state.sd_file_count > 0) {
                            // Network is back and there is a backlog: catch up. The radio is up
                            // now; the first catch-up cycle suspends it to stage from SD.
                            switch_to_catchup_mode();
                        } else {
                            switch_to_streaming_mode();
                        }
                    } else {
                        // Still unreachable: remount SD and keep buffering.
                        sd_card_mount();
                    }
                }
                
                // Yield to prevent watchdog
                if (did_work) {
                    vTaskDelay(pdMS_TO_TICKS(1));
                } else {
                    vTaskDelay(pdMS_TO_TICKS(10));
                }
                break;
            }
            
            case MODE_CATCHING_UP: {
                // One full ping-pong cycle: SD phase (radio off) stages the oldest file,
                // radio phase (SD off) sends it and deletes it on confirmed receipt. The
                // function owns the bus and decides the next mode itself.
                run_catchup_cycle(work_buffer);
                break;
            }
        }
    }
    
    heap_caps_free(work_buffer);
    vTaskDelete(NULL);
}

/* ========================== Network Monitor Task ========================== */

static void network_monitor_task(void *arg)
{
    ESP_LOGI(TAG, "Network monitor task started");
    
    while (1) {
        xSemaphoreTake(g_state.state_mutex, portMAX_DELAY);
        stream_mode_t current_mode = g_state.mode;
        xSemaphoreGive(g_state.state_mutex);
        
        // Skip the health check while SD owns the bus during a catch-up SD phase: the radio
        // is intentionally suspended then, so a "failure" would be spurious log noise.
        if (current_mode == MODE_STREAMING ||
            (current_mode == MODE_CATCHING_UP && !g_state.sd_mounted)) {
            bool network_ok = check_network_health();
            
            if (!network_ok) {
                ESP_LOGW(TAG, "Network health check failed");
                g_state.network_healthy = false;
                
                if (current_mode == MODE_STREAMING) {
                    // Don't immediately switch to SD, let stream_manager_task handle retries
                    g_state.stream_healthy = false;
                }
            }
        }
        
        vTaskDelay(pdMS_TO_TICKS(30000));  // 30 seconds
    }
    
    vTaskDelete(NULL);
}

/* ========================== USB Host Functions ========================== */

static void daemon_task(void *arg)
{
    while (1) {
        uint32_t flags = 0;
        esp_err_t err = usb_host_lib_handle_events(portMAX_DELAY, &flags);
        if (err != ESP_OK) {
            ESP_LOGE(TAG, "usb_host_lib_handle_events: %s", esp_err_to_name(err));
        }
    }
}

static void ctrl_cb(usb_transfer_t *xfer)
{
    xSemaphoreGiveFromISR(ctrl_sem, NULL);
}

static esp_err_t ctrl_set_interface(uint8_t intf, uint8_t alt)
{
    usb_transfer_t *xfer;
    ESP_RETURN_ON_ERROR(usb_host_transfer_alloc(sizeof(usb_setup_packet_t), 0, &xfer),
                        TAG, "alloc ctrl");

    usb_setup_packet_t *setup = (usb_setup_packet_t *)xfer->data_buffer;
    USB_SETUP_PACKET_INIT_SET_INTERFACE(setup, intf, alt);

    xfer->device_handle     = g_dev;
    xfer->bEndpointAddress  = 0;
    xfer->num_bytes         = sizeof(*setup);
    xfer->callback          = ctrl_cb;
    xfer->context           = NULL;

    while (xSemaphoreTake(ctrl_sem, 0) == pdTRUE) { }
    ESP_RETURN_ON_ERROR(usb_host_transfer_submit_control(g_client, xfer), TAG, "submit ctrl");

    while (xSemaphoreTake(ctrl_sem, 10 / portTICK_PERIOD_MS) != pdTRUE) {
        usb_host_client_handle_events(g_client, 10 / portTICK_PERIOD_MS);
    }

    esp_err_t ret = ESP_OK;
    if (xfer->status != USB_TRANSFER_STATUS_COMPLETED) {
        ESP_LOGE(TAG, "SET_INTERFACE failed, status=%d", xfer->status);
        ret = ESP_FAIL;
    }
    usb_host_transfer_free(xfer);
    return ret;
}

static void isoc_in_cb(usb_transfer_t *t)
{
    iso_cb_ctx_t *c = (iso_cb_ctx_t*)t->context;
    const int mps = c->mps;

    size_t off = 0;
    size_t out = 0;

    for (int i = 0; i < t->num_isoc_packets; i++) {
        const usb_isoc_packet_desc_t *d = &t->isoc_packet_desc[i];
        if (d->status == USB_TRANSFER_STATUS_COMPLETED && d->actual_num_bytes) {
            const uint8_t *pcm_bytes = (const uint8_t *)(t->data_buffer + off);
            
            if ((out + d->actual_num_bytes) <= c->batch_cap) {
                memcpy(c->batch_buf + out, pcm_bytes, d->actual_num_bytes);
                out += d->actual_num_bytes;
            }
        }
        off += mps;
    }

    // Write to incoming ring buffer
    if (out > 0) {
        size_t written = ring_buffer_write(g_incoming_buffer, c->batch_buf, out);
        g_total_bytes_received += written;

        // Edge-triggered loss reporting. This callback fires continuously, so logging every
        // dropped batch floods the console during a sustained overflow. Instead we log once
        // when loss begins ("Losing data!") and once when it ends (with the total dropped),
        // staying silent for the steady-state in between. g_losing_data and g_lost_bytes_run
        // are file-scope (declared near the stats globals) and only touched here.
        if (written < out) {
            size_t dropped = out - written;
            g_lost_bytes_run += dropped;
            g_total_bytes_lost += dropped;
            if (!g_losing_data) {
                g_losing_data = true;
                ESP_LOGW(TAG, "Losing data! Incoming ring full (audio is being dropped)");
            }
        } else if (g_losing_data) {
            // First clean write after a loss run: report the run total once, then go quiet.
            ESP_LOGW(TAG, "Data loss ended: dropped %llu bytes during that overflow",
                     (unsigned long long)g_lost_bytes_run);
            g_losing_data = false;
            g_lost_bytes_run = 0;
        }
    }

    // Re-submit URB
    esp_err_t err = usb_host_transfer_submit(t);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "ISO resubmit failed: %s", esp_err_to_name(err));
        usb_host_transfer_free(t);
    }
}

static esp_err_t start_isoc_stream(uint8_t ep_addr, int mps)
{
    size_t buf_size = mps * ISO_PKTS_PER_URB;

    for (int u = 0; u < NUM_ISO_URBS; u++) {
        usb_transfer_t *xfer;
        ESP_RETURN_ON_ERROR(usb_host_transfer_alloc(buf_size, ISO_PKTS_PER_URB, &xfer),
                            TAG, "alloc iso");
        s_iso_urbs[u] = xfer;

        iso_cb_ctx_t *ctx = (iso_cb_ctx_t*)calloc(1, sizeof(*ctx));
        if (!ctx) return ESP_ERR_NO_MEM;
        ctx->mps = mps;
        ctx->batch_cap = buf_size;
        // Allocate batch buffer in internal RAM for faster USB processing
        ctx->batch_buf = (uint8_t*)heap_caps_malloc(buf_size, MALLOC_CAP_INTERNAL);
        if (!ctx->batch_buf) {
            free(ctx);
            return ESP_ERR_NO_MEM;
        }
        s_iso_ctxs[u] = ctx;

        xfer->device_handle    = g_dev;
        xfer->bEndpointAddress = ep_addr;
        xfer->callback         = isoc_in_cb;
        xfer->context          = ctx;
        xfer->num_bytes        = buf_size;

        for (int i = 0; i < ISO_PKTS_PER_URB; i++) {
            xfer->isoc_packet_desc[i].num_bytes = mps;
        }
    }

    for (int u = 0; u < NUM_ISO_URBS; u++) {
        esp_err_t err = usb_host_transfer_submit(s_iso_urbs[u]);
        if (err != ESP_OK) {
            ESP_LOGE(TAG, "submit iso urb %d failed: %s", u, esp_err_to_name(err));
            return err;
        }
    }
    return ESP_OK;
}

static void client_event_cb(const usb_host_client_event_msg_t *event_msg, void *arg)
{
    switch (event_msg->event) {
    case USB_HOST_CLIENT_EVENT_NEW_DEV: {
        ESP_LOGI(TAG, "NEW_DEV addr=%d", event_msg->new_dev.address);
        ESP_ERROR_CHECK(usb_host_device_open(g_client, event_msg->new_dev.address, &g_dev));
        ESP_ERROR_CHECK(usb_host_interface_claim(g_client, g_dev, 1, 1));
        ESP_ERROR_CHECK(ctrl_set_interface(1, 1));

        esp_err_t err = start_isoc_stream(0x82, ISO_MPS);
        if (err != ESP_OK) {
            ESP_LOGE(TAG, "start_isoc_stream failed: %s", esp_err_to_name(err));
        }
        break;
    }
    case USB_HOST_CLIENT_EVENT_DEV_GONE:
        ESP_LOGW(TAG, "DEV_GONE - USB device disconnected");
        // Clean up ISO URBs and contexts
        for (int u = 0; u < NUM_ISO_URBS; u++) {
            if (s_iso_ctxs[u]) {
                if (s_iso_ctxs[u]->batch_buf) {
                    heap_caps_free(s_iso_ctxs[u]->batch_buf);
                }
                free(s_iso_ctxs[u]);
                s_iso_ctxs[u] = NULL;
            }
            s_iso_urbs[u] = NULL;  // URBs are freed by USB host
        }
        g_dev = NULL;
        break;
    default:
        break;
    }
}

static void client_task(void *arg)
{
    const usb_host_client_config_t cfg = {
        .is_synchronous = false,
        .max_num_event_msg = 16,
        .async = {
            .client_event_callback = client_event_cb,
            .callback_arg = NULL,
        },
    };
    ESP_ERROR_CHECK(usb_host_client_register(&cfg, &g_client));

    while (1) {
        usb_host_client_handle_events(g_client, portMAX_DELAY);
    }
}

/* ========================== Initialization Functions ========================== */

static void print_psram_info(void)
{
    ESP_LOGI(TAG, "=== PSRAM Memory Information ===");
    
    size_t psram_total = heap_caps_get_total_size(MALLOC_CAP_SPIRAM);
    size_t psram_free = heap_caps_get_free_size(MALLOC_CAP_SPIRAM);
    size_t psram_largest = heap_caps_get_largest_free_block(MALLOC_CAP_SPIRAM);
    
    if (psram_total > 0) {
        ESP_LOGI(TAG, "PSRAM is available!");
        ESP_LOGI(TAG, "Total PSRAM: %zu bytes (%.2f MB)", psram_total, psram_total / (1024.0 * 1024.0));
        ESP_LOGI(TAG, "Free PSRAM: %zu bytes (%.2f MB)", psram_free, psram_free / (1024.0 * 1024.0));
        ESP_LOGI(TAG, "Largest free block: %zu bytes (%.2f MB)", psram_largest, psram_largest / (1024.0 * 1024.0));
        ESP_LOGI(TAG, "PSRAM usage: %.1f%%", ((psram_total - psram_free) * 100.0) / psram_total);
    } else {
        ESP_LOGE(TAG, "No PSRAM detected!");
    }
    
    size_t internal_free = heap_caps_get_free_size(MALLOC_CAP_INTERNAL);
    size_t internal_total = heap_caps_get_total_size(MALLOC_CAP_INTERNAL);
    ESP_LOGI(TAG, "Internal RAM - Free: %zu bytes (%.2f KB), Total: %zu bytes (%.2f KB)", 
             internal_free, internal_free / 1024.0, internal_total, internal_total / 1024.0);
    
    ESP_LOGI(TAG, "=== End PSRAM Info ===");
}

static esp_err_t initialize_psram_buffers(void)
{
    ESP_LOGI(TAG, "Initializing PSRAM buffers...");
    
    // Check PSRAM availability
    size_t psram_size = heap_caps_get_total_size(MALLOC_CAP_SPIRAM);
    size_t psram_free = heap_caps_get_free_size(MALLOC_CAP_SPIRAM);
    size_t psram_largest = heap_caps_get_largest_free_block(MALLOC_CAP_SPIRAM);
    
    ESP_LOGI(TAG, "=== PSRAM Status ===");
    ESP_LOGI(TAG, "Total PSRAM: %zu KB", psram_size / 1024);
    ESP_LOGI(TAG, "Free PSRAM: %zu KB", psram_free / 1024);
    ESP_LOGI(TAG, "Largest free block: %zu KB", psram_largest / 1024);
    
    if (psram_size == 0) {
        ESP_LOGE(TAG, "No PSRAM detected! Cannot continue with audio streaming");
        return ESP_ERR_NO_MEM;
    }
    
    // Check if we have enough PSRAM for our buffers
    size_t total_needed = PSRAM_INCOMING_BUFFER_SIZE + PSRAM_STAGE_BUFFER_SIZE;
    if (psram_free < total_needed) {
        ESP_LOGE(TAG, "Not enough PSRAM! Need %zu KB, have %zu KB free", 
                 total_needed / 1024, psram_free / 1024);
        return ESP_ERR_NO_MEM;
    }
    
    // Create incoming USB ring buffer
    g_incoming_buffer = ring_buffer_create(PSRAM_INCOMING_BUFFER_SIZE);
    if (!g_incoming_buffer) {
        ESP_LOGE(TAG, "Failed to create incoming buffer");
        return ESP_ERR_NO_MEM;
    }
    
    // Allocate the flat catch-up stage buffer. This holds one whole SD file read off the
    // card during the radio-off phase, then streamed out radio-on. It is not a ring: it
    // is filled once per cycle and drained once per cycle.
    g_stage_buffer = (uint8_t*)heap_caps_malloc(PSRAM_STAGE_BUFFER_SIZE, MALLOC_CAP_SPIRAM);
    if (!g_stage_buffer) {
        ESP_LOGE(TAG, "Failed to allocate stage buffer in PSRAM");
        ring_buffer_destroy(g_incoming_buffer);
        g_incoming_buffer = NULL;
        return ESP_ERR_NO_MEM;
    }
    ESP_LOGI(TAG, "Stage buffer allocated: %d KB at 0x%p", PSRAM_STAGE_BUFFER_SIZE / 1024, g_stage_buffer);
    
    // Show final memory status
    psram_free = heap_caps_get_free_size(MALLOC_CAP_SPIRAM);
    size_t internal_free = heap_caps_get_free_size(MALLOC_CAP_INTERNAL);
    
    ESP_LOGI(TAG, "=== Buffer Allocation Complete ===");
    ESP_LOGI(TAG, "Total allocated: %zu MB", total_needed / (1024 * 1024));
    ESP_LOGI(TAG, "PSRAM remaining: %zu KB", psram_free / 1024);
    ESP_LOGI(TAG, "Internal RAM free: %zu KB", internal_free / 1024);
    
    return ESP_OK;
}

static esp_err_t initialize_sd_card_first(void)
{
    esp_err_t ret;
    
    ESP_LOGI(TAG, "Initializing SD card first (before HaLow)...");
    
    // Force release SPI bus
    ESP_LOGI(TAG, "Freeing SPI bus before SD card init");
    spi_bus_free(sd_host.slot);
    
    // Force HaLow module's CS high
    force_halow_cs_high();
    
    // Initialize SPI bus for SD card
    ret = spi_bus_initialize(sd_host.slot, &spi_bus_cfg, SPI_DMA_CH_AUTO);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "Failed to initialize SPI bus for SD: %s", esp_err_to_name(ret));
        return ret;
    }
    
    ESP_LOGI(TAG, "SPI bus initialized for SD card");
    
    // Mount and test SD card
    sd_slot_config.gpio_cs = PIN_SD_CS;
    sd_slot_config.host_id = sd_host.slot;
    
    ret = esp_vfs_fat_sdspi_mount(MOUNT_POINT, &sd_host, &sd_slot_config, 
                                   &sd_mount_config, &sd_card);
    if (ret == ESP_OK) {
        g_state.sd_mounted = true;
        ESP_LOGI(TAG, "SD card mounted successfully on startup");
        sdmmc_card_print_info(stdout, sd_card);
        
        // Scan for existing buffered files
        sd_scan_buffered_files();
        
        // Test write
        FILE* f = fopen(MOUNT_POINT"/test.txt", "w");
        if (f != NULL) {
            fprintf(f, "SD Card test at system init\n");
            fclose(f);
            ESP_LOGI(TAG, "SD card write test successful");
        }
        
        // Unmount SD card (but keep file list in memory)
        ret = esp_vfs_fat_sdcard_unmount(MOUNT_POINT, sd_card);
        if (ret == ESP_OK) {
            g_state.sd_mounted = false;
            sd_card = NULL;
            ESP_LOGI(TAG, "SD card unmounted after initial scan");
        }
    } else {
        ESP_LOGE(TAG, "Failed to mount SD card on startup: %s", esp_err_to_name(ret));
    }
    
    // Free the SPI bus so HaLow can use it
    ret = spi_bus_free(sd_host.slot);
    if (ret != ESP_OK) {
        ESP_LOGW(TAG, "Issue freeing SPI bus after SD init: %s", esp_err_to_name(ret));
    }
    
    ESP_LOGI(TAG, "SPI bus freed, ready for HaLow initialization");
    return ESP_OK;
}

static void initialize_system(void)
{
    // Initialize state mutex
    g_state.state_mutex = xSemaphoreCreateMutex();
    if (!g_state.state_mutex) {
        ESP_LOGE(TAG, "Failed to create state mutex");
        return;
    }
    
    // Create SPI bus mutex
    spi_bus_mutex = xSemaphoreCreateMutex();
    if (!spi_bus_mutex) {
        ESP_LOGE(TAG, "Failed to create SPI bus mutex");
        return;
    }
    
    // (event loop is created inside halow_init_once, which owns netif/event-loop setup)
    
    // Initialize SD card FIRST and scan for buffered files
    initialize_sd_card_first();
    
    // Small delay before HaLow init
    vTaskDelay(pdMS_TO_TICKS(200));
    
    // Initialize PSRAM buffers. If PSRAM is missing/misconfigured this returns
    // ESP_ERR_NO_MEM. Do NOT ESP_ERROR_CHECK it: that calls abort() -> reboot ->
    // infinite boot loop with the error scrolling past. Halt readably instead.
    if (initialize_psram_buffers() != ESP_OK) {
        ESP_LOGE(TAG, "============================================================");
        ESP_LOGE(TAG, "FATAL: PSRAM buffers could not be allocated.");
        ESP_LOGE(TAG, "PSRAM is likely disabled in sdkconfig. Enable it via:");
        ESP_LOGE(TAG, "  menuconfig -> Component config -> ESP PSRAM ->");
        ESP_LOGE(TAG, "  'Support for external SPI-connected RAM' (Octal mode).");
        ESP_LOGE(TAG, "Halting (no reboot) so this message stays readable.");
        ESP_LOGE(TAG, "============================================================");
        while (1) {
            vTaskDelay(pdMS_TO_TICKS(5000));
        }
    }
    
    // Initialize NVS (required by the mmhalow / esp_netif stack)
    esp_err_t nvs_ret = nvs_flash_init();
    if (nvs_ret == ESP_ERR_NVS_NO_FREE_PAGES || nvs_ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        nvs_ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(nvs_ret);

    // Initialize the NVS-backed monotonic file counter. Must run after nvs_flash_init and
    // after the boot SD scan (which sets g_max_existing_ctr), so a counter that fell behind
    // the files on the card is bumped forward. A failure here is not fatal to streaming, but
    // SD buffering would be unable to name files, so log loudly.
    if (file_counter_init() != ESP_OK) {
        ESP_LOGE(TAG, "File counter init failed; SD buffering will not be able to name files");
    }

    // Load any cached AP BSSID (shares the counter's NVS handle, so must run after
    // file_counter_init). Enables directed reconnect on the very first catch-up cycle.
    bssid_cache_load();

    // Initialize HaLow module (netif, driver, STA config)
    ESP_LOGI(TAG, "Initializing HaLow module...");
    if (halow_init_once() != ESP_OK) {
        ESP_LOGE(TAG, "HaLow initialization failed");
    }
}

static void print_statistics_task(void *arg)
{
    // Backlog trend tracking. We sample sd_bytes_to_catch_up once per tick and compare to the
    // previous sample to decide whether the backlog is shrinking (catching up), growing
    // (falling behind), or flat. First tick has no prior, so it just seeds the baseline.
    uint64_t prev_backlog = 0;
    int64_t  prev_ms = 0;
    bool     have_prev = false;

    // Scratch buffers for human_bytes(). Several are needed at once on a single line, so each
    // formatted value gets its own buffer.
    char b1[20], b2[20], b3[20], b4[20], b5[20], b6[20];

    while (1) {
        vTaskDelay(pdMS_TO_TICKS(10000));  // Print every 10 seconds

        int64_t now_ms = esp_timer_get_time() / 1000;
        uint64_t backlog = g_state.sd_bytes_to_catch_up;

        ESP_LOGI(TAG, "=== Statistics ===");
        ESP_LOGI(TAG, "Mode: %s",
                 g_state.mode == MODE_STREAMING ? "STREAMING" :
                 g_state.mode == MODE_SD_BUFFERING ? "SD_BUFFERING" : "CATCHING_UP");
        ESP_LOGI(TAG, "Total received: %s", human_bytes(g_total_bytes_received, b1, sizeof(b1)));
        ESP_LOGI(TAG, "Total sent: %s", human_bytes(g_total_bytes_sent, b2, sizeof(b2)));
        ESP_LOGI(TAG, "Total SD written: %s", human_bytes(g_total_bytes_sd_written, b3, sizeof(b3)));
        ESP_LOGI(TAG, "SD files pending: %d", g_state.sd_file_count);
        ESP_LOGI(TAG, "Incoming buffer: %s / %s",
                 human_bytes(ring_buffer_get_data_size(g_incoming_buffer), b4, sizeof(b4)),
                 human_bytes(PSRAM_INCOMING_BUFFER_SIZE, b5, sizeof(b5)));

        // Backlog trend: compare against the previous sample to report whether we are actually
        // catching up or falling behind, plus the net rate and (when draining) an ETA to clear.
        if (backlog == 0) {
            ESP_LOGI(TAG, "Backlog: CLEAR");
        } else if (!have_prev || now_ms <= prev_ms) {
            // No usable prior sample yet; just report the standing backlog.
            ESP_LOGI(TAG, "Backlog: %s pending (trend pending)",
                     human_bytes(backlog, b6, sizeof(b6)));
        } else {
            double secs = (double)(now_ms - prev_ms) / 1000.0;
            // Signed delta: negative means the backlog shrank (we are catching up).
            double delta = (double)backlog - (double)prev_backlog;
            double rate = delta / secs;                 // bytes/sec, signed
            double abs_rate = rate < 0 ? -rate : rate;
            // Treat near-zero drift (< 1 KB/s) as holding steady to avoid noisy flip-flop.
            if (abs_rate < 1024.0) {
                ESP_LOGI(TAG, "Backlog: HOLDING at %s (~0 B/s)",
                         human_bytes(backlog, b6, sizeof(b6)));
            } else if (rate < 0) {
                // Draining: ETA = remaining backlog / drain rate.
                uint64_t eta_s = (uint64_t)((double)backlog / abs_rate);
                ESP_LOGI(TAG, "Backlog: CATCHING UP, %s left, draining %s/s, ETA ~%llu s",
                         human_bytes(backlog, b6, sizeof(b6)),
                         human_bytes((uint64_t)abs_rate, b1, sizeof(b1)),
                         (unsigned long long)eta_s);
            } else {
                ESP_LOGI(TAG, "Backlog: FALLING BEHIND, %s and growing %s/s",
                         human_bytes(backlog, b6, sizeof(b6)),
                         human_bytes((uint64_t)abs_rate, b1, sizeof(b1)));
            }
        }
        prev_backlog = backlog;
        prev_ms = now_ms;
        have_prev = true;

        ESP_LOGI(TAG, "Network: %s, Stream: %s, Retries: %d/%d",
                 g_state.network_healthy ? "OK" : "DOWN",
                 g_state.stream_healthy ? "OK" : "DOWN",
                 g_state.stream_retry_count, STREAM_MAX_RETRIES);

        // Lifetime fault counters: how rough the link has been since boot.
        ESP_LOGI(TAG, "Faults: %lu stream failure(s), %lu SD fallback(s), %lu catch-up abort(s)",
                 (unsigned long)g_total_stream_failures,
                 (unsigned long)g_total_sd_fallbacks,
                 (unsigned long)g_total_catchup_aborts);

        // Only surface data loss if any has occurred; in the normal no-loss case this stays
        // silent. Flags whether loss is happening right now versus a past total.
        if (g_total_bytes_lost > 0) {
            ESP_LOGW(TAG, "Data lost: %s total since boot%s",
                     human_bytes(g_total_bytes_lost, b1, sizeof(b1)),
                     g_losing_data ? " (LOSING NOW)" : "");
        }

        // PSRAM usage info.
        size_t psram_free = heap_caps_get_free_size(MALLOC_CAP_SPIRAM);
        size_t psram_total = heap_caps_get_total_size(MALLOC_CAP_SPIRAM);
        ESP_LOGI(TAG, "PSRAM: %zu/%zu KB free (%.1f%% used)",
                 psram_free / 1024, psram_total / 1024,
                 ((psram_total - psram_free) * 100.0) / psram_total);

        // HaLow signal (only meaningful when the radio is up, i.e. STREAMING / radio phase).
        log_halow_link("stats");
    }
}

/* ========================== Main Application ========================== */

void app_main(void)
{
    ESP_LOGI(TAG, "=== Robust Audio Streaming System Starting ===");

    // Silence the ESP-IDF gpio driver's per-pin INFO spam. Every SD mount/unmount reconfigures
    // the shared-bus CS pins, which otherwise logs a GPIO[..] line per pin each cycle. Warnings
    // and errors from the driver still come through.
    esp_log_level_set("gpio", ESP_LOG_WARN);

    // Print PSRAM information first
    print_psram_info();
    
    // Initialize system components
    initialize_system();
    
    // Startup test sequence
    ESP_LOGI(TAG, "=== Starting HaLow WiFi after SD initialization ===");
    
    // Start WiFi and connect to network
    if (wifi_reconnect() == ESP_OK) {
        ESP_LOGI(TAG, "WiFi connected successfully");
        
        // Try to connect to streaming endpoint
        if (stream_connect() == ESP_OK) {
            g_state.stream_healthy = true;
            
            // Check if we have buffered files to upload
            if (g_state.sd_file_count > 0) {
                ESP_LOGI(TAG, "Found %d buffered files to upload, starting in CATCHING_UP mode", 
                         g_state.sd_file_count);
                // Just set the mode. The first catch-up cycle owns the SD<->radio bus
                // handoff; do not mount here (the radio is currently up).
                g_state.mode = MODE_CATCHING_UP;
            } else {
                g_state.mode = MODE_STREAMING;
                ESP_LOGI(TAG, "No buffered files, starting in STREAMING mode");
            }
        } else {
            ESP_LOGW(TAG, "Stream connection failed, will start in SD mode");
            switch_to_sd_mode();
        }
    } else {
        ESP_LOGW(TAG, "Initial WiFi connection failed, starting in SD mode");
        switch_to_sd_mode();
    }
    
    ESP_LOGI(TAG, "=== Startup tests complete, initializing USB ===");
    
    // Start USB host
    ctrl_sem = xSemaphoreCreateBinary();
    const usb_host_config_t host_cfg = {
        .skip_phy_setup = false,
        .intr_flags = 0,
    };
    ESP_ERROR_CHECK(usb_host_install(&host_cfg));
    
    // Create tasks
    xTaskCreatePinnedToCore(daemon_task, "usb_daemon", 4096, NULL, 3, NULL, 0);
    xTaskCreatePinnedToCore(client_task, "usb_client", 8192, NULL, 4, NULL, 1);
    xTaskCreatePinnedToCore(stream_manager_task, "stream_mgr", 8192, NULL, 5, &stream_manager_task_handle, 1);
    xTaskCreatePinnedToCore(network_monitor_task, "net_monitor", 4096, NULL, 4, &network_monitor_task_handle, 1);
    xTaskCreatePinnedToCore(print_statistics_task, "stats", 4096, NULL, 1, NULL, 0);
    
    ESP_LOGI(TAG, "=== System initialization complete ===");
    ESP_LOGI(TAG, "Audio data rate: %d kbps", AUDIO_DATA_RATE_BPS / 1000);
    ESP_LOGI(TAG, "Buffer capacity: ~%d seconds", PSRAM_INCOMING_BUFFER_SIZE / (AUDIO_DATA_RATE_BPS / 8));
    ESP_LOGI(TAG, "Stream retry delay: %d ms, max retries: %d", STREAM_RETRY_DELAY_MS, STREAM_MAX_RETRIES);
    ESP_LOGI(TAG, "Network check interval: %d ms", NETWORK_CHECK_INTERVAL_MS);
    ESP_LOGI(TAG, "Current mode: %s", 
             g_state.mode == MODE_STREAMING ? "STREAMING" :
             g_state.mode == MODE_SD_BUFFERING ? "SD_BUFFERING" : "CATCHING_UP");
    if (g_state.sd_file_count > 0) {
        ESP_LOGI(TAG, "Buffered files to upload: %d", g_state.sd_file_count);
    }
    
    // Final memory status
    size_t psram_free = heap_caps_get_free_size(MALLOC_CAP_SPIRAM);
    size_t internal_free = heap_caps_get_free_size(MALLOC_CAP_INTERNAL);
    ESP_LOGI(TAG, "Memory status - PSRAM free: %zu KB, Internal free: %zu KB", 
             psram_free / 1024, internal_free / 1024);
    
    // Main loop
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(60000));  // Sleep for 1 minute
    }
}