// AudioMoth USB -> HTTP chunked streamer with SD fallback and robust error handling
// ESP-IDF v5.1.1 compatible

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

#include "mm_app_common.h"
#include "mmosal.h"
#include "mmwlan.h"
#include "mmipal.h"

#include "usb/usb_host.h"
#include "usb/usb_types_stack.h"
#include "usb/usb_helpers.h"
#include "usb/usb_types_ch9.h"

#include "driver/gpio.h"
#include "esp_rom_gpio.h"
#include "hal/gpio_types.h"
#include "soc/gpio_sig_map.h"

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
#define PIN_MIRROR_CS 6

#define MOUNT_POINT "/sdcard"

// Network configuration
#define SERVER_URL          "http://192.168.100.162:8000/recordings_stream"
#define LISTENER_ID         "esp32_01_outdoor_test"
#define WIFI_CONNECT_TIMEOUT_MS  10000  // 10 seconds timeout for WiFi connection

// Buffer configuration (easily changeable)
#define PSRAM_INCOMING_BUFFER_SIZE  (3 * 1024 * 1024)  // 3MB for incoming USB data
#define PSRAM_OUTGOING_BUFFER_SIZE  (3 * 1024 * 1024)  // 3MB for outgoing network data
#define PSRAM_SWITCH_BUFFER_SIZE    (256 * 1024)       // 256KB for mode switching
#define SD_BLOCK_SIZE               (32 * 1024)        // 32KB blocks for SD writes
#define SD_MAX_WRITE_SIZE           (128 * 1024)       // Max 128KB per SD write operation

// Timing configuration (easily changeable)
#define STREAM_RETRY_DELAY_MS       5000   // Delay between stream reconnection attempts
#define STREAM_MAX_RETRIES          2       // Number of stream reconnection attempts before SD mode
#define NETWORK_CHECK_INTERVAL_MS   20000  // Check network every 20 seconds when on SD
#define CATCHUP_PAUSE_INTERVAL_MS   10000  // Pause catch-up every 10 seconds to write new data
#define STREAM_HEALTH_CHECK_MS      10000  // Check stream health every 10 seconds

// Audio data rate: 96 bytes/ms = 96KB/s = 768kbps
#define AUDIO_DATA_RATE_BPS         768000
#define AUDIO_BYTES_PER_MS          96

// USB configuration
#define ISO_MPS              96
#define ISO_PKTS_PER_URB     16
#define NUM_ISO_URBS         3

#define FRAME_HEADER_SIZE    6   // 3 bytes seq + 3 bytes length

static const char *TAG = "am_robust_stream";

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
    uint32_t sd_bytes_buffered;
    
    // SD file management
    char current_write_filename[128];
    char current_read_filename[128];
    FILE *sd_write_file;
    FILE *sd_read_file;
    sd_file_info_t *sd_file_list;
    int sd_file_count;
    int sd_current_file_index;
    
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
static ring_buffer_t *g_outgoing_buffer = NULL;
static uint8_t *g_switch_buffer = NULL;

// System state
static system_state_t g_state = {
    .mode = MODE_STREAMING,
    .network_healthy = false,
    .stream_healthy = false,
    .sd_mounted = false,
    .wifi_connected = false,
    .stream_retry_count = 0,
    .sequence_number = 0,
    .sd_bytes_buffered = 0,
    .sd_write_file = NULL,
    .sd_read_file = NULL,
    .sd_file_list = NULL,
    .sd_file_count = 0,
    .sd_current_file_index = 0
};

// Streaming context
static streaming_context_t stream_ctx = {0};

// USB variables
static usb_host_client_handle_t g_client;
static usb_device_handle_t      g_dev;
static SemaphoreHandle_t        ctrl_sem;
static usb_transfer_t          *s_iso_urbs[NUM_ISO_URBS] = {0};
static iso_cb_ctx_t            *s_iso_ctxs[NUM_ISO_URBS] = {0};

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

static size_t ring_buffer_read(ring_buffer_t *rb, uint8_t *data, size_t len)
{
    if (!rb || !data || len == 0) return 0;
    
    xSemaphoreTake(rb->mutex, portMAX_DELAY);
    
    size_t to_read = (len > rb->data_size) ? rb->data_size : len;
    
    if (to_read == 0) {
        xSemaphoreGive(rb->mutex);
        return 0;
    }
    
    // Read in up to two chunks (wrap around)
    size_t first_chunk = rb->capacity - rb->read_pos;
    if (first_chunk > to_read) first_chunk = to_read;
    
    memcpy(data, rb->buffer + rb->read_pos, first_chunk);
    
    if (to_read > first_chunk) {
        memcpy(data + first_chunk, rb->buffer, to_read - first_chunk);
    }
    
    rb->read_pos = (rb->read_pos + to_read) % rb->capacity;
    rb->data_size -= to_read;
    
    xSemaphoreGive(rb->mutex);
    return to_read;
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
        .timeout_ms = 5000,
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

static void setup_gpio_mirroring(void)
{
    ESP_LOGI(TAG, "Setting up GPIO 6 to mirror GPIO 21");
    gpio_set_direction(PIN_SD_CS, GPIO_MODE_INPUT_OUTPUT);
    gpio_set_direction(PIN_MIRROR_CS, GPIO_MODE_OUTPUT);
    esp_rom_gpio_connect_out_signal(PIN_MIRROR_CS, 0x100 + PIN_SD_CS, false, false);
    ESP_LOGI(TAG, "GPIO 6 now mirrors GPIO 21 continuously");
}

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
    g_state.sd_current_file_index = 0;
    
    dir = opendir(MOUNT_POINT);
    if (dir == NULL) {
        ESP_LOGE(TAG, "Failed to open SD card directory");
        return ESP_FAIL;
    }
    
    // Count audio buffer files
    int file_count = 0;
    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, "audio_buffer_") != NULL && strstr(entry->d_name, ".bin") != NULL) {
            file_count++;
        }
    }
    
    if (file_count == 0) {
        closedir(dir);
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
    while ((entry = readdir(dir)) != NULL && index < file_count) {
        if (strstr(entry->d_name, "audio_buffer_") != NULL && strstr(entry->d_name, ".bin") != NULL) {
            // Check if the filename is too long
            if (strlen(entry->d_name) > 400) {  // Leave room for mount point and slash
                ESP_LOGW(TAG, "Filename too long, skipping: %.50s...", entry->d_name);
                continue;
            }
            
            int ret = snprintf(full_path, sizeof(full_path), "%s/%s", MOUNT_POINT, entry->d_name);
            if (ret >= sizeof(full_path)) {
                ESP_LOGW(TAG, "Path truncated, skipping file: %s", entry->d_name);
                continue;
            }
            
            if (stat(full_path, &file_stat) == 0) {
                strncpy(g_state.sd_file_list[index].filename, full_path, sizeof(g_state.sd_file_list[index].filename) - 1);
                g_state.sd_file_list[index].filename[sizeof(g_state.sd_file_list[index].filename) - 1] = '\0';  // Ensure null termination
                
                // Extract timestamp from filename (audio_buffer_TIMESTAMP.bin)
                char *timestamp_str = strstr(entry->d_name, "audio_buffer_") + strlen("audio_buffer_");
                g_state.sd_file_list[index].timestamp = atoll(timestamp_str);
                
                index++;
            }
        }
    }
    
    closedir(dir);
    g_state.sd_file_count = index;
    
    // Sort files by timestamp
    qsort(g_state.sd_file_list, g_state.sd_file_count, sizeof(sd_file_info_t), compare_files_by_timestamp);
    
    ESP_LOGI(TAG, "Sorted %d buffered files for upload", g_state.sd_file_count);
    for (int i = 0; i < g_state.sd_file_count; i++) {
        ESP_LOGI(TAG, "  [%d] %s (timestamp: %lld)", i, g_state.sd_file_list[i].filename, g_state.sd_file_list[i].timestamp);
    }
    
    return ESP_OK;
}

static esp_err_t sd_card_mount(void)
{
    esp_err_t ret;
    
    if (g_state.sd_mounted) {
        return ESP_OK;
    }
    
    ESP_LOGI(TAG, "Mounting SD card...");
    
    // Stop WiFi to get SPI bus
    ESP_LOGI(TAG, "Stopping HaLow for SD access...");
    app_wlan_stop();
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

static esp_err_t sd_write_audio_data(const uint8_t *data, size_t len)
{
    if (!g_state.sd_mounted || !data || len == 0) {
        return ESP_FAIL;
    }
    
    // Open file if not already open
    if (!g_state.sd_write_file) {
        // Generate filename with timestamp
        int64_t timestamp = esp_timer_get_time() / 1000000; // Convert to seconds
        snprintf(g_state.current_write_filename, sizeof(g_state.current_write_filename),
                 MOUNT_POINT"/audio_buffer_%lld.bin", timestamp);
        
        // Make sure to close any stale file handle first
        if (g_state.sd_write_file) {
            fclose(g_state.sd_write_file);
            g_state.sd_write_file = NULL;
        }
        
        g_state.sd_write_file = fopen(g_state.current_write_filename, "wb");
        if (!g_state.sd_write_file) {
            ESP_LOGE(TAG, "Failed to open SD file for writing: %s", g_state.current_write_filename);
            ESP_LOGE(TAG, "errno: %d (%s)", errno, strerror(errno));
            
            // Try with a simpler filename
            static int file_counter = 0;
            snprintf(g_state.current_write_filename, sizeof(g_state.current_write_filename),
                     MOUNT_POINT"/audio_%d.bin", file_counter++);
            
            g_state.sd_write_file = fopen(g_state.current_write_filename, "wb");
            if (!g_state.sd_write_file) {
                ESP_LOGE(TAG, "Failed to open simplified SD file: %s", g_state.current_write_filename);
                return ESP_FAIL;
            }
        }
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
            fclose(g_state.sd_write_file);
            g_state.sd_write_file = NULL;
            return ESP_FAIL;
        }
        written += result;
    }
    
    // Flush to ensure data is written
    if (fflush(g_state.sd_write_file) != 0) {
        ESP_LOGE(TAG, "SD flush failed: errno: %d (%s)", errno, strerror(errno));
        // Close the failed file
        fclose(g_state.sd_write_file);
        g_state.sd_write_file = NULL;
        return ESP_FAIL;
    }
    
    g_state.sd_bytes_buffered += written;
    g_total_bytes_sd_written += written;
    
    // Periodically close and reopen file to prevent issues
    static size_t bytes_in_current_file = 0;
    bytes_in_current_file += written;
    if (bytes_in_current_file > (10 * 1024 * 1024)) { // 10MB per file
        ESP_LOGI(TAG, "Rotating SD file after 10MB");
        fclose(g_state.sd_write_file);
        g_state.sd_write_file = NULL;
        bytes_in_current_file = 0;
    }
    
    return ESP_OK;
}

static esp_err_t sd_open_next_file_for_reading(void)
{
    // Close current read file if open
    if (g_state.sd_read_file) {
        fclose(g_state.sd_read_file);
        g_state.sd_read_file = NULL;
        
        // Delete the file we just finished reading
        if (strlen(g_state.current_read_filename) > 0) {
            ESP_LOGI(TAG, "Deleting uploaded file: %s", g_state.current_read_filename);
            unlink(g_state.current_read_filename);
        }
    }
    
    // Check if we have more files to read
    if (g_state.sd_current_file_index >= g_state.sd_file_count) {
        ESP_LOGI(TAG, "All buffered files have been uploaded");
        // Reset file list
        if (g_state.sd_file_list) {
            free(g_state.sd_file_list);
            g_state.sd_file_list = NULL;
        }
        g_state.sd_file_count = 0;
        g_state.sd_current_file_index = 0;
        g_state.sd_bytes_buffered = 0;
        return ESP_OK;  // No more files
    }
    
    // Open next file
    strncpy(g_state.current_read_filename, g_state.sd_file_list[g_state.sd_current_file_index].filename, 
            sizeof(g_state.current_read_filename) - 1);
    
    g_state.sd_read_file = fopen(g_state.current_read_filename, "rb");
    if (!g_state.sd_read_file) {
        ESP_LOGE(TAG, "Failed to open SD file for reading: %s", g_state.current_read_filename);
        g_state.sd_current_file_index++;
        return ESP_FAIL;
    }
    
    ESP_LOGI(TAG, "Opened file [%d/%d] for reading: %s", 
             g_state.sd_current_file_index + 1, g_state.sd_file_count, g_state.current_read_filename);
    
    g_state.sd_current_file_index++;
    return ESP_OK;
}

static esp_err_t sd_read_audio_data(uint8_t *data, size_t len, size_t *bytes_read)
{
    if (!g_state.sd_mounted || !data) {
        return ESP_FAIL;
    }
    
    *bytes_read = 0;
    
    // If no read file is open, try to open the next one
    if (!g_state.sd_read_file) {
        if (sd_open_next_file_for_reading() != ESP_OK) {
            return ESP_OK;  // No more files, but not an error
        }
    }
    
    // Read from current file
    *bytes_read = fread(data, 1, len, g_state.sd_read_file);
    
    if (*bytes_read == 0 && feof(g_state.sd_read_file)) {
        // Current file finished, try next one
        ESP_LOGI(TAG, "Finished reading current file");
        if (sd_open_next_file_for_reading() == ESP_OK && g_state.sd_read_file) {
            // Try reading from the new file
            *bytes_read = fread(data, 1, len, g_state.sd_read_file);
        }
    }
    
    if (g_state.sd_bytes_buffered >= *bytes_read) {
        g_state.sd_bytes_buffered -= *bytes_read;
    }
    
    return (*bytes_read > 0) ? ESP_OK : ESP_OK;  // Always return OK, 0 bytes means no more data
}

/* ========================== Network Management ========================== */

static esp_err_t wifi_reconnect(void)
{
    ESP_LOGI(TAG, "Attempting WiFi reconnection...");
    
    // Make sure SD is unmounted first
    if (g_state.sd_mounted) {
        sd_card_unmount();
    }
    
    // Start WiFi with timeout
    int64_t start_time = esp_timer_get_time() / 1000;
    app_wlan_start();  // This blocks until connected
    
    // Check if connection succeeded within timeout
    int64_t elapsed = (esp_timer_get_time() / 1000) - start_time;
    if (elapsed > WIFI_CONNECT_TIMEOUT_MS) {
        ESP_LOGE(TAG, "WiFi connection timeout");
        app_wlan_stop();
        return ESP_FAIL;
    }
    
    // Configure WiFi settings
    mmwlan_set_power_save_mode(MMWLAN_PS_DISABLED);
    mmwlan_set_wnm_sleep_enabled(false);
    
    g_state.wifi_connected = true;
    g_state.network_healthy = true;
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

static esp_err_t attempt_stream_reconnection(void)
{
    ESP_LOGI(TAG, "Attempting to reconnect stream (attempt %d/%d)", 
             g_state.stream_retry_count + 1, STREAM_MAX_RETRIES);
    
    // Disconnect existing stream
    stream_disconnect();
    
    // Wait before retry
    vTaskDelay(pdMS_TO_TICKS(STREAM_RETRY_DELAY_MS));
    
    // Try to reconnect
    if (stream_connect() == ESP_OK) {
        ESP_LOGI(TAG, "Stream reconnection successful");
        g_state.stream_retry_count = 0;
        g_state.stream_healthy = true;
        return ESP_OK;
    }
    
    g_state.stream_retry_count++;
    return ESP_FAIL;
}

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
    
    xSemaphoreGive(g_state.state_mutex);
    
    ESP_LOGI(TAG, "Switched to SD_BUFFERING mode");
    return ESP_OK;
}

static esp_err_t switch_to_catchup_mode(void)
{
    ESP_LOGI(TAG, "Switching to CATCHING_UP mode");
    
    xSemaphoreTake(g_state.state_mutex, portMAX_DELAY);
    
    // Need to have SD files to catch up from
    if (g_state.sd_file_count == 0) {
        ESP_LOGI(TAG, "No SD data to catch up, going directly to streaming");
        xSemaphoreGive(g_state.state_mutex);
        return switch_to_streaming_mode();
    }
    
    // Ensure we can read from SD
    if (!g_state.sd_read_file) {
        if (sd_open_next_file_for_reading() != ESP_OK) {
            ESP_LOGE(TAG, "Failed to open SD file for catch-up");
            xSemaphoreGive(g_state.state_mutex);
            return ESP_FAIL;
        }
    }
    
    g_state.mode = MODE_CATCHING_UP;
    g_state.last_catchup_pause_ms = esp_timer_get_time() / 1000;
    
    xSemaphoreGive(g_state.state_mutex);
    
    ESP_LOGI(TAG, "Switched to CATCHING_UP mode");
    return ESP_OK;
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
    
    // Check if we need to catch up from boot
    if (g_state.sd_file_count > 0) {
        ESP_LOGI(TAG, "Found %d buffered files on boot, switching to catch-up mode", g_state.sd_file_count);
        switch_to_catchup_mode();
    }
    
    while (1) {
        xSemaphoreTake(g_state.state_mutex, portMAX_DELAY);
        stream_mode_t current_mode = g_state.mode;
        xSemaphoreGive(g_state.state_mutex);
        
        switch (current_mode) {
            case MODE_STREAMING: {
                // Read from incoming buffer and send to network
                size_t available = ring_buffer_get_data_size(g_incoming_buffer);
                if (available >= SD_BLOCK_SIZE) {
                    size_t bytes_read = ring_buffer_read(g_incoming_buffer, work_buffer, SD_BLOCK_SIZE);
                    if (bytes_read > 0) {
                        esp_err_t err = stream_send_with_header(work_buffer, bytes_read, g_state.sequence_number);
                        if (err == ESP_OK) {
                            g_state.sequence_number = (g_state.sequence_number + 1) & 0xFFFFFF;
                            g_state.last_stream_write_ms = esp_timer_get_time() / 1000;
                            g_total_bytes_sent += bytes_read;
                            
                            // Reset retry count on successful send
                            g_state.stream_retry_count = 0;
                        } else {
                            // Stream failed, try to reconnect
                            ESP_LOGE(TAG, "Stream write failed (retry %d/%d)", g_state.stream_retry_count, STREAM_MAX_RETRIES);
                            g_state.stream_healthy = false;
                            
                            // Put data back into buffer so we don't lose it
                            ring_buffer_write(g_incoming_buffer, work_buffer, bytes_read);
                            
                            if (g_state.stream_retry_count < STREAM_MAX_RETRIES) {
                                if (attempt_stream_reconnection() != ESP_OK) {
                                    if (g_state.stream_retry_count >= STREAM_MAX_RETRIES) {
                                        ESP_LOGE(TAG, "Max stream retry attempts reached, switching to SD mode");
                                        switch_to_sd_mode();
                                    }
                                }
                            } else {
                                switch_to_sd_mode();
                            }
                        }
                    }
                } else {
                    vTaskDelay(pdMS_TO_TICKS(10));
                }
                break;
            }
            
            case MODE_SD_BUFFERING: {
                // Read from incoming buffer and write to SD
                size_t available = ring_buffer_get_data_size(g_incoming_buffer);
                size_t free_space = ring_buffer_get_free_space(g_incoming_buffer);
                bool did_work = false;
                
                if (available >= SD_BLOCK_SIZE) {
                    // Write full blocks
                    size_t bytes_read = ring_buffer_read(g_incoming_buffer, work_buffer, SD_BLOCK_SIZE);
                    if (bytes_read > 0) {
                        ESP_LOGI(TAG, "Writing %zu bytes to SD (full block)", bytes_read);
                        esp_err_t err = sd_write_audio_data(work_buffer, bytes_read);
                        if (err != ESP_OK) {
                            ESP_LOGE(TAG, "SD write failed!");
                            // SD failure is critical - only try network if buffer has space
                            if (free_space > (PSRAM_INCOMING_BUFFER_SIZE / 2)) {
                                ESP_LOGW(TAG, "Attempting emergency network fallback due to SD failure");
                                
                                // Put data back in buffer before switching
                                ring_buffer_write(g_incoming_buffer, work_buffer, bytes_read);
                                
                                // Properly unmount SD before trying network
                                sd_card_unmount();
                                
                                if (wifi_reconnect() == ESP_OK && stream_connect() == ESP_OK) {
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
                            did_work = true;
                        }
                    }
                } else if (available > 0 && free_space < SD_BLOCK_SIZE) {
                    // Buffer is getting full, flush to prevent overflow
                    size_t bytes_read = ring_buffer_read(g_incoming_buffer, work_buffer, available);
                    if (bytes_read > 0) {
                        ESP_LOGW(TAG, "Emergency flush: writing %zu bytes to SD", bytes_read);
                        esp_err_t err = sd_write_audio_data(work_buffer, bytes_read);
                        if (err != ESP_OK) {
                            ESP_LOGE(TAG, "Emergency flush failed!");
                            // Put data back
                            ring_buffer_write(g_incoming_buffer, work_buffer, bytes_read);
                        } else {
                            did_work = true;
                        }
                    }
                }
                
                // Check if it's time to try reconnecting
                int64_t now_ms = esp_timer_get_time() / 1000;
                if ((now_ms - g_state.last_network_check_ms) > NETWORK_CHECK_INTERVAL_MS) {
                    ESP_LOGI(TAG, "Attempting to reconnect to network...");
                    g_state.last_network_check_ms = now_ms;
                    
                    // Close write file before unmounting
                    if (g_state.sd_write_file) {
                        fclose(g_state.sd_write_file);
                        g_state.sd_write_file = NULL;
                    }
                    
                    // Rescan files before switching modes
                    sd_scan_buffered_files();
                    
                    // Properly unmount SD before network attempt
                    sd_card_unmount();
                    
                    if (wifi_reconnect() == ESP_OK && stream_connect() == ESP_OK) {
                        if (g_state.sd_file_count > 0) {
                            // Need to remount SD for reading
                            sd_card_mount();
                            switch_to_catchup_mode();
                        } else {
                            switch_to_streaming_mode();
                        }
                    } else {
                        // Failed to reconnect, go back to SD
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
                // Balance uploading old data with processing new data
                size_t incoming_available = ring_buffer_get_data_size(g_incoming_buffer);
                size_t incoming_free = ring_buffer_get_free_space(g_incoming_buffer);
                
                // If incoming buffer is getting full, write it to SD
                if (incoming_free < (PSRAM_INCOMING_BUFFER_SIZE / 4)) {
                    ESP_LOGI(TAG, "Incoming buffer filling during catch-up, writing to SD");
                    
                    // Need to ensure we have SD write capability
                    if (!g_state.sd_write_file) {
                        // Generate new filename for incoming data during catch-up
                        int64_t timestamp = esp_timer_get_time() / 1000000;
                        snprintf(g_state.current_write_filename, sizeof(g_state.current_write_filename),
                                 MOUNT_POINT"/audio_buffer_%lld.bin", timestamp);
                        
                        g_state.sd_write_file = fopen(g_state.current_write_filename, "wb");
                        if (!g_state.sd_write_file) {
                            ESP_LOGE(TAG, "Failed to open SD file for catch-up overflow");
                            // Critical error - switch back to SD mode
                            switch_to_sd_mode();
                            continue;
                        }
                        ESP_LOGI(TAG, "Opened new SD file for catch-up overflow: %s", g_state.current_write_filename);
                    }
                    
                    // Write incoming data to SD
                    while (incoming_available >= SD_BLOCK_SIZE) {
                        size_t bytes_read = ring_buffer_read(g_incoming_buffer, work_buffer, SD_BLOCK_SIZE);
                        if (bytes_read > 0) {
                            sd_write_audio_data(work_buffer, bytes_read);
                        }
                        incoming_available = ring_buffer_get_data_size(g_incoming_buffer);
                    }
                    
                    // Close write file and add to catch-up list
                    if (g_state.sd_write_file) {
                        fclose(g_state.sd_write_file);
                        g_state.sd_write_file = NULL;
                        // Rescan to include the new file
                        sd_scan_buffered_files();
                    }
                }
                
                // Upload from SD to network
                size_t bytes_read;
                esp_err_t err = sd_read_audio_data(work_buffer, SD_BLOCK_SIZE, &bytes_read);
                
                if (err == ESP_OK && bytes_read > 0) {
                    err = stream_send_with_header(work_buffer, bytes_read, g_state.sequence_number);
                    if (err == ESP_OK) {
                        g_state.sequence_number = (g_state.sequence_number + 1) & 0xFFFFFF;
                        g_state.last_stream_write_ms = esp_timer_get_time() / 1000;
                        g_total_bytes_sent += bytes_read;
                    } else {
                        ESP_LOGE(TAG, "Stream failed during catch-up");
                        g_state.stream_healthy = false;
                        
                        // Try to reconnect
                        if (g_state.stream_retry_count < STREAM_MAX_RETRIES) {
                            if (attempt_stream_reconnection() != ESP_OK) {
                                if (g_state.stream_retry_count >= STREAM_MAX_RETRIES) {
                                    switch_to_sd_mode();
                                }
                            }
                        } else {
                            switch_to_sd_mode();
                        }
                    }
                } else if (bytes_read == 0) {
                    // No more data in files, check if we're completely caught up
                    if (g_state.sd_current_file_index >= g_state.sd_file_count) {
                        ESP_LOGI(TAG, "Caught up with all SD buffers, switching to streaming mode");
                        switch_to_streaming_mode();
                    }
                }
                
                vTaskDelay(pdMS_TO_TICKS(1));
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
        
        if (current_mode == MODE_STREAMING || current_mode == MODE_CATCHING_UP) {
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
        if (written < out) {
            ESP_LOGW(TAG, "Incoming buffer overflow! Lost %zu bytes", out - written);
        }
        g_total_bytes_received += written;
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
    size_t total_needed = PSRAM_INCOMING_BUFFER_SIZE + PSRAM_OUTGOING_BUFFER_SIZE + PSRAM_SWITCH_BUFFER_SIZE;
    if (psram_free < total_needed) {
        ESP_LOGE(TAG, "Not enough PSRAM! Need %zu KB, have %zu KB free", 
                 total_needed / 1024, psram_free / 1024);
        return ESP_ERR_NO_MEM;
    }
    
    // Create incoming buffer (3MB)
    g_incoming_buffer = ring_buffer_create(PSRAM_INCOMING_BUFFER_SIZE);
    if (!g_incoming_buffer) {
        ESP_LOGE(TAG, "Failed to create incoming buffer");
        return ESP_ERR_NO_MEM;
    }
    
    // Create outgoing buffer (3MB)
    g_outgoing_buffer = ring_buffer_create(PSRAM_OUTGOING_BUFFER_SIZE);
    if (!g_outgoing_buffer) {
        ESP_LOGE(TAG, "Failed to create outgoing buffer");
        ring_buffer_destroy(g_incoming_buffer);
        return ESP_ERR_NO_MEM;
    }
    
    // Allocate switch buffer (256KB) explicitly in PSRAM
    g_switch_buffer = (uint8_t*)heap_caps_malloc(PSRAM_SWITCH_BUFFER_SIZE, MALLOC_CAP_SPIRAM);
    if (!g_switch_buffer) {
        ESP_LOGE(TAG, "Failed to allocate switch buffer in PSRAM");
        ring_buffer_destroy(g_incoming_buffer);
        ring_buffer_destroy(g_outgoing_buffer);
        return ESP_ERR_NO_MEM;
    }
    ESP_LOGI(TAG, "Switch buffer allocated: %d KB at 0x%p", PSRAM_SWITCH_BUFFER_SIZE / 1024, g_switch_buffer);
    
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
    
    // Set up GPIO mirroring
    setup_gpio_mirroring();
    
    // Initialize event loop
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    
    // Initialize SD card FIRST and scan for buffered files
    initialize_sd_card_first();
    
    // Small delay before HaLow init
    vTaskDelay(pdMS_TO_TICKS(200));
    
    // Initialize PSRAM buffers
    ESP_ERROR_CHECK(initialize_psram_buffers());
    
    // Initialize HaLow module
    ESP_LOGI(TAG, "Initializing HaLow module...");
    app_wlan_init();
}

static void print_statistics_task(void *arg)
{
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(10000));  // Print every 10 seconds
        
        ESP_LOGI(TAG, "=== Statistics ===");
        ESP_LOGI(TAG, "Mode: %s", 
                 g_state.mode == MODE_STREAMING ? "STREAMING" :
                 g_state.mode == MODE_SD_BUFFERING ? "SD_BUFFERING" : "CATCHING_UP");
        ESP_LOGI(TAG, "Total received: %llu bytes", g_total_bytes_received);
        ESP_LOGI(TAG, "Total sent: %llu bytes", g_total_bytes_sent);
        ESP_LOGI(TAG, "Total SD written: %llu bytes", g_total_bytes_sd_written);
        ESP_LOGI(TAG, "SD buffered: %lu bytes", g_state.sd_bytes_buffered);
        ESP_LOGI(TAG, "SD files pending: %d", g_state.sd_file_count - g_state.sd_current_file_index);
        ESP_LOGI(TAG, "Incoming buffer: %zu/%zu bytes", 
                 ring_buffer_get_data_size(g_incoming_buffer), PSRAM_INCOMING_BUFFER_SIZE);
        ESP_LOGI(TAG, "Network: %s, Stream: %s, Retries: %d/%d", 
                 g_state.network_healthy ? "OK" : "DOWN",
                 g_state.stream_healthy ? "OK" : "DOWN",
                 g_state.stream_retry_count, STREAM_MAX_RETRIES);
        
        // Add PSRAM usage info
        size_t psram_free = heap_caps_get_free_size(MALLOC_CAP_SPIRAM);
        size_t psram_total = heap_caps_get_total_size(MALLOC_CAP_SPIRAM);
        ESP_LOGI(TAG, "PSRAM: %zu/%zu KB free (%.1f%% used)", 
                 psram_free / 1024, psram_total / 1024,
                 ((psram_total - psram_free) * 100.0) / psram_total);
    }
}

/* ========================== Main Application ========================== */

void app_main(void)
{
    ESP_LOGI(TAG, "=== Robust Audio Streaming System Starting ===");
    
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
                g_state.mode = MODE_CATCHING_UP;
                // Need to mount SD for reading
                sd_card_mount();
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