// AudioMoth USB -> HTTP chunked streamer with URB-level batching
// ESP-IDF v5.1.1 compatible

#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"

#include "esp_log.h"
#include "esp_err.h"
#include "esp_check.h"
#include "esp_timer.h"
#include "esp_event.h"
#include "esp_system.h"
#include "esp_netif.h"
#include "esp_http_client.h"
#include "esp_timer.h"

#include "mm_app_common.h"
#include "mmosal.h"
#include "mmwlan.h"
#include "mmipal.h"

#include "usb/usb_host.h"
#include "usb/usb_types_stack.h"
#include "usb/usb_helpers.h"
#include "usb/usb_types_ch9.h"

// ADD THESE INCLUDES FOR GPIO MIRRORING
#include "driver/gpio.h"
#include "esp_rom_gpio.h"
#include "hal/gpio_types.h"
#include "soc/gpio_sig_map.h"

#include <sys/unistd.h>
#include <sys/stat.h>
#include "esp_vfs_fat.h"
#include "sdmmc_cmd.h"

#define PIN_SD_MISO 8
#define PIN_SD_MOSI 9
#define PIN_SD_CLK  7
#define PIN_SD_CS   21  // HaLow CS is 4

#define PIN_MIRROR_CS 6  // ADD THIS: GPIO 6 will mirror GPIO 21

#define MOUNT_POINT "/sdcard"

/* -------------------------- HTTP streaming -------------------------- */

#define SERVER_URL          "http://192.168.137.138:8000/recordings_stream"
#define LISTENER_ID         "esp32_01_outdoor_test"

#define FRAME_HEADER_SIZE   6   // 3 bytes seq + 3 bytes length

static const char *TAG = "am_usb_stream_batch";

/* Simple streaming context */
typedef struct {
    esp_http_client_handle_t client;
    uint32_t sequence;
    bool is_connected;
    SemaphoreHandle_t mutex;
} streaming_context_t;

static streaming_context_t stream_ctx = {0};

static int write_chunked_vec(esp_http_client_handle_t client,
                             const uint8_t *hdr, size_t hdr_len,
                             const uint8_t *payload, size_t pay_len)
{
    size_t total = hdr_len + pay_len;
    char size_str[12];
    int size_len = snprintf(size_str, sizeof(size_str), "%X\r\n", (unsigned int)total);
    if (esp_http_client_write(client, size_str, size_len) != size_len) return -1;

    if (hdr_len && esp_http_client_write(client, (const char*)hdr, hdr_len) != (int)hdr_len) return -1;
    if (pay_len && esp_http_client_write(client, (const char*)payload, pay_len) != (int)pay_len) return -1;

    if (esp_http_client_write(client, "\r\n", 2) != 2) return -1;
    return (int)total;
}

static int write_chunked_end(esp_http_client_handle_t client)
{
    return (esp_http_client_write(client, "0\r\n\r\n", 5) == 5) ? 0 : -1;
}

static esp_err_t stream_init(void)
{
    if (stream_ctx.client != NULL) {
        ESP_LOGW(TAG, "Stream already initialized");
        return ESP_OK;
    }

    stream_ctx.mutex = xSemaphoreCreateMutex();
    if (!stream_ctx.mutex) {
        ESP_LOGE(TAG, "Failed to create mutex");
        return ESP_ERR_NO_MEM;
    }

    char url[256];
    snprintf(url, sizeof(url), "%s?listener_id=%s", SERVER_URL, LISTENER_ID);

    esp_http_client_config_t config = {
        .url = url,
        .method = HTTP_METHOD_POST,
        .timeout_ms = 0,
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
    esp_err_t err = esp_http_client_open(stream_ctx.client, -1); // -1 => chunked
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed to open connection: %s", esp_err_to_name(err));
        esp_http_client_cleanup(stream_ctx.client);
        stream_ctx.client = NULL;
        vSemaphoreDelete(stream_ctx.mutex);
        stream_ctx.mutex = NULL;
        return err;
    }

    stream_ctx.sequence = 0;
    stream_ctx.is_connected = true;

    ESP_LOGI(TAG, "Streaming connection established");
    return ESP_OK;
}

static esp_err_t stream_send_frame(const uint8_t *data, size_t data_len)
{
    if (!stream_ctx.is_connected) return ESP_FAIL;
    if (xSemaphoreTake(stream_ctx.mutex, pdMS_TO_TICKS(1000)) != pdTRUE) return ESP_ERR_TIMEOUT;

    uint8_t hdr[FRAME_HEADER_SIZE];
    hdr[0] = stream_ctx.sequence & 0xFF;
    hdr[1] = (stream_ctx.sequence >> 8) & 0xFF;
    hdr[2] = (stream_ctx.sequence >> 16) & 0xFF;
    hdr[3] = data_len & 0xFF;
    hdr[4] = (data_len >> 8) & 0xFF;
    hdr[5] = (data_len >> 16) & 0xFF;

    int result = write_chunked_vec(stream_ctx.client, hdr, FRAME_HEADER_SIZE, data, data_len);
    if (result < 0) {
        stream_ctx.is_connected = false;
        xSemaphoreGive(stream_ctx.mutex);
        return ESP_FAIL;
    }

    stream_ctx.sequence = (stream_ctx.sequence + 1) & 0xFFFFFF;
    xSemaphoreGive(stream_ctx.mutex);
    return ESP_OK;
}

static esp_err_t stream_close(void)
{
    if (!stream_ctx.client) return ESP_OK;

    ESP_LOGI(TAG, "Closing stream...");

    if (xSemaphoreTake(stream_ctx.mutex, pdMS_TO_TICKS(5000)) != pdTRUE) {
        ESP_LOGE(TAG, "Failed to take mutex");
        return ESP_ERR_TIMEOUT;
    }

    write_chunked_end(stream_ctx.client);

    esp_err_t err = esp_http_client_fetch_headers(stream_ctx.client);
    if (err >= 0) {
        int status = esp_http_client_get_status_code(stream_ctx.client);
        ESP_LOGI(TAG, "HTTP Status: %d", status);
        char response[256];
        int read_len = esp_http_client_read(stream_ctx.client, response, sizeof(response) - 1);
        if (read_len > 0) {
            response[read_len] = '\0';
            ESP_LOGI(TAG, "Server response: %s", response);
        }
    } else {
        ESP_LOGW(TAG, "Failed to fetch headers: %s", esp_err_to_name(err));
    }

    esp_http_client_close(stream_ctx.client);
    esp_http_client_cleanup(stream_ctx.client);
    stream_ctx.client = NULL;
    stream_ctx.is_connected = false;
    stream_ctx.sequence = 0;

    xSemaphoreGive(stream_ctx.mutex);
    vSemaphoreDelete(stream_ctx.mutex);
    stream_ctx.mutex = NULL;
    return ESP_OK;
}

/* -------------------------- USB host (AudioMoth) -------------------------- */

#define ISO_MPS              96
#define ISO_PKTS_PER_URB     16
#define NUM_ISO_URBS         3

typedef struct {
    int      mps;
    uint8_t *batch_buf;
    size_t   batch_cap;
} iso_cb_ctx_t;

static usb_host_client_handle_t g_client;
static usb_device_handle_t      g_dev;
static SemaphoreHandle_t        ctrl_sem;
static usb_transfer_t          *s_iso_urbs[NUM_ISO_URBS] = {0};
static iso_cb_ctx_t            *s_iso_ctxs[NUM_ISO_URBS] = {0};

static uint64_t g_pkt_cnt = 0;
static uint64_t g_byte_cnt = 0;
static int64_t  g_last_log_us = 0;

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
    xfer->bEndpointAddress  = 0; // EP0
    xfer->num_bytes         = sizeof(*setup);
    xfer->callback          = ctrl_cb;
    xfer->context           = NULL;

    while (xSemaphoreTake(ctrl_sem, 0) == pdTRUE) { /* drain */ }
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

    const int64_t now_us = esp_timer_get_time();
    static int16_t last_first_sample = 0;

    for (int i = 0; i < t->num_isoc_packets; i++) {
        const usb_isoc_packet_desc_t *d = &t->isoc_packet_desc[i];
        if (d->status == USB_TRANSFER_STATUS_COMPLETED && d->actual_num_bytes) {
            const uint8_t *pcm_bytes = (const uint8_t *)(t->data_buffer + off);
            last_first_sample = ((const int16_t *)pcm_bytes)[0];

            if ((out + d->actual_num_bytes) <= c->batch_cap) {
                memcpy(c->batch_buf + out, pcm_bytes, d->actual_num_bytes);
                out += d->actual_num_bytes;
            } else {
                // Shouldn't happen if batch_cap = mps * ISO_PKTS_PER_URB,
                // but guard just in case.
                ESP_LOGW(TAG, "Batch buffer overflow, dropping remainder");
                break;
            }

            g_pkt_cnt++;
            g_byte_cnt += d->actual_num_bytes;
        }
        off += mps;
    }

    // Ship once per URB
    if (out > 0 && stream_ctx.is_connected) {
        esp_err_t se = stream_send_frame(c->batch_buf, out);
        if (se != ESP_OK) {
            ESP_LOGE(TAG, "stream_send_frame failed: %s", esp_err_to_name(se));
        }
    }

    if (now_us - g_last_log_us > 500000) {
        float kbps = (g_byte_cnt * 8.0f) / ((now_us - g_last_log_us) / 1000.0f);
        ESP_LOGI(TAG, "pkts=%llu bytes=%llu ~%.1f kbps first_sample=%d (batched)",
                 (unsigned long long)g_pkt_cnt,
                 (unsigned long long)g_byte_cnt,
                 kbps,
                 last_first_sample);
        g_pkt_cnt = 0;
        g_byte_cnt = 0;
        g_last_log_us = now_us;
    }

    // Re-submit THIS URB immediately
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

        iso_cb_ctx_t *ctx = calloc(1, sizeof(*ctx));
        if (!ctx) return ESP_ERR_NO_MEM;
        ctx->mps = mps;
        ctx->batch_cap = buf_size;
        ctx->batch_buf = malloc(buf_size);
        if (!ctx->batch_buf) {
            free(ctx);
            return ESP_ERR_NO_MEM;
        }
        s_iso_ctxs[u] = ctx;

        xfer->device_handle    = g_dev;
        xfer->bEndpointAddress = ep_addr;
        xfer->callback         = isoc_in_cb;
        xfer->context          = ctx;          // pass ctx instead of mps
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

        // Claim IF=1 ALT=1 (AudioMoth)
        ESP_ERROR_CHECK(usb_host_interface_claim(g_client, g_dev, 1, 1));
        ESP_ERROR_CHECK(ctrl_set_interface(1, 1));

        esp_err_t err = start_isoc_stream(0x82, ISO_MPS);
        if (err != ESP_OK) {
            ESP_LOGE(TAG, "start_isoc_stream failed: %s", esp_err_to_name(err));
        }
        break;
    }
    case USB_HOST_CLIENT_EVENT_DEV_GONE:
        ESP_LOGW(TAG, "DEV_GONE (TODO: stop stream, free ctxs/URBs)");
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

/* ========================== Global Variables for SPI Bus Management ========================== */

static SemaphoreHandle_t spi_bus_mutex = NULL;
static sdmmc_card_t *sd_card = NULL;
static bool sd_mounted = false;
static sdmmc_host_t sd_host = SDSPI_HOST_DEFAULT();
static sdspi_device_config_t sd_slot_config = SDSPI_DEVICE_CONFIG_DEFAULT();

/* ========================== SPI Bus Configuration ========================== */

static const spi_bus_config_t spi_bus_cfg = {
    .mosi_io_num = PIN_SD_MOSI,
    .miso_io_num = PIN_SD_MISO,
    .sclk_io_num = PIN_SD_CLK,
    .quadwp_io_num = -1,
    .quadhd_io_num = -1,
    .max_transfer_sz = 4000,
    .flags = 0,
    .intr_flags = ESP_INTR_FLAG_IRAM  // Use IRAM safe interrupt
};

static const esp_vfs_fat_sdmmc_mount_config_t sd_mount_config = {
    .format_if_mount_failed = false,
    .max_files = 5,
    .allocation_unit_size = 16 * 1024
};

/* ========================== GPIO Control Functions ========================== */

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

/* ========================== SPI Bus Management Functions ========================== */

static esp_err_t acquire_spi_bus_for_sd_and_stop_wifi(void)
{
    esp_err_t ret;
    
    ESP_LOGI(TAG, "Acquiring SPI bus for SD card...");
    
    // Take mutex to ensure exclusive access
    if (xSemaphoreTake(spi_bus_mutex, pdMS_TO_TICKS(5000)) != pdTRUE) {
        ESP_LOGE(TAG, "Failed to acquire SPI bus mutex");
        return ESP_ERR_TIMEOUT;
    }
    
    // Stop HaLow to release the bus
    ESP_LOGI(TAG, "Stopping HaLow to release SPI bus...");
    app_wlan_stop();
    
    // Small delay to ensure clean shutdown
    vTaskDelay(pdMS_TO_TICKS(200));
    
    // CRITICAL: Force HaLow module's CS high so it ignores SD card SPI traffic
    force_halow_cs_high();
    
    // Try to initialize SPI bus for SD card
    // First try to free in case it wasn't properly released
    spi_bus_free(sd_host.slot);
    
    ret = spi_bus_initialize(sd_host.slot, &spi_bus_cfg, SDSPI_DEFAULT_DMA);
    if (ret != ESP_OK && ret != ESP_ERR_INVALID_STATE) {
        ESP_LOGE(TAG, "Failed to initialize SPI bus: %s", esp_err_to_name(ret));
        xSemaphoreGive(spi_bus_mutex);
        return ret;
    }
    
    ESP_LOGI(TAG, "SPI bus acquired for SD card");
    return ESP_OK;
}

static esp_err_t release_spi_bus_from_sd_and_restart_wifi(void)
{
    esp_err_t ret;
    
    ESP_LOGI(TAG, "Releasing SPI bus from SD card...");
    
    // Unmount SD card if mounted
    if (sd_mounted) {
        ret = esp_vfs_fat_sdcard_unmount(MOUNT_POINT, sd_card);
        if (ret == ESP_OK) {
            sd_mounted = false;
            sd_card = NULL;
            ESP_LOGI(TAG, "SD card unmounted");
        } else {
            ESP_LOGW(TAG, "Failed to unmount SD card: %s", esp_err_to_name(ret));
        }
    }
    
    // Free the SPI bus
    ret = spi_bus_free(sd_host.slot);
    if (ret != ESP_OK && ret != ESP_ERR_INVALID_STATE) {
        ESP_LOGW(TAG, "Issue freeing SPI bus: %s", esp_err_to_name(ret));
    }
    
    // Small delay before HaLow restart
    vTaskDelay(pdMS_TO_TICKS(200));
    
    // Restart HaLow
    ESP_LOGI(TAG, "Restarting HaLow module...");
    app_wlan_start();
    mmwlan_set_power_save_mode(MMWLAN_PS_DISABLED);
    mmwlan_set_wnm_sleep_enabled(false);
    
    // Release mutex
    xSemaphoreGive(spi_bus_mutex);
    
    ESP_LOGI(TAG, "SPI bus released, HaLow restarted");
    return ESP_OK;
}

/* ========================== SD Card Functions ========================== */

static esp_err_t mount_sd_card(void)
{
    esp_err_t ret;
    
    if (sd_mounted) {
        ESP_LOGW(TAG, "SD card already mounted");
        return ESP_OK;
    }
    
    sd_slot_config.gpio_cs = PIN_SD_CS;
    sd_slot_config.host_id = sd_host.slot;
    
    ret = esp_vfs_fat_sdspi_mount(MOUNT_POINT, &sd_host, &sd_slot_config, 
                                   &sd_mount_config, &sd_card);
    if (ret == ESP_OK) {
        sd_mounted = true;
        ESP_LOGI(TAG, "SD card mounted successfully");
        sdmmc_card_print_info(stdout, sd_card);
    } else {
        ESP_LOGE(TAG, "Failed to mount SD card: %s", esp_err_to_name(ret));
    }
    return ret;
}

static void test_sd_operations(void)
{
    ESP_LOGI(TAG, "Performing SD card operations...");
    
    // Write test
    FILE* f = fopen(MOUNT_POINT"/test.txt", "w");
    if (f == NULL) {
        ESP_LOGE(TAG, "Failed to open file for writing");
    } else {
        fprintf(f, "SD Card test at %lld\n", esp_timer_get_time());
        fclose(f);
        ESP_LOGI(TAG, "File written successfully");
    }
    
    // Read test
    f = fopen(MOUNT_POINT"/test.txt", "r");
    if (f == NULL) {
        ESP_LOGE(TAG, "Failed to open file for reading");
    } else {
        char line[64];
        fgets(line, sizeof(line), f);
        fclose(f);
        ESP_LOGI(TAG, "Read: %s", line);
    }
    
    // Create buffer file for audio streaming
    f = fopen(MOUNT_POINT"/audio_buffer.bin", "wb");
    if (f != NULL) {
        ESP_LOGI(TAG, "Created audio buffer file");
        fclose(f);
    }
}

static esp_err_t perform_sd_card_operation(void)
{
    esp_err_t ret;
    
    // Acquire bus
    ret = acquire_spi_bus_for_sd_and_stop_wifi();
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "Failed to acquire SPI bus for SD card");
        return ret;
    }
    
    // Mount SD card
    ret = mount_sd_card();
    if (ret == ESP_OK) {
        // Perform operations
        test_sd_operations();
    }
    
    // Always release bus (this also unmounts SD)
    release_spi_bus_from_sd_and_restart_wifi();
    
    return ret;
}

/* ========================== Initialization Functions ========================== */

static esp_err_t initialize_sd_card_first(void)
{
    esp_err_t ret;
    
    ESP_LOGI(TAG, "Initializing SD card first (before HaLow)...");
    
    // MANDATORY SEQUENCE - DO NOT CHANGE ORDER:
    // 1. Force release SPI bus in case another driver grabbed it
    // 2. Hold HaLow CS high to prevent conflicts
    // 3. Initialize and test SD card
    // 4. Free bus for HaLow to use
    // This sequence is critical for proper operation
    
    // Step 1: Force release SPI bus
    ESP_LOGI(TAG, "Freeing SPI bus before SD card init (mandatory step)");
    spi_bus_free(sd_host.slot);
    
    // Step 2: Force HaLow module's CS high so it ignores SD card SPI traffic
    force_halow_cs_high();
    
    // Step 3: Initialize SPI bus for SD card
    ret = spi_bus_initialize(sd_host.slot, &spi_bus_cfg, SDSPI_DEFAULT_DMA);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "Failed to initialize SPI bus for SD: %s", esp_err_to_name(ret));
        return ret;
    }
    
    ESP_LOGI(TAG, "SPI bus initialized for SD card");
    
    // Mount and test SD card
    ret = mount_sd_card();
    if (ret == ESP_OK) {
        ESP_LOGI(TAG, "SD card mounted successfully on startup");
        test_sd_operations();
        
        // Unmount SD card
        ret = esp_vfs_fat_sdcard_unmount(MOUNT_POINT, sd_card);
        if (ret == ESP_OK) {
            sd_mounted = false;
            sd_card = NULL;
            ESP_LOGI(TAG, "SD card unmounted after initial test");
        }
    } else {
        ESP_LOGE(TAG, "Failed to mount SD card on startup");
    }
    
    // Step 4: Free the SPI bus so HaLow can use it
    ret = spi_bus_free(sd_host.slot);
    if (ret != ESP_OK) {
        ESP_LOGW(TAG, "Issue freeing SPI bus after SD init: %s", esp_err_to_name(ret));
    }
    
    ESP_LOGI(TAG, "SPI bus freed, ready for HaLow initialization");
    return ESP_OK;
}

static void initialize_system(void)
{
    // Create mutex for SPI bus coordination
    spi_bus_mutex = xSemaphoreCreateMutex();
    if (spi_bus_mutex == NULL) {
        ESP_LOGE(TAG, "Failed to create SPI bus mutex");
        return;
    }
    
    // Set up GPIO mirroring
    setup_gpio_mirroring();
    
    // Initialize event loop
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    
    // MANDATORY: Initialize SD card FIRST before HaLow
    initialize_sd_card_first();
    
    // Small delay before HaLow init
    vTaskDelay(pdMS_TO_TICKS(200));
    
    // Now initialize HaLow module (it will take over the SPI bus)
    ESP_LOGI(TAG, "Initializing HaLow module...");
    app_wlan_init();
}

static void start_wifi_halow(void)
{
    ESP_LOGI(TAG, "Starting HaLow WiFi...");
    app_wlan_start();
    mmwlan_set_power_save_mode(MMWLAN_PS_DISABLED);
    mmwlan_set_wnm_sleep_enabled(false);
}

static void countdown_delay(int seconds, const char* message)
{
    ESP_LOGI(TAG, "%s", message);
    for (int i = seconds; i >= 0; i--) {
        printf("Countdown: %d\n", i);
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}

/* ========================== Main Application ========================== */

void app_main(void)
{
    // Initialize system components (SD card first, then HaLow)
    initialize_system();
    
    // ===== Main test sequence =====
    
    ESP_LOGI(TAG, "=== Starting HaLow WiFi after SD initialization ===");
    start_wifi_halow();
    
    // Now WiFi is connected, must detect network problems in order to switch to MicroSD or not
    // Connection, speed, access to streaming endpoint?
    // If not good enough, must switch to MicroSD and periodically check network
    // Use PSRAM (8MB) and the other ram to limit MicroSD wear and handle meantime: verify how to write properly to card
    // After enough writing to a card (enough time), switch back to network and keep buffering in PSRAM
    // If network is good, upload buffer, if network is not, time out and switch back to MicroSD


    
    //ESP_LOGI(TAG, "=== Stopping WiFi and Testing SD Card Access (test 1) ===");
    //perform_sd_card_operation();
    
    // Wait a bit with WiFi running
    //countdown_delay(10, "WiFi running again, waiting...");
    
    ESP_LOGI(TAG, "=== Stopping WiFi and Testing SD Card Access (test 2) ===");
    perform_sd_card_operation();
    
    ESP_LOGI(TAG, "=== Final WiFi state ===");
    ESP_LOGI(TAG, "WiFi HaLow is running and ready for streaming");
    
    // Uncomment when ready to test USB streaming
    ESP_ERROR_CHECK(stream_init());
    ctrl_sem = xSemaphoreCreateBinary();
    const usb_host_config_t host_cfg = {
         .skip_phy_setup = false,
         .intr_flags = 0,
    };
    ESP_ERROR_CHECK(usb_host_install(&host_cfg));
    xTaskCreatePinnedToCore(daemon_task, "usb_daemon", 4096, NULL, 3, NULL, 0);
    xTaskCreatePinnedToCore(client_task, "usb_client", 8192, NULL, 4, NULL, 1);
    ESP_LOGI(TAG, "AudioMoth->HTTP streaming (batched) started");
    
    ESP_LOGI(TAG, "=== Main initialization complete ===");
    
    // Main loop - could periodically buffer to SD when network is down
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(1000));
        
        // Example of periodic SD buffering (uncomment when needed):
        // if (network_is_down() || stream_ctx.is_connected == false) {
        //     ESP_LOGI(TAG, "Network issue detected, buffering to SD...");
        //     perform_sd_card_operation();
        // }
    }
}