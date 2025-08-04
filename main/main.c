// AudioMoth USB -> HTTP chunked streamer with URB-level batching
// ESP-IDF v5.4.x compatible

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


#include <sys/unistd.h>
#include <sys/stat.h>
#include "esp_vfs_fat.h"
#include "sdmmc_cmd.h"

#define PIN_SD_MISO 8
#define PIN_SD_MOSI 9
#define PIN_SD_CLK  7
#define PIN_SD_CS   21  // HaLow CS is 4

#define MOUNT_POINT "/sdcard"

/* -------------------------- HTTP streaming -------------------------- */

#define SERVER_URL          "http://192.168.137.254:8000/recordings_stream"
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

/* -------------------------- app_main -------------------------- */

void app_main(void)
{
    // Try to add MicroSD code here:
    esp_err_t ret;

    // Options for mounting the filesystem.
    // If format_if_mount_failed is set to true, SD card will be partitioned and
    // formatted in case when mounting fails.
    esp_vfs_fat_sdmmc_mount_config_t mount_config = {
        .format_if_mount_failed = false,
        .max_files = 5,
        .allocation_unit_size = 16 * 1024
    };
    sdmmc_card_t *card;
    const char mount_point[] = MOUNT_POINT;
    ESP_LOGI(TAG, "Initializing SD card");
    ESP_LOGI(TAG, "Using SPI peripheral");
    sdmmc_host_t host = SDSPI_HOST_DEFAULT();

    // Force release SPI2 in case another driver grabbed it
    spi_bus_free(host.slot);

    spi_bus_config_t bus_cfg = {
        .mosi_io_num = PIN_SD_MOSI,
        .miso_io_num = PIN_SD_MISO,
        .sclk_io_num = PIN_SD_CLK,
        .quadwp_io_num = -1,
        .quadhd_io_num = -1,
        .max_transfer_sz = 4000,
    };

    ret = spi_bus_initialize(host.slot, &bus_cfg, SDSPI_DEFAULT_DMA);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "Failed to initialize bus.");
        return;
    }

    // This initializes the slot without card detect (CD) and write protect (WP) signals.
    // Modify slot_config.gpio_cd and slot_config.gpio_wp if your board has these signals.
    sdspi_device_config_t slot_config = SDSPI_DEVICE_CONFIG_DEFAULT();
    slot_config.gpio_cs = PIN_SD_CS;
    slot_config.host_id = host.slot;

    ESP_LOGI(TAG, "Mounting filesystem but first putting HaLow CS to High");
    // Force HaLow module's CS high so it ignores SD card SPI traffic
    gpio_set_direction(GPIO_NUM_4, GPIO_MODE_OUTPUT);
    gpio_set_level(GPIO_NUM_4, 1);
    ret = esp_vfs_fat_sdspi_mount(mount_point, &host, &slot_config, &mount_config, &card);

    if (ret != ESP_OK) {
        if (ret == ESP_FAIL) {
            ESP_LOGE(TAG, "Failed to mount filesystem. "
                     "If you want the card to be formatted, set the CONFIG_EXAMPLE_FORMAT_IF_MOUNT_FAILED menuconfig option.");
        } else {
            ESP_LOGE(TAG, "Failed to initialize the card (%s). "
                     "Make sure SD card lines have pull-up resistors in place.", esp_err_to_name(ret));
        }
        return;
    }
    ESP_LOGI(TAG, "Filesystem mounted");

    // Card has been initialized, print its properties
    sdmmc_card_print_info(stdout, card);

    // All done, unmount partition and disable SPI peripheral
    esp_vfs_fat_sdcard_unmount(mount_point, card);
    ESP_LOGI(TAG, "Card unmounted");

    //deinitialize the bus after all devices are removed
    spi_bus_free(host.slot);


    ESP_ERROR_CHECK(esp_event_loop_create_default());

    app_wlan_init();
    app_wlan_start();

    mmwlan_set_power_save_mode(MMWLAN_PS_DISABLED);
    mmwlan_set_wnm_sleep_enabled(false);

    vTaskDelay(pdMS_TO_TICKS(2000));

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

    while (1) {
        vTaskDelay(pdMS_TO_TICKS(30000));
    }
}