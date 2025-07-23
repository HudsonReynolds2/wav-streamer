#include <stdio.h>
#include <string.h>
#include <time.h>
#include "mm_app_common.h"      // for app_wlan_init / app_wlan_start
#include "esp_log.h"
#include "esp_system.h"
#include "esp_http_client.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "mmosal.h"
#include "mmwlan.h"
#include "mmipal.h"

#include "esp_event.h"
#include "esp_netif.h"

// Server Configuration - CHANGE THESE!
#define SERVER_URL     "http://192.168.137.142:8000/recordings_upload"
#define LISTENER_ID    "esp32_01"

static const char *TAG = "wav-streamer";

// Embedded WAV file from binary
extern const uint8_t _binary_testing1_wav_start[];
extern const uint8_t _binary_testing1_wav_end[];

static void generate_timestamp(char *timestamp, size_t len)
{
    time_t now = time(NULL);
    struct tm timeinfo = *gmtime(&now);
    strftime(timestamp, len, "%Y%m%dT%H%M%SZ", &timeinfo);
}

static void generate_multipart_body(char **body, size_t *body_len, const char *filename)
{
    const char *boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW";
    size_t wav_size = _binary_testing1_wav_end - _binary_testing1_wav_start;

    char part1[256], part2[512];

    int part1_len = snprintf(part1, sizeof(part1),
        "--%s\r\n"
        "Content-Disposition: form-data; name=\"listener_id\"\r\n\r\n"
        "%s\r\n", boundary, LISTENER_ID);

    int part2_header_len = snprintf(part2, sizeof(part2),
        "--%s\r\n"
        "Content-Disposition: form-data; name=\"file\"; filename=\"%s\"\r\n"
        "Content-Type: audio/wav\r\n\r\n", boundary, filename);

    int end_len = snprintf(part2 + part2_header_len, sizeof(part2) - part2_header_len,
        "\r\n--%s--\r\n", boundary);

    size_t total_size = part1_len + part2_header_len + wav_size + end_len;

    *body = malloc(total_size);
    if (!*body) {
        ESP_LOGE(TAG, "Failed to allocate multipart buffer");
        return;
    }

    char *ptr = *body;
    memcpy(ptr, part1, part1_len); ptr += part1_len;
    memcpy(ptr, part2, part2_header_len); ptr += part2_header_len;
    memcpy(ptr, _binary_testing1_wav_start, wav_size); ptr += wav_size;
    memcpy(ptr, part2 + part2_header_len, end_len);  // write tail
    *body_len = total_size;
}

static void upload_wav_file(void)
{
    char timestamp[32];
    generate_timestamp(timestamp, sizeof(timestamp));

    char filename[64];
    snprintf(filename, sizeof(filename), "%s_%s.wav", LISTENER_ID, timestamp);

    ESP_LOGI(TAG, "Uploading file: %s", filename);

    size_t wav_size = _binary_testing1_wav_end - _binary_testing1_wav_start;
    ESP_LOGI(TAG, "WAV file size: %d bytes", wav_size);

    char *body = NULL;
    size_t body_len = 0;
    generate_multipart_body(&body, &body_len, filename);

    if (body == NULL) {
        ESP_LOGE(TAG, "Failed to generate multipart body");
        return;
    }

    esp_http_client_config_t config = {
        .url = SERVER_URL,
        .method = HTTP_METHOD_POST,
        .timeout_ms = 30000,
    };

    esp_http_client_handle_t client = esp_http_client_init(&config);

    // Set headers
    esp_http_client_set_header(client, "Content-Type",
        "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW");

    // Set post data
    esp_http_client_set_post_field(client, body, body_len);

    // Perform the request
    esp_err_t err = esp_http_client_perform(client);

    if (err == ESP_OK) {
        int status = esp_http_client_get_status_code(client);
        int content_length = esp_http_client_get_content_length(client);
        ESP_LOGI(TAG, "HTTP POST Status = %d, content_length = %d", status, content_length);

        if (status == 200) {
            ESP_LOGI(TAG, "File uploaded successfully!");
        } else {
            ESP_LOGE(TAG, "Upload failed with status code: %d", status);
        }
    } else {
        ESP_LOGE(TAG, "HTTP POST request failed: %s", esp_err_to_name(err));
    }

    esp_http_client_cleanup(client);
    free(body);
}

void app_main(void)
{
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    app_wlan_init();
    app_wlan_start();

    mmwlan_set_power_save_mode(MMWLAN_PS_DISABLED);
    mmwlan_set_wnm_sleep_enabled(false);

    size_t wav_size = _binary_testing1_wav_end - _binary_testing1_wav_start;
    ESP_LOGI(TAG, "Embedded WAV size: %d bytes", wav_size);

    upload_wav_file();

    while (1) {
        vTaskDelay(30000 / portTICK_PERIOD_MS);
        ESP_LOGI(TAG, "Uploading again...");
        upload_wav_file();
    }
}
