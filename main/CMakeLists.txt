idf_component_register(
    SRCS "mm_app_loadconfig.c" "mm_app_common.c" "main.c"
    INCLUDE_DIRS "."
    PRIV_REQUIRES
        driver
        morselib
        mm_shims
        mmipal
        mmiperf
        lwip
        esp_http_client
        esp_netif
        esp_event
        usb
        esp_timer
        fatfs
        vfs
        sdmmc
        nvs_flash
        esp_psram
)
# Embed the test file into the binary
# target_add_binary_data(${COMPONENT_TARGET} "testing1.wav" BINARY)