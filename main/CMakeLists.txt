idf_component_register(
    SRCS "main.c"
    INCLUDE_DIRS "."
    REQUIRES
        halow
        nvs_flash
        driver
        esp_http_client
        esp_netif
        esp_event
        lwip
        usb
        esp_timer
        fatfs
        vfs
        sdmmc
        esp_psram
)