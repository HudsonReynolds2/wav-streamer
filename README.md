# WAV Streamer
Working repo for streaming .WAV files to an Aggregator from a Listener (ESP32S3) over WiFi HaLow.
Uses Seeed studios Xiao ESP32S3Plus and Xiao WiFi HaLow Transceiver.

Now we successfully get data from AudioMoth Dev (running AudioMoth-USB-Firmware) onto ESP32S3 over USB,
and upload to cloud over WiFi HaLow in real time, no microSD involved for now.

## How to use example

## Folder contents

The project **sample_project** contains one source file in C language [main.c](main/main.c). The file is located in folder [main](main).

ESP-IDF projects are built using CMake. The project build configuration is contained in `CMakeLists.txt`
files that provide set of directives and instructions describing the project's source files and targets
(executable, library, or both). 

Below is short explanation of remaining files in the project folder.

```
├── CMakeLists.txt
├── main
│   ├── CMakeLists.txt
│   └── main.c
└── README.md                  This is the file you are currently reading
```
Additionally, the sample project contains Makefile and component.mk files, used for the legacy Make based build system. 
They are not used or needed when building with CMake and idf.py.
