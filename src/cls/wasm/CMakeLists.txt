cmake_minimum_required (VERSION 2.6)
project (WasmerCApiExample)
add_executable(wasmer-c-api-example /mnt/sda4/skyhookdm-ceph/src/cls/wasm/wasmer-c-api-example.c)
add_executable(wasmer-cpp-api-example /mnt/sda4/skyhookdm-ceph/src/cls/wasm/wasmer-cpp-api-example.cc)
add_library(cls_wasm SHARED /mnt/sda4/skyhookdm-ceph/src/cls/wasm/cls_wasm.cc)
add_executable(wasmer_sum /mnt/sda4/skyhookdm-ceph/src/cls/wasm/wasmer_sum.c)
add_executable(wasmer_sum_cpp /mnt/sda4/skyhookdm-ceph/src/cls/wasm/wasmer_sum.cc)

include(ExternalProject)
set_directory_properties(PROPERTIES EP_PREFIX ${CMAKE_BINARY_DIR}/rust-build)
ExternalProject_Add(
        wasmer-runtime-c-api
        DOWNLOAD_DIR /mnt/sda4/skyhookdm-ceph/src/cls/wasm/build
        GIT_REPOSITORY https://github.com/wasmerio/wasmer.git
        GIT_TAG origin/master
        CONFIGURE_COMMAND ""
        BUILD_COMMAND cargo build -p wasmer-runtime-c-api
        COMMAND cargo build -p wasmer-runtime-c-api
        BINARY_DIR "/mnt/sda4/skyhookdm-ceph/src/cls/wasm/rust-build/src/wasmer-runtime-c-api/"
        INSTALL_COMMAND ""
        LOG_BUILD ON)
add_dependencies(wasmer-c-api-example wasmer-runtime-c-api)
add_dependencies(wasmer-cpp-api-example wasmer-runtime-c-api)
add_dependencies(cls_wasm wasmer-runtime-c-api)
add_dependencies(wasmer_sum wasmer-runtime-c-api)
add_dependencies(wasmer_sum_cpp wasmer-runtime-c-api)

if(WIN32)
    set(WASMER_LIB "${CMAKE_SOURCE_DIR}/rust-build/src/wasmer-runtime-c-api/target/debug/wasmer_runtime_c_api.dll")
else()
    set(WASMER_LIB "/mnt/sda4/skyhookdm-ceph/src/cls/wasm/rust-build/src/wasmer-runtime-c-api/target/debug/libwasmer_runtime_c_api${CMAKE_SHARED_LIBRARY_SUFFIX}")
endif()

if(NOT WASMER_LIB)
    message(FATAL_ERROR "wasmer library not found")
endif()

target_link_libraries(wasmer-c-api-example general ${WASMER_LIB})
target_link_libraries(wasmer-cpp-api-example general ${WASMER_LIB})
target_link_libraries(cls_wasm general ${WASMER_LIB})
target_link_libraries(wasmer_sum general ${WASMER_LIB})
target_link_libraries(wasmer_sum_cpp general ${WASMER_LIB})
