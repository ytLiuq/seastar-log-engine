cmake_minimum_required(VERSION 3.20)

project(seastar_log_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(SEASTAR_BUILD_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/../seastar/build/release"
    CACHE PATH
    "Path to a Seastar build directory containing SeastarConfig.cmake")

list(APPEND CMAKE_MODULE_PATH
    "${CMAKE_CURRENT_SOURCE_DIR}/../seastar/cmake")

find_package(Seastar REQUIRED CONFIG PATHS "${SEASTAR_BUILD_DIR}" NO_DEFAULT_PATH)
find_package(ZLIB REQUIRED)
find_package(fmt QUIET CONFIG)
find_package(spdlog QUIET CONFIG)
find_library(SYSTEM_FMT_LIBRARY fmt PATHS /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu NO_DEFAULT_PATH)
find_library(FMT_LIBRARY fmt)
find_library(GLOG_LIBRARY glog)

add_library(log_engine_core
    src/async_writer.cc
    src/compat_glog.cc
    src/config_loader.cc
    src/log_manager.cc
    src/log_reader.cc
    src/log_engine.cc
    src/record_codec.cc)

target_include_directories(log_engine_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(log_engine_core
    PUBLIC
        Seastar::seastar
        ZLIB::ZLIB)

add_executable(log_engine_demo
    src/main.cc)

target_link_libraries(log_engine_demo
    PRIVATE
        log_engine_core)

add_executable(log_engine_bench
    src/bench.cc)

target_link_libraries(log_engine_bench
    PRIVATE
        log_engine_core)

add_executable(log_engine_verify
    src/verify.cc)

target_link_libraries(log_engine_verify
    PRIVATE
        log_engine_core)

add_executable(log_engine_read
    src/read.cc)

target_link_libraries(log_engine_read
    PRIVATE
        log_engine_core)

add_executable(log_engine_unit_tests
    src/unit_tests.cc)

target_link_libraries(log_engine_unit_tests
    PRIVATE
        log_engine_core)

if (GLOG_LIBRARY)
    add_executable(glog_bench
        src/glog_bench.cc)
    target_link_libraries(glog_bench
        PRIVATE
            ${GLOG_LIBRARY})
endif ()

if (spdlog_FOUND)
    add_executable(spdlog_bench
        src/spdlog_bench.cc)
    target_include_directories(spdlog_bench
        BEFORE PRIVATE
            /usr/include)
    if (TARGET spdlog::spdlog)
        target_link_libraries(spdlog_bench
            PRIVATE
                spdlog::spdlog)
    elseif (TARGET spdlog::spdlog_header_only)
        target_link_libraries(spdlog_bench
            PRIVATE
                spdlog::spdlog_header_only)
    endif ()
    if (SYSTEM_FMT_LIBRARY)
        target_link_libraries(spdlog_bench
            PRIVATE
                ${SYSTEM_FMT_LIBRARY})
    elseif (TARGET fmt::fmt)
        target_link_libraries(spdlog_bench
            PRIVATE
                fmt::fmt)
    elseif (FMT_LIBRARY)
        target_link_libraries(spdlog_bench
            PRIVATE
                ${FMT_LIBRARY})
    endif ()
endif ()
