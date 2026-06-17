cmake_minimum_required(VERSION 3.20)

project(seastar_log_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(SEASTAR_ROOT
    "${CMAKE_CURRENT_SOURCE_DIR}/../seastar"
    CACHE PATH
    "Path to a Seastar source checkout")

set(SEASTAR_BUILD_DIR
    "${SEASTAR_ROOT}/build/release"
    CACHE PATH
    "Path to a Seastar build directory containing SeastarConfig.cmake")

if (EXISTS "${SEASTAR_ROOT}/cmake")
    list(APPEND CMAKE_MODULE_PATH "${SEASTAR_ROOT}/cmake")
endif ()

find_package(Seastar REQUIRED CONFIG PATHS "${SEASTAR_BUILD_DIR}" NO_DEFAULT_PATH)
find_package(Boost REQUIRED COMPONENTS program_options)
find_package(ZLIB REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(fmt QUIET CONFIG)
find_package(spdlog QUIET CONFIG)
pkg_check_modules(GRPCPP REQUIRED IMPORTED_TARGET grpc++)
pkg_check_modules(PROTOBUF REQUIRED IMPORTED_TARGET protobuf)
find_program(PROTOC_EXECUTABLE protoc HINTS /usr/local/bin /usr/bin REQUIRED)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)
set(FMT_LOCAL_STATIC_LIBRARY "/usr/local/lib/libfmt.a")
find_library(SYSTEM_FMT_LIBRARY fmt PATHS /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu NO_DEFAULT_PATH)
find_library(FMT_LIBRARY fmt)
find_library(GLOG_LIBRARY glog)

set(GENERATED_PROTO_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${GENERATED_PROTO_DIR}")

set(LOG_ENGINE_QUERY_PROTO "${CMAKE_CURRENT_SOURCE_DIR}/proto/log_engine_query.proto")
set(LOG_ENGINE_QUERY_PROTO_SRCS
    "${GENERATED_PROTO_DIR}/log_engine_query.pb.cc"
    "${GENERATED_PROTO_DIR}/log_engine_query.grpc.pb.cc")
set(LOG_ENGINE_QUERY_PROTO_HDRS
    "${GENERATED_PROTO_DIR}/log_engine_query.pb.h"
    "${GENERATED_PROTO_DIR}/log_engine_query.grpc.pb.h")

add_custom_command(
    OUTPUT ${LOG_ENGINE_QUERY_PROTO_SRCS} ${LOG_ENGINE_QUERY_PROTO_HDRS}
    COMMAND ${PROTOC_EXECUTABLE}
        --experimental_allow_proto3_optional
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
        --cpp_out=${GENERATED_PROTO_DIR}
        --grpc_out=${GENERATED_PROTO_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
        ${LOG_ENGINE_QUERY_PROTO}
    DEPENDS "${LOG_ENGINE_QUERY_PROTO}"
    VERBATIM)

add_library(log_engine_core
    src/agent_support.cc
    src/append_writer.cc
    src/async_writer.cc
    src/compat_glog.cc
    src/config_loader.cc
    src/health_monitor.cc
    src/log_layout.cc
    src/log_manager.cc
    src/log_reader.cc
    src/log_engine.cc
    src/routing.cc
    src/record_codec.cc)

target_include_directories(log_engine_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(log_engine_core
    PUBLIC
        Seastar::seastar
        ZLIB::ZLIB)

if (EXISTS "${FMT_LOCAL_STATIC_LIBRARY}")
    target_link_libraries(log_engine_core PUBLIC "${FMT_LOCAL_STATIC_LIBRARY}")
elseif (TARGET fmt::fmt)
    target_link_libraries(log_engine_core PUBLIC fmt::fmt)
elseif (FMT_LIBRARY)
    target_link_libraries(log_engine_core PUBLIC ${FMT_LIBRARY})
elseif (SYSTEM_FMT_LIBRARY)
    target_link_libraries(log_engine_core PUBLIC ${SYSTEM_FMT_LIBRARY})
endif ()

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

add_executable(log_engine_agent
    src/agent.cc)

target_link_libraries(log_engine_agent
    PRIVATE
        Boost::program_options
        log_engine_core)

add_executable(log_engine_query_server
    src/query_server.cc
    ${LOG_ENGINE_QUERY_PROTO_SRCS})

target_include_directories(log_engine_query_server
    PRIVATE
        ${GENERATED_PROTO_DIR})

target_link_libraries(log_engine_query_server
    PRIVATE
        log_engine_core
        PkgConfig::PROTOBUF
        PkgConfig::GRPCPP)

add_executable(log_engine_query_client
    src/query_client.cc
    ${LOG_ENGINE_QUERY_PROTO_SRCS})

target_include_directories(log_engine_query_client
    PRIVATE
        ${GENERATED_PROTO_DIR})

target_link_libraries(log_engine_query_client
    PRIVATE
        Boost::program_options
        PkgConfig::PROTOBUF
        PkgConfig::GRPCPP)

if (EXISTS "${FMT_LOCAL_STATIC_LIBRARY}")
    target_link_libraries(log_engine_query_client
        PRIVATE
            "${FMT_LOCAL_STATIC_LIBRARY}")
elseif (TARGET fmt::fmt)
    target_link_libraries(log_engine_query_client
        PRIVATE
            fmt::fmt)
elseif (FMT_LIBRARY)
    target_link_libraries(log_engine_query_client
        PRIVATE
            ${FMT_LIBRARY})
elseif (SYSTEM_FMT_LIBRARY)
    target_link_libraries(log_engine_query_client
        PRIVATE
            ${SYSTEM_FMT_LIBRARY})
endif ()

add_executable(log_engine_unit_tests
    src/unit_tests.cc
    src/unit_tests_health.cc
    src/unit_tests_query.cc
    src/unit_tests_recovery_streaming.cc
    ${LOG_ENGINE_QUERY_PROTO_SRCS})

target_include_directories(log_engine_unit_tests
    PRIVATE
        ${GENERATED_PROTO_DIR})

target_link_libraries(log_engine_unit_tests
    PRIVATE
        log_engine_core
        PkgConfig::PROTOBUF
        PkgConfig::GRPCPP)

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
    if (EXISTS "${FMT_LOCAL_STATIC_LIBRARY}")
        target_link_libraries(spdlog_bench
            PRIVATE
                "${FMT_LOCAL_STATIC_LIBRARY}")
    elseif (TARGET fmt::fmt)
        target_link_libraries(spdlog_bench
            PRIVATE
                fmt::fmt)
    elseif (FMT_LIBRARY)
        target_link_libraries(spdlog_bench
            PRIVATE
                ${FMT_LIBRARY})
    elseif (SYSTEM_FMT_LIBRARY)
        target_link_libraries(spdlog_bench
            PRIVATE
                ${SYSTEM_FMT_LIBRARY})
    endif ()
endif ()
