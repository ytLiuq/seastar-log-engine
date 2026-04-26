#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

// Prefer the distro fmt headers required by Ubuntu's spdlog package.
#if __has_include("/usr/include/fmt/core.h") && __has_include("/usr/include/fmt/format.h")
#include "/usr/include/fmt/core.h"
#include "/usr/include/fmt/format.h"
#endif

#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

int main(int argc, char** argv) {
    std::string log_dir = "logs-spdlog";
    std::uint64_t messages = 200000;
    std::size_t payload_size = 256;
    std::size_t queue_size = 8192;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--log-dir" && i + 1 < argc) {
            log_dir = argv[++i];
        } else if (arg == "--messages" && i + 1 < argc) {
            messages = std::stoull(argv[++i]);
        } else if (arg == "--payload-size" && i + 1 < argc) {
            payload_size = std::stoull(argv[++i]);
        } else if (arg == "--queue-size" && i + 1 < argc) {
            queue_size = std::stoull(argv[++i]);
        }
    }

    const auto log_path = log_dir + "/spdlog_bench.log";
    auto sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_path, true);
    auto thread_pool = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
    auto logger = std::make_shared<spdlog::async_logger>(
        "spdlog_bench",
        sink,
        thread_pool,
        spdlog::async_overflow_policy::block);

    logger->set_pattern("%v");
    logger->flush_on(spdlog::level::info);
    spdlog::register_logger(logger);
    spdlog::set_default_logger(logger);

    std::string payload(payload_size, 's');
    auto start = std::chrono::steady_clock::now();
    for (std::uint64_t i = 0; i < messages; ++i) {
        logger->info("spdlog-bench-{} {}", i, payload);
    }
    logger->flush();
    spdlog::shutdown();
    auto end = std::chrono::steady_clock::now();

    const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    const auto throughput = elapsed_us == 0 ? 0.0 : (static_cast<double>(messages) * 1000000.0 / static_cast<double>(elapsed_us));
    std::cout << "messages=" << messages
              << " elapsed_us=" << elapsed_us
              << " throughput_msg_per_sec=" << throughput
              << "\n";
    return 0;
}
