#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>

#include <glog/logging.h>

int main(int argc, char** argv) {
    std::string log_dir = "logs-glog";
    std::uint64_t messages = 200000;
    std::size_t payload_size = 256;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--log-dir" && i + 1 < argc) {
            log_dir = argv[++i];
        } else if (arg == "--messages" && i + 1 < argc) {
            messages = std::stoull(argv[++i]);
        } else if (arg == "--payload-size" && i + 1 < argc) {
            payload_size = std::stoull(argv[++i]);
        }
    }

    FLAGS_log_dir = log_dir;
    FLAGS_logbufsecs = 0;
    FLAGS_stop_logging_if_full_disk = true;
    google::InitGoogleLogging(argv[0]);

    std::string payload(payload_size, 'g');
    auto start = std::chrono::steady_clock::now();
    for (std::uint64_t i = 0; i < messages; ++i) {
        LOG(INFO) << "glog-bench-" << i << ' ' << payload;
    }
    google::FlushLogFiles(google::INFO);
    auto end = std::chrono::steady_clock::now();
    google::ShutdownGoogleLogging();

    const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    const auto throughput = elapsed_us == 0 ? 0.0 : (static_cast<double>(messages) * 1000000.0 / static_cast<double>(elapsed_us));
    std::cout << "messages=" << messages
              << " elapsed_us=" << elapsed_us
              << " throughput_msg_per_sec=" << throughput
              << "\n";
    return 0;
}
