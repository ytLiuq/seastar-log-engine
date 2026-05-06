#include <algorithm>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <seastar/core/future.hh>

#include "log_engine/log_engine.hh"
#include "log_engine/log_layout.hh"
#include "log_engine/log_manager.hh"
#include "log_engine/record_codec.hh"

namespace {
namespace fs = std::filesystem;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void reset_directory(const fs::path& dir) {
    fs::create_directories(dir);
    for (const auto& entry : fs::directory_iterator(dir)) {
        fs::remove_all(entry.path());
    }
}

std::optional<fs::path> find_non_empty_shard_log(const std::string& log_dir) {
    for (const auto& entry : fs::directory_iterator(log_dir)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto name = entry.path().filename().string();
        if (name.rfind("shard-", 0) != 0 || entry.path().extension() != ".log") {
            continue;
        }
        if (fs::file_size(entry.path()) == 0) {
            continue;
        }
        return entry.path();
    }
    return std::nullopt;
}

std::string read_file(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    require(in.good(), "failed to open test file");
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

}  // namespace

seastar::future<> test_recovery_scan_large_file_streaming(const std::string& root_dir) {
    const auto log_dir = (fs::path(root_dir) / "recovery-large-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "recovery-large-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 32;
    config.checkpoint_enabled = true;
    config.truncate_on_start = false;
    config.record_sequence_enabled = true;

    const std::string payload(1024, 'L');
    log_engine::LogEngine engine;
    co_await engine.start(config);
    for (int i = 0; i < 256; ++i) {
        co_await engine.info(payload + std::to_string(i), "route-large");
    }
    co_await engine.stop();

    const auto shard_path = find_non_empty_shard_log(log_dir);
    require(shard_path.has_value(), "large recovery test should find a non-empty shard log");

    const auto original = read_file(*shard_path);
    const auto verified = log_engine::scan_log_content(original);
    require(verified.valid_records == 256, "large recovery test should create all valid records before corruption");
    require(original.size() > 64 * 1024, "large recovery test should exceed one streaming chunk");

    {
        std::ofstream out(*shard_path, std::ios::app | std::ios::binary);
        out << "BROKEN_LARGE_STREAMING_TAIL_WITHOUT_NEWLINE";
    }

    log_engine::LogManager manager;
    const auto active_segment = log_engine::layout::describe_path(config, shard_path->string());
    require(active_segment.has_value(), "large recovery test should describe active shard log");
    const auto recovery = co_await manager.recover_active_file(*active_segment, 4096);
    require(recovery.logical_size == verified.valid_size, "streaming recovery should preserve valid prefix size");
    require(recovery.sequence == verified.next_sequence, "streaming recovery should preserve next sequence");
    require(recovery.tail_buffer.size() == verified.valid_size % 4096, "streaming recovery should preserve valid alignment tail");
    co_return;
}

