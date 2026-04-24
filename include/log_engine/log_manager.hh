#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <chrono>

#include <seastar/core/future.hh>

#include "log_engine/config.hh"

namespace log_engine {

struct CheckpointState {
    std::uint64_t logical_size = 0;
    std::uint64_t sequence = 0;
    std::uint64_t rotation_index = 0;
};

struct RecoveryState {
    std::uint64_t logical_size = 0;
    std::uint64_t sequence = 0;
    std::uint64_t rotation_index = 0;
    std::string tail_buffer;
};

class LogManager {
public:
    seastar::future<> prepare(const EngineConfig& config);
    seastar::future<> rotate_active_file(
        const EngineConfig& config,
        const std::string& active_path,
        unsigned shard_id,
        std::uint64_t rotation_index);
    seastar::future<> store_checkpoint(const std::string& active_path, const CheckpointState& checkpoint);
    seastar::future<std::optional<CheckpointState>> load_checkpoint(const std::string& active_path);
    seastar::future<RecoveryState> recover_active_file(const std::string& active_path, std::size_t alignment);

private:
    static std::string make_archive_path(
        const EngineConfig& config,
        const std::string& active_path,
        unsigned shard_id,
        std::uint64_t rotation_index);
    static std::string checkpoint_path(const std::string& active_path);
    static void cleanup_archives(const EngineConfig& config, unsigned shard_id);
    static void gzip_file(const std::string& path);
};

}  // namespace log_engine
