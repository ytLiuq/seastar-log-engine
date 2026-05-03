#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <chrono>

#include <seastar/core/future.hh>
#include <seastar/core/metrics_registration.hh>

#include "log_engine/config.hh"
#include "log_engine/log_layout.hh"

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

struct LogManagerStats {
    std::uint64_t rotate_operations = 0;
    std::uint64_t checkpoint_write_successes = 0;
    std::uint64_t checkpoint_write_failures = 0;
    std::uint64_t recovery_fallbacks = 0;
    std::uint64_t gzip_archive_successes = 0;
    std::uint64_t gzip_archive_failures = 0;
    std::uint64_t recovery_fallback_incomplete_checkpoint = 0;
    std::uint64_t recovery_fallback_stale_checkpoint = 0;
};

enum class RecoveryFallbackReason {
    none,
    incomplete_checkpoint,
    stale_checkpoint,
};

const char* recovery_fallback_reason_to_string(RecoveryFallbackReason reason) noexcept;

class LogManager {
public:
    seastar::future<> prepare(const EngineConfig& config);
    seastar::future<> rotate_active_file(
        const EngineConfig& config,
        const layout::SegmentDescriptor& active_segment,
        std::uint64_t rotation_index);
    seastar::future<> store_checkpoint(const layout::SegmentDescriptor& active_segment, const CheckpointState& checkpoint);
    seastar::future<std::optional<CheckpointState>> load_checkpoint(const layout::SegmentDescriptor& active_segment);
    seastar::future<RecoveryState> recover_active_file(const layout::SegmentDescriptor& active_segment, std::size_t alignment);

private:
    static void cleanup_archives(const EngineConfig& config, unsigned shard_id);
    static void gzip_file(const std::string& path);
};

LogManagerStats get_log_manager_stats() noexcept;
RecoveryFallbackReason get_last_recovery_fallback_reason() noexcept;
void reset_log_manager_stats() noexcept;
void register_log_manager_metrics();
void unregister_log_manager_metrics() noexcept;

}  // namespace log_engine
