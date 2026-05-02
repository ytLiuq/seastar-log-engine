#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>

#include <seastar/core/metrics_registration.hh>

namespace log_engine {

// A counter tracked over a sliding window of N one-minute buckets
// plus a lifetime total. Thread-safe.
class SlidingWindowCounter {
public:
    static constexpr std::size_t kNumWindows = 5;  // 5-minute window

    void record(std::uint64_t delta = 1) noexcept;
    [[nodiscard]] std::uint64_t recent_sum() const noexcept;
    [[nodiscard]] std::uint64_t lifetime_total() const noexcept;
    void reset() noexcept;
    bool maybe_advance_window() noexcept;

private:
    void clear_stale_windows(std::uint64_t current_minute) noexcept;

    std::array<std::atomic<std::uint64_t>, kNumWindows> _windows{};
    std::atomic<std::uint64_t> _lifetime{0};
    std::atomic<std::uint64_t> _last_minute{0};
    std::atomic<std::size_t> _current_bucket{0};
};

struct HealthSnapshot {
    // Reader recent-window counts
    std::uint64_t reader_corrupted_segments_recent = 0;
    std::uint64_t reader_corrupted_lines_recent = 0;
    std::uint64_t reader_gzip_read_errors_recent = 0;

    // Reader lifetime counts
    std::uint64_t reader_corrupted_segments_lifetime = 0;
    std::uint64_t reader_corrupted_lines_lifetime = 0;
    std::uint64_t reader_gzip_read_errors_lifetime = 0;

    // Log manager recent-window counts
    std::uint64_t log_manager_checkpoint_failures_recent = 0;
    std::uint64_t log_manager_gzip_failures_recent = 0;
    std::uint64_t log_manager_recovery_fallbacks_recent = 0;

    // Log manager lifetime counts
    std::uint64_t log_manager_checkpoint_failures_lifetime = 0;
    std::uint64_t log_manager_gzip_failures_lifetime = 0;
    std::uint64_t log_manager_recovery_fallbacks_lifetime = 0;
};

enum class HealthStatus {
    ok,
    degraded,
    unhealthy,
};

const char* health_status_to_string(HealthStatus status) noexcept;
HealthSnapshot collect_health_snapshot() noexcept;
HealthStatus compute_health_status(const HealthSnapshot& snapshot) noexcept;

void record_reader_corrupted_segment() noexcept;
void record_reader_corrupted_line() noexcept;
void record_reader_gzip_read_error() noexcept;
void record_checkpoint_write_failure() noexcept;
void record_gzip_archive_failure() noexcept;
void record_recovery_fallback() noexcept;

void register_health_metrics();
void unregister_health_metrics() noexcept;

}  // namespace log_engine
