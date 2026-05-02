#include "log_engine/health_monitor.hh"

#include <chrono>
#include <memory>
#include <vector>

#include <seastar/core/metrics.hh>

namespace log_engine {

namespace {

std::uint64_t current_wall_clock_minute() noexcept {
    const auto now = std::chrono::system_clock::now();
    const auto epoch_seconds = std::chrono::duration_cast<std::chrono::seconds>(
        now.time_since_epoch()).count();
    return static_cast<std::uint64_t>(epoch_seconds) / 60;
}

SlidingWindowCounter g_reader_corrupted_segments;
SlidingWindowCounter g_reader_corrupted_lines;
SlidingWindowCounter g_reader_gzip_read_errors;
SlidingWindowCounter g_checkpoint_write_failures;
SlidingWindowCounter g_gzip_archive_failures;
SlidingWindowCounter g_recovery_fallbacks;

std::unique_ptr<seastar::metrics::metric_groups> g_health_metrics;

}  // namespace

void SlidingWindowCounter::record(std::uint64_t delta) noexcept {
    maybe_advance_window();
    _windows[_current_bucket.load(std::memory_order_relaxed)].fetch_add(delta, std::memory_order_relaxed);
    _lifetime.fetch_add(delta, std::memory_order_relaxed);
}

std::uint64_t SlidingWindowCounter::recent_sum() const noexcept {
    const auto current = _current_bucket.load(std::memory_order_relaxed);
    std::uint64_t sum = 0;
    for (std::size_t i = 0; i < kNumWindows; ++i) {
        sum += _windows[i].load(std::memory_order_relaxed);
    }
    return sum;  // all buckets are within the window since stale ones are cleared on advance
}

std::uint64_t SlidingWindowCounter::lifetime_total() const noexcept {
    return _lifetime.load(std::memory_order_relaxed);
}

void SlidingWindowCounter::reset() noexcept {
    for (auto& window : _windows) {
        window.store(0, std::memory_order_relaxed);
    }
    _lifetime.store(0, std::memory_order_relaxed);
    _last_minute.store(0, std::memory_order_relaxed);
    _current_bucket.store(0, std::memory_order_relaxed);
}

bool SlidingWindowCounter::maybe_advance_window() noexcept {
    const auto minute = current_wall_clock_minute();
    auto last = _last_minute.load(std::memory_order_relaxed);
    if (minute == last) {
        return false;
    }
    if (!_last_minute.compare_exchange_strong(last, minute, std::memory_order_relaxed)) {
        return false;  // another caller advanced
    }
    clear_stale_windows(minute);
    const auto next = (_current_bucket.load(std::memory_order_relaxed) + 1) % kNumWindows;
    _current_bucket.store(next, std::memory_order_relaxed);
    _windows[next].store(0, std::memory_order_relaxed);
    return true;
}

void SlidingWindowCounter::clear_stale_windows(std::uint64_t current_minute) noexcept {
    const auto oldest_valid_minute = current_minute > kNumWindows
        ? current_minute - kNumWindows
        : 0;
    // If we've been idle for more than kNumWindows minutes, clear all windows
    if (current_minute - oldest_valid_minute >= kNumWindows) {
        // Only clear buckets that are more than kNumWindows behind
        for (std::size_t i = 0; i < kNumWindows; ++i) {
            const auto bucket = (i + _current_bucket.load(std::memory_order_relaxed) + 1) % kNumWindows;
            // Clear stale buckets that are too far behind
            if (i >= kNumWindows - 1) {
                _windows[bucket].store(0, std::memory_order_relaxed);
            }
        }
    }
}

const char* health_status_to_string(HealthStatus status) noexcept {
    switch (status) {
    case HealthStatus::ok:
        return "ok";
    case HealthStatus::degraded:
        return "degraded";
    case HealthStatus::unhealthy:
        return "unhealthy";
    }
    return "ok";
}

HealthSnapshot collect_health_snapshot() noexcept {
    HealthSnapshot snapshot;

    snapshot.reader_corrupted_segments_recent = g_reader_corrupted_segments.recent_sum();
    snapshot.reader_corrupted_lines_recent = g_reader_corrupted_lines.recent_sum();
    snapshot.reader_gzip_read_errors_recent = g_reader_gzip_read_errors.recent_sum();

    snapshot.reader_corrupted_segments_lifetime = g_reader_corrupted_segments.lifetime_total();
    snapshot.reader_corrupted_lines_lifetime = g_reader_corrupted_lines.lifetime_total();
    snapshot.reader_gzip_read_errors_lifetime = g_reader_gzip_read_errors.lifetime_total();

    snapshot.log_manager_checkpoint_failures_recent = g_checkpoint_write_failures.recent_sum();
    snapshot.log_manager_gzip_failures_recent = g_gzip_archive_failures.recent_sum();
    snapshot.log_manager_recovery_fallbacks_recent = g_recovery_fallbacks.recent_sum();

    snapshot.log_manager_checkpoint_failures_lifetime = g_checkpoint_write_failures.lifetime_total();
    snapshot.log_manager_gzip_failures_lifetime = g_gzip_archive_failures.lifetime_total();
    snapshot.log_manager_recovery_fallbacks_lifetime = g_recovery_fallbacks.lifetime_total();

    return snapshot;
}

HealthStatus compute_health_status(const HealthSnapshot& s) noexcept {
    // Unhealthy: >10 errors of any category in the recent 5-minute window
    constexpr std::uint64_t unhealthy_threshold = 10;
    if (s.reader_corrupted_segments_recent > unhealthy_threshold ||
        s.reader_corrupted_lines_recent > unhealthy_threshold ||
        s.reader_gzip_read_errors_recent > unhealthy_threshold ||
        s.log_manager_checkpoint_failures_recent > unhealthy_threshold ||
        s.log_manager_gzip_failures_recent > unhealthy_threshold ||
        s.log_manager_recovery_fallbacks_recent > unhealthy_threshold) {
        return HealthStatus::unhealthy;
    }

    // Degraded: any non-zero error count in the recent window
    if (s.reader_corrupted_segments_recent > 0 ||
        s.reader_corrupted_lines_recent > 0 ||
        s.reader_gzip_read_errors_recent > 0 ||
        s.log_manager_checkpoint_failures_recent > 0 ||
        s.log_manager_gzip_failures_recent > 0 ||
        s.log_manager_recovery_fallbacks_recent > 0) {
        return HealthStatus::degraded;
    }

    return HealthStatus::ok;
}

void record_reader_corrupted_segment() noexcept {
    g_reader_corrupted_segments.record();
}

void record_reader_corrupted_line() noexcept {
    g_reader_corrupted_lines.record();
}

void record_reader_gzip_read_error() noexcept {
    g_reader_gzip_read_errors.record();
}

void record_checkpoint_write_failure() noexcept {
    g_checkpoint_write_failures.record();
}

void record_gzip_archive_failure() noexcept {
    g_gzip_archive_failures.record();
}

void record_recovery_fallback() noexcept {
    g_recovery_fallbacks.record();
}

void register_health_metrics() {
    namespace sm = seastar::metrics;
    static const sm::label component_label("component");
    const auto component = component_label("health");

    if (!g_health_metrics) {
        g_health_metrics = std::make_unique<sm::metric_groups>();
    } else {
        g_health_metrics->clear();
    }

    std::vector<sm::metric_definition> definitions;
    definitions.reserve(12);

    auto add_recent_gauge = [&](const char* name, const char* desc, auto&& accessor) {
        definitions.push_back(sm::make_gauge(name, sm::description(desc),
            [accessor = std::forward<decltype(accessor)>(accessor)] {
                return accessor().recent_sum();
            })(component));
    };

    auto add_lifetime_counter = [&](const char* name, const char* desc, auto&& accessor) {
        definitions.push_back(sm::make_counter(name, sm::description(desc),
            [accessor = std::forward<decltype(accessor)>(accessor)] {
                return accessor().lifetime_total();
            })(component));
    };

    add_recent_gauge("reader_corrupted_segments_recent",
        "Corrupted segments in the last 5-minute window", []() -> const SlidingWindowCounter& { return g_reader_corrupted_segments; });
    add_recent_gauge("reader_corrupted_lines_recent",
        "Corrupted lines in the last 5-minute window", []() -> const SlidingWindowCounter& { return g_reader_corrupted_lines; });
    add_recent_gauge("reader_gzip_read_errors_recent",
        "Gzip read errors in the last 5-minute window", []() -> const SlidingWindowCounter& { return g_reader_gzip_read_errors; });
    add_recent_gauge("checkpoint_write_failures_recent",
        "Checkpoint write failures in the last 5-minute window", []() -> const SlidingWindowCounter& { return g_checkpoint_write_failures; });
    add_recent_gauge("gzip_archive_failures_recent",
        "Gzip archive failures in the last 5-minute window", []() -> const SlidingWindowCounter& { return g_gzip_archive_failures; });
    add_recent_gauge("recovery_fallbacks_recent",
        "Recovery fallbacks in the last 5-minute window", []() -> const SlidingWindowCounter& { return g_recovery_fallbacks; });

    add_lifetime_counter("reader_corrupted_segments_lifetime",
        "Lifetime total of corrupted segments", []() -> const SlidingWindowCounter& { return g_reader_corrupted_segments; });
    add_lifetime_counter("reader_corrupted_lines_lifetime",
        "Lifetime total of corrupted lines", []() -> const SlidingWindowCounter& { return g_reader_corrupted_lines; });
    add_lifetime_counter("reader_gzip_read_errors_lifetime",
        "Lifetime total of gzip read errors", []() -> const SlidingWindowCounter& { return g_reader_gzip_read_errors; });
    add_lifetime_counter("checkpoint_write_failures_lifetime",
        "Lifetime total of checkpoint write failures", []() -> const SlidingWindowCounter& { return g_checkpoint_write_failures; });
    add_lifetime_counter("gzip_archive_failures_lifetime",
        "Lifetime total of gzip archive failures", []() -> const SlidingWindowCounter& { return g_gzip_archive_failures; });
    add_lifetime_counter("recovery_fallbacks_lifetime",
        "Lifetime total of recovery fallbacks", []() -> const SlidingWindowCounter& { return g_recovery_fallbacks; });

    g_health_metrics->add_group("log_engine_health", definitions);
}

void unregister_health_metrics() noexcept {
    if (g_health_metrics) {
        g_health_metrics->clear();
    }
}

}  // namespace log_engine
