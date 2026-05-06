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
    record_for_minute(current_wall_clock_minute(), delta);
}

std::uint64_t SlidingWindowCounter::recent_sum() const noexcept {
    return recent_sum_for_minute(current_wall_clock_minute());
}

std::uint64_t SlidingWindowCounter::lifetime_total() const noexcept {
    return _lifetime.load(std::memory_order_relaxed);
}

void SlidingWindowCounter::reset() noexcept {
    for (auto& window : _windows) {
        window.store(0, std::memory_order_relaxed);
    }
    for (auto& minute : _window_minutes) {
        minute.store(0, std::memory_order_relaxed);
    }
    _lifetime.store(0, std::memory_order_relaxed);
    _last_minute.store(0, std::memory_order_relaxed);
    _current_bucket.store(0, std::memory_order_relaxed);
}

bool SlidingWindowCounter::maybe_advance_window() noexcept {
    const auto before = _last_minute.load(std::memory_order_relaxed);
    rotate_current_bucket(current_wall_clock_minute());
    return _last_minute.load(std::memory_order_relaxed) != before;
}

void SlidingWindowCounter::record_for_minute(std::uint64_t minute, std::uint64_t delta) noexcept {
    rotate_current_bucket(minute);
    _windows[_current_bucket.load(std::memory_order_relaxed)].fetch_add(delta, std::memory_order_relaxed);
    _lifetime.fetch_add(delta, std::memory_order_relaxed);
}

std::uint64_t SlidingWindowCounter::recent_sum_for_minute(std::uint64_t minute) const noexcept {
    std::uint64_t sum = 0;
    for (std::size_t i = 0; i < kNumWindows; ++i) {
        const auto bucket_minute = _window_minutes[i].load(std::memory_order_relaxed);
        if (bucket_minute == 0 || bucket_minute > minute) {
            continue;
        }
        if (minute - bucket_minute >= kNumWindows) {
            continue;
        }
        sum += _windows[i].load(std::memory_order_relaxed);
    }
    return sum;
}

void SlidingWindowCounter::rotate_current_bucket(std::uint64_t minute) noexcept {
    while (true) {
        auto last = _last_minute.load(std::memory_order_relaxed);
        if (last == minute) {
            return;
        }
        if (!_last_minute.compare_exchange_weak(last, minute, std::memory_order_relaxed)) {
            continue;
        }

        auto next = _current_bucket.load(std::memory_order_relaxed);
        if (last == 0 || minute <= last || minute - last >= kNumWindows) {
            next = static_cast<std::size_t>(minute % kNumWindows);
        } else {
            next = (next + static_cast<std::size_t>(minute - last)) % kNumWindows;
        }

        _current_bucket.store(next, std::memory_order_relaxed);
        _windows[next].store(0, std::memory_order_relaxed);
        _window_minutes[next].store(minute, std::memory_order_relaxed);
        return;
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

const char* health_reason_to_string(HealthReason reason) noexcept {
    switch (reason) {
    case HealthReason::none:
        return "none";
    case HealthReason::checkpoint_write_failures_recent:
        return "checkpoint_write_failures_recent";
    case HealthReason::gzip_archive_failures_recent:
        return "gzip_archive_failures_recent";
    case HealthReason::recovery_fallbacks_recent:
        return "recovery_fallbacks_recent";
    case HealthReason::reader_gzip_read_errors_recent:
        return "reader_gzip_read_errors_recent";
    case HealthReason::reader_corrupted_segments_recent:
        return "reader_corrupted_segments_recent";
    case HealthReason::reader_corrupted_lines_recent:
        return "reader_corrupted_lines_recent";
    }
    return "none";
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
    if (s.log_manager_checkpoint_failures_recent > 0 ||
        s.log_manager_gzip_failures_recent > 0) {
        return HealthStatus::unhealthy;
    }

    if (s.log_manager_recovery_fallbacks_recent > 3 ||
        s.reader_gzip_read_errors_recent > 3) {
        return HealthStatus::unhealthy;
    }

    constexpr std::uint64_t unhealthy_threshold = 10;
    if (s.reader_corrupted_segments_recent > unhealthy_threshold ||
        s.reader_corrupted_lines_recent > unhealthy_threshold) {
        return HealthStatus::unhealthy;
    }

    if (s.reader_corrupted_segments_recent > 0 ||
        s.reader_corrupted_lines_recent > 0 ||
        s.reader_gzip_read_errors_recent > 0 ||
        s.log_manager_recovery_fallbacks_recent > 0) {
        return HealthStatus::degraded;
    }

    return HealthStatus::ok;
}

HealthReason compute_health_reason(const HealthSnapshot& s) noexcept {
    if (s.log_manager_checkpoint_failures_recent > 0) {
        return HealthReason::checkpoint_write_failures_recent;
    }
    if (s.log_manager_gzip_failures_recent > 0) {
        return HealthReason::gzip_archive_failures_recent;
    }
    if (s.log_manager_recovery_fallbacks_recent > 0) {
        return HealthReason::recovery_fallbacks_recent;
    }
    if (s.reader_gzip_read_errors_recent > 0) {
        return HealthReason::reader_gzip_read_errors_recent;
    }
    if (s.reader_corrupted_segments_recent > 0) {
        return HealthReason::reader_corrupted_segments_recent;
    }
    if (s.reader_corrupted_lines_recent > 0) {
        return HealthReason::reader_corrupted_lines_recent;
    }
    return HealthReason::none;
}

std::string_view health_reason_basis(const HealthSnapshot& snapshot) noexcept {
    return compute_health_reason(snapshot) == HealthReason::none
        ? "none"
        : "recent_window";
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
