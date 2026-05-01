#pragma once

#include <array>
#include <deque>
#include <string>
#include <chrono>

#include <seastar/core/gate.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>

#include "log_engine/append_writer.hh"
#include "log_engine/config.hh"
#include "log_engine/log_layout.hh"
#include "log_engine/log_manager.hh"

namespace log_engine {

class AsyncWriter {
public:
    AsyncWriter();

    seastar::future<> start(EngineConfig config);
    seastar::future<> stop();
    seastar::future<> submit(LogMessage message);

    std::size_t pending_entries() const noexcept;
    std::string shard_path() const;

private:
    seastar::future<> flush_once(bool sync_after_write, bool flush_partial_tail);
    seastar::future<> flush_background(bool sync_after_write, bool flush_partial_tail);
    seastar::future<> maybe_rotate();
    seastar::future<> recover_from_checkpoint();
    seastar::future<> persist_checkpoint();
    seastar::temporary_buffer<char> format_record(LogMessage&& message);
    void submit_record(LogMessage&& message);
    seastar::future<> maybe_wait_for_backpressure();
    void notify_backpressure_waiters();
    [[nodiscard]] bool backpressure_enabled() const noexcept;
    [[nodiscard]] bool above_backpressure_limit() const noexcept;
    [[nodiscard]] bool below_backpressure_resume_threshold() const noexcept;
    [[nodiscard]] std::size_t backpressure_resume_threshold() const noexcept;
    void setup_metrics();
    void reset_metrics();
    struct TimestampBuffer {
        std::array<char, 64> data{};
        std::size_t size = 0;

        [[nodiscard]] std::string_view view() const noexcept {
            return std::string_view(data.data(), size);
        }
    };
    static TimestampBuffer format_timestamp();
    static std::size_t align_up(std::size_t value, std::size_t alignment) noexcept;

private:
    EngineConfig _config;
    seastar::timer<seastar::lowres_clock> _flush_timer;
    seastar::gate _gate;
    seastar::condition_variable _backpressure;
    std::deque<seastar::temporary_buffer<char>> _pending;
    std::size_t _pending_bytes = 0;
    layout::SegmentDescriptor _active_segment;
    std::uint64_t _sequence = 0;
    std::uint64_t _rotation_index = 0;
    AppendWriter _append_writer;
    LogManager _log_manager;
    seastar::metrics::metric_groups _metrics;
    std::uint64_t _metric_submitted_messages = 0;
    std::uint64_t _metric_submitted_bytes = 0;
    std::uint64_t _metric_flushed_batches = 0;
    std::uint64_t _metric_flushed_bytes = 0;
    std::uint64_t _metric_flush_errors = 0;
    std::uint64_t _metric_backpressure_waits = 0;
    std::uint64_t _waiting_submitters = 0;
    bool _started = false;
    bool _stopping = false;
    bool _flush_in_progress = false;
};

}  // namespace log_engine
