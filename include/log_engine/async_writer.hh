#pragma once

#include <array>
#include <deque>
#include <string>
#include <chrono>

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>

#include "log_engine/append_writer.hh"
#include "log_engine/config.hh"
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
    seastar::future<> flush_fast_once(bool sync_after_write, bool flush_partial_tail);
    seastar::future<> flush_full_once(bool sync_after_write, bool flush_partial_tail);
    seastar::future<> flush_background(bool sync_after_write, bool flush_partial_tail);
    void schedule_background_flush();
    seastar::future<> maybe_rotate();
    seastar::future<> recover_from_checkpoint();
    seastar::future<> persist_checkpoint();
    seastar::temporary_buffer<char> format_record(LogMessage&& message);
    void flush_open_fast_block();
    void append_fast_payload_to_pending(std::string_view payload);
    void setup_metrics();
    void reset_metrics();
    [[nodiscard]] std::size_t fast_block_size() const noexcept;
    [[nodiscard]] std::size_t fast_flush_bytes_limit() const noexcept;
    void submit_fast(LogMessage&& message);
    void submit_full(LogMessage&& message);
    [[nodiscard]] bool use_fast_path() const noexcept;
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
    std::deque<seastar::temporary_buffer<char>> _fast_pending;
    seastar::temporary_buffer<char> _fast_open_block;
    std::size_t _fast_open_block_used = 0;
    std::size_t _fast_pending_entries = 0;
    std::size_t _fast_pending_bytes = 0;
    std::deque<seastar::temporary_buffer<char>> _full_pending;
    std::string _file_path;
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
    bool _started = false;
    bool _stopping = false;
    bool _flush_in_progress = false;
    bool _fast_flush_scheduled = false;
};

}  // namespace log_engine
