#include "log_engine/async_writer.hh"

#include <chrono>
#include <cstring>
#include <cstdio>
#include <filesystem>
#include <utility>
#include <vector>

#include <seastar/core/future-util.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include "log_engine/log_layout.hh"
#include "log_engine/record_codec.hh"

namespace log_engine {

namespace {

seastar::logger applog("log-engine");

std::size_t batch_bytes(const std::deque<seastar::temporary_buffer<char>>& batch) {
    std::size_t bytes = 0;
    for (const auto& entry : batch) {
        bytes += entry.size();
    }
    return bytes;
}

void write_fixed_width_decimal(char* out, std::uint32_t value, std::size_t width) noexcept {
    for (std::size_t i = 0; i < width; ++i) {
        out[width - 1 - i] = static_cast<char>('0' + (value % 10));
        value /= 10;
    }
}

}

AsyncWriter::AsyncWriter()
    : _flush_timer([this] {
          (void)flush_background(false, false).handle_exception([](std::exception_ptr ep) {
              applog.warn("background flush failed: {}", ep);
          });
      }) {
}

seastar::future<> AsyncWriter::start(EngineConfig config) {
    _config = std::move(config);
    _config.validate();
    co_await _log_manager.prepare(_config);
    _pending.clear();
    _pending_bytes = 0;
    _sequence = 0;
    _rotation_index = 0;
    reset_metrics();
    _stopping = false;
    _flush_in_progress = false;
    _started = true;
    setup_metrics();

    auto shard_id = seastar::this_shard_id();
    std::filesystem::create_directories(_config.log_dir);
    _active_segment = layout::active_segment(_config, shard_id);

    co_await _append_writer.start(_config, _active_segment.path);
    if (!_config.truncate_on_start) {
        co_await recover_from_checkpoint();
    } else if (_config.checkpoint_enabled) {
        co_await persist_checkpoint();
    }
    if (_config.flush_interval_ms > 0) {
        _flush_timer.arm_periodic(std::chrono::milliseconds(_config.flush_interval_ms));
    }
}

seastar::future<> AsyncWriter::stop() {
    if (!_started) {
        co_return;
    }
    _stopping = true;
    _flush_timer.cancel();
    notify_backpressure_waiters();
    co_await seastar::with_gate(_gate, [this] {
        return seastar::repeat([this] {
            if (pending_entries() == 0) {
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            }
            return flush_once(true, true).then([] {
                return seastar::stop_iteration::no;
            });
        });
    });
    co_await _gate.close();
    co_await _append_writer.flush_tail(true);
    if (_config.checkpoint_enabled) {
        co_await persist_checkpoint();
    }
    co_await _append_writer.close();
    _metrics.clear();
    _started = false;
}

seastar::future<> AsyncWriter::submit(LogMessage message) {
    if (_stopping) {
        co_return;
    }
    submit_record(std::move(message));
    if (pending_entries() >= _config.batch_size || above_backpressure_limit()) {
        switch (_config.ack_mode) {
        case AckMode::write_ack:
            co_await flush_background(false, true);
            break;
        case AckMode::sync_ack:
            co_await flush_background(true, true);
            break;
        }
    }
    co_await maybe_wait_for_backpressure();
}

seastar::future<> AsyncWriter::submit_many(std::vector<LogMessage> messages) {
    if (_stopping || messages.empty()) {
        co_return;
    }
    for (auto& message : messages) {
        submit_record(std::move(message));
    }
    if (pending_entries() >= _config.batch_size || above_backpressure_limit()) {
        switch (_config.ack_mode) {
        case AckMode::write_ack:
            co_await flush_background(false, true);
            break;
        case AckMode::sync_ack:
            co_await flush_background(true, true);
            break;
        }
    }
    co_await maybe_wait_for_backpressure();
}

std::size_t AsyncWriter::pending_entries() const noexcept {
    return _pending.size();
}

std::string AsyncWriter::shard_path() const {
    return _active_segment.path;
}

seastar::future<> AsyncWriter::flush_once(bool sync_after_write, bool flush_partial_tail) {
    if (_flush_in_progress || pending_entries() == 0) {
        co_return;
    }

    _flush_in_progress = true;
    auto guard = seastar::defer([this] { _flush_in_progress = false; });

    std::deque<seastar::temporary_buffer<char>> batch;
    batch.swap(_pending);
    const auto batch_byte_count = batch_bytes(batch);
    _pending_bytes = 0;

    try {
        co_await _append_writer.append_batch(batch, sync_after_write);
        if (flush_partial_tail) {
            co_await _append_writer.force_flush(sync_after_write);
        }
        ++_metric_flushed_batches;
        _metric_flushed_bytes += batch_byte_count;
        co_await maybe_rotate();
        notify_backpressure_waiters();
    } catch (...) {
        ++_metric_flush_errors;
        while (!batch.empty()) {
            _pending_bytes += batch.back().size();
            _pending.emplace_front(std::move(batch.back()));
            batch.pop_back();
        }
        notify_backpressure_waiters();
        throw;
    }
}

seastar::future<> AsyncWriter::flush_background(bool sync_after_write, bool flush_partial_tail) {
    if (_stopping || _flush_in_progress || pending_entries() == 0) {
        co_return;
    }
    co_await seastar::with_gate(_gate, [this, sync_after_write, flush_partial_tail] {
        return flush_once(sync_after_write, flush_partial_tail);
    });
}

seastar::future<> AsyncWriter::maybe_rotate() {
    const auto size_ready = _config.rotate_size_bytes > 0 && _append_writer.logical_size() >= _config.rotate_size_bytes;
    const auto time_ready = _config.rotate_interval_seconds > 0 &&
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - _append_writer.opened_at()).count() >=
            static_cast<long long>(_config.rotate_interval_seconds);
    if (!size_ready && !time_ready) {
        co_return;
    }

    co_await _append_writer.flush_tail(true);
    co_await _append_writer.close();
    ++_rotation_index;
    co_await _log_manager.rotate_active_file(
        _config,
        _active_segment,
        _rotation_index);
    _append_writer.reset_after_rotation();
    co_await _append_writer.start(_config, _active_segment.path);
    if (_config.checkpoint_enabled) {
        co_await persist_checkpoint();
    }
}

seastar::temporary_buffer<char> AsyncWriter::format_record(LogMessage&& message) {
    const auto sequence = _sequence++;
    const auto timestamp = _config.record_timestamp_enabled ? format_timestamp() : TimestampBuffer{};
    return encode_record_buffer(
        _config,
        seastar::this_shard_id(),
        sequence,
        message.level,
        timestamp.view(),
        message.payload);
}

void AsyncWriter::submit_record(LogMessage&& message) {
    auto record = format_record(std::move(message));
    ++_metric_submitted_messages;
    _metric_submitted_bytes += record.size();
    _pending_bytes += record.size();
    _pending.emplace_back(std::move(record));
}

seastar::future<> AsyncWriter::maybe_wait_for_backpressure() {
    if (!backpressure_enabled() || !above_backpressure_limit() || _stopping) {
        co_return;
    }
    ++_metric_backpressure_waits;
    ++_waiting_submitters;
    auto guard = seastar::defer([this] {
        --_waiting_submitters;
    });
    co_await _backpressure.wait([this] {
        return _stopping || below_backpressure_resume_threshold();
    });
}

void AsyncWriter::notify_backpressure_waiters() {
    if (_waiting_submitters > 0 && (_stopping || below_backpressure_resume_threshold())) {
        _backpressure.broadcast();
    }
}

bool AsyncWriter::backpressure_enabled() const noexcept {
    return _config.max_pending_bytes > 0;
}

bool AsyncWriter::above_backpressure_limit() const noexcept {
    return backpressure_enabled() && _pending_bytes >= _config.max_pending_bytes;
}

bool AsyncWriter::below_backpressure_resume_threshold() const noexcept {
    return !backpressure_enabled() || _pending_bytes <= backpressure_resume_threshold();
}

std::size_t AsyncWriter::backpressure_resume_threshold() const noexcept {
    if (!backpressure_enabled()) {
        return 0;
    }
    if (_config.pending_bytes_low_watermark > 0) {
        return _config.pending_bytes_low_watermark;
    }
    return _config.max_pending_bytes / 2;
}

void AsyncWriter::setup_metrics() {
    namespace sm = seastar::metrics;
    static const sm::label mode_label("mode");
    const auto mode = mode_label("unified");
    _metrics.clear();
    std::vector<sm::metric_definition> definitions;
    definitions.reserve(10);
    definitions.push_back(sm::make_counter("submitted_messages", sm::description("Total submitted log messages"), [this] {
            return _metric_submitted_messages;
        })(mode));
    definitions.push_back(sm::make_total_bytes("submitted_bytes", [this] {
            return _metric_submitted_bytes;
        }, sm::description("Total submitted log bytes"))(mode));
    definitions.push_back(sm::make_counter("flushed_batches", sm::description("Total flushed write batches"), [this] {
            return _metric_flushed_batches;
        })(mode));
    definitions.push_back(sm::make_total_bytes("flushed_bytes", [this] {
            return _metric_flushed_bytes;
        }, sm::description("Total flushed log bytes"))(mode));
    definitions.push_back(sm::make_counter("flush_errors", sm::description("Total failed flush attempts"), [this] {
            return _metric_flush_errors;
        })(mode));
    definitions.push_back(sm::make_counter("backpressure_waits", sm::description("Total waits caused by pending queue backpressure"), [this] {
            return _metric_backpressure_waits;
        })(mode));
    definitions.push_back(sm::make_queue_length("pending_entries", [this] {
            return pending_entries();
        }, sm::description("Current queued log entries"))(mode));
    definitions.push_back(sm::make_current_bytes("pending_bytes", [this] {
            return _pending_bytes;
        }, sm::description("Current queued log bytes"))(mode));
    definitions.push_back(sm::make_queue_length("waiting_submitters", [this] {
            return _waiting_submitters;
        }, sm::description("Current submitters waiting on pending queue backpressure"))(mode));
    definitions.push_back(sm::make_current_bytes("logical_size_bytes", [this] {
            return _append_writer.logical_size();
        }, sm::description("Current logical log size"))(mode));
    _metrics.add_group("log_engine_writer", definitions);
}

void AsyncWriter::reset_metrics() {
    _metric_submitted_messages = 0;
    _metric_submitted_bytes = 0;
    _metric_flushed_batches = 0;
    _metric_flushed_bytes = 0;
    _metric_flush_errors = 0;
    _metric_backpressure_waits = 0;
}

AsyncWriter::TimestampBuffer AsyncWriter::format_timestamp() {
    using clock = std::chrono::system_clock;
    const auto epoch_micros = std::chrono::duration_cast<std::chrono::microseconds>(
        clock::now().time_since_epoch()).count();
    const auto current_second = static_cast<std::time_t>(epoch_micros / 1000000);
    const auto micros = static_cast<std::uint32_t>(epoch_micros % 1000000);

    struct TimestampCache {
        std::time_t second = 0;
        std::array<char, 21> prefix{};
        bool initialized = false;
    };
    thread_local TimestampCache cache;

    if (!cache.initialized || cache.second != current_second) {
        std::tm tm{};
        const auto time = current_second;
        localtime_r(&time, &tm);
        const auto written = std::snprintf(
            cache.prefix.data(),
            cache.prefix.size(),
            "%04d-%02d-%02d %02d:%02d:%02d.",
            tm.tm_year + 1900,
            tm.tm_mon + 1,
            tm.tm_mday,
            tm.tm_hour,
            tm.tm_min,
            tm.tm_sec);
        cache.second = current_second;
        cache.initialized = written > 0;
    }

    TimestampBuffer buffer;
    constexpr std::size_t prefix_size = 20;
    std::memcpy(buffer.data.data(), cache.prefix.data(), prefix_size);
    write_fixed_width_decimal(buffer.data.data() + prefix_size, micros, 6);
    buffer.size = prefix_size + 6;
    return buffer;
}

std::size_t AsyncWriter::align_up(std::size_t value, std::size_t alignment) noexcept {
    if (alignment == 0) {
        return value;
    }
    const auto remainder = value % alignment;
    return remainder == 0 ? value : (value + alignment - remainder);
}

seastar::future<> AsyncWriter::recover_from_checkpoint() {
    const auto recovery = co_await _log_manager.recover_active_file(_active_segment, _append_writer.alignment());
    _sequence = recovery.sequence;
    _rotation_index = recovery.rotation_index;
    co_await _append_writer.truncate_to(recovery.logical_size, recovery.tail_buffer);
    if (_config.checkpoint_enabled) {
        co_await persist_checkpoint();
    }
}

seastar::future<> AsyncWriter::persist_checkpoint() {
    co_await _log_manager.store_checkpoint(
        _active_segment,
        CheckpointState{
            .logical_size = _append_writer.logical_size(),
            .sequence = _sequence,
            .rotation_index = _rotation_index,
        });
}

}  // namespace log_engine
