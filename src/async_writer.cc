#include "log_engine/async_writer.hh"

#include <chrono>
#include <cstring>
#include <cstdio>
#include <filesystem>
#include <utility>

#include <seastar/core/future-util.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include "log_engine/record_codec.hh"

namespace log_engine {

namespace {

seastar::logger applog("log-engine");

}

AsyncWriter::AsyncWriter()
    : _flush_timer([this] {
          (void)flush_background().handle_exception([](std::exception_ptr ep) {
              applog.warn("background flush failed: {}", ep);
          });
      }) {
}

seastar::future<> AsyncWriter::start(EngineConfig config) {
    _config = std::move(config);
    _config.validate();
    co_await _log_manager.prepare(_config);
    _fast_pending.clear();
    _fast_open_block = seastar::temporary_buffer<char>();
    _fast_open_block_used = 0;
    _fast_pending_entries = 0;
    _fast_pending_bytes = 0;
    _full_pending.clear();
    _sequence = 0;
    _rotation_index = 0;
    _stopping = false;
    _flush_in_progress = false;
    _fast_flush_scheduled = false;
    _started = true;

    auto shard_id = seastar::this_shard_id();
    std::filesystem::create_directories(_config.log_dir);
    _file_path = seastar::format(
        "{}/{}-{}.log",
        _config.log_dir,
        _config.shard_file_prefix,
        shard_id);

    co_await _append_writer.start(_config, _file_path, use_fast_path());
    if (_config.is_full_path() && !_config.truncate_on_start) {
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
    co_await seastar::with_gate(_gate, [this] {
        return seastar::repeat([this] {
            if (pending_entries() == 0) {
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            }
            return flush_once().then([] {
                return seastar::stop_iteration::no;
            });
        });
    });
    co_await _gate.close();
    if (_config.is_full_path()) {
        co_await _append_writer.flush_tail(true);
        if (_config.checkpoint_enabled) {
            co_await persist_checkpoint();
        }
    }
    co_await _append_writer.close();
    _started = false;
}

seastar::future<> AsyncWriter::submit(LogMessage message) {
    if (_stopping) {
        co_return;
    }
    if (use_fast_path()) {
        submit_fast(std::move(message));
    } else {
        submit_full(std::move(message));
    }
    if (pending_entries() >= _config.batch_size ||
        (use_fast_path() && _fast_pending_bytes >= fast_flush_bytes_limit())) {
        if (use_fast_path()) {
            schedule_fast_flush();
        } else {
            co_await flush_background();
        }
    }
}

std::size_t AsyncWriter::pending_entries() const noexcept {
    return use_fast_path() ? _fast_pending_entries : _full_pending.size();
}

std::string AsyncWriter::shard_path() const {
    return _file_path;
}

seastar::future<> AsyncWriter::flush_once() {
    if (_flush_in_progress || pending_entries() == 0) {
        co_return;
    }

    _flush_in_progress = true;
    auto guard = seastar::defer([this] { _flush_in_progress = false; });
    if (use_fast_path()) {
        co_await flush_fast_once();
    } else {
        co_await flush_full_once();
    }
}

seastar::future<> AsyncWriter::flush_fast_once() {
    flush_open_fast_block();
    std::deque<seastar::temporary_buffer<char>> batch;
    batch.swap(_fast_pending);
    const auto batch_entries = _fast_pending_entries;
    _fast_pending_entries = 0;
    _fast_pending_bytes = 0;

    try {
        co_await _append_writer.append_batch(batch);
    } catch (...) {
        _fast_pending_entries += batch_entries;
        while (!batch.empty()) {
            _fast_pending_bytes += batch.back().size();
            _fast_pending.emplace_front(std::move(batch.back()));
            batch.pop_back();
        }
        throw;
    }
}

seastar::future<> AsyncWriter::flush_full_once() {
    std::deque<seastar::temporary_buffer<char>> batch;
    batch.swap(_full_pending);

    try {
        co_await _append_writer.append_batch(batch);
        co_await maybe_rotate();
    } catch (...) {
        while (!batch.empty()) {
            _full_pending.emplace_front(std::move(batch.back()));
            batch.pop_back();
        }
        throw;
    }
}

seastar::future<> AsyncWriter::flush_background() {
    if (_stopping || _flush_in_progress || pending_entries() == 0) {
        co_return;
    }
    co_await seastar::with_gate(_gate, [this] {
        return flush_once();
    });
}

void AsyncWriter::schedule_fast_flush() {
    if (_stopping || _fast_flush_scheduled || pending_entries() == 0) {
        return;
    }

    _fast_flush_scheduled = true;
    (void)seastar::with_gate(_gate, [this] {
        return flush_once();
    }).finally([this] {
        _fast_flush_scheduled = false;
        if (!_stopping && pending_entries() > 0) {
            schedule_fast_flush();
        }
    }).handle_exception([](std::exception_ptr ep) {
        applog.warn("scheduled fast flush failed: {}", ep);
    });
}

seastar::future<> AsyncWriter::maybe_rotate() {
    if (_config.is_fast_path()) {
        co_return;
    }
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
        _file_path,
        seastar::this_shard_id(),
        _rotation_index);
    _append_writer.reset_after_rotation();
    co_await _append_writer.start(_config, _file_path, false);
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

void AsyncWriter::submit_fast(LogMessage&& message) {
    append_fast_payload_to_pending(message.payload);
    ++_fast_pending_entries;
}

void AsyncWriter::submit_full(LogMessage&& message) {
    _full_pending.emplace_back(format_record(std::move(message)));
}

void AsyncWriter::flush_open_fast_block() {
    if (_fast_open_block_used == 0) {
        return;
    }

    _fast_open_block.trim(_fast_open_block_used);
    _fast_pending.emplace_back(std::move(_fast_open_block));
    _fast_open_block = seastar::temporary_buffer<char>();
    _fast_open_block_used = 0;
}

void AsyncWriter::append_fast_payload_to_pending(std::string_view payload) {
    const auto record_size = payload.size() + 1;
    const auto block_size = fast_block_size();

    if (record_size >= block_size) {
        flush_open_fast_block();
        auto buffer = seastar::temporary_buffer<char>(record_size);
        std::memcpy(buffer.get_write(), payload.data(), payload.size());
        buffer.get_write()[payload.size()] = '\n';
        _fast_pending_bytes += record_size;
        _fast_pending.emplace_back(std::move(buffer));
        return;
    }

    if (!_fast_open_block || _fast_open_block.size() - _fast_open_block_used < record_size) {
        flush_open_fast_block();
        _fast_open_block = seastar::temporary_buffer<char>(block_size);
    }

    char* out = _fast_open_block.get_write() + _fast_open_block_used;
    std::memcpy(out, payload.data(), payload.size());
    out[payload.size()] = '\n';
    _fast_open_block_used += record_size;
    _fast_pending_bytes += record_size;
}

std::size_t AsyncWriter::fast_flush_bytes_limit() const noexcept {
    return std::max<std::size_t>(_config.fast_path_max_pending_bytes, fast_block_size() * 4);
}

std::size_t AsyncWriter::fast_block_size() const noexcept {
    return std::max<std::size_t>(_config.stream_buffer_size, 1024 * 1024);
}

AsyncWriter::TimestampBuffer AsyncWriter::format_timestamp() {
    using clock = std::chrono::system_clock;
    const auto now = clock::now();
    const auto time = clock::to_time_t(now);
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
                            now.time_since_epoch())
                            .count() %
        1000000;

    std::tm tm{};
    localtime_r(&time, &tm);

    TimestampBuffer buffer;
    const auto written = std::snprintf(
        buffer.data.data(),
        buffer.data.size(),
        "%04d-%02d-%02d %02d:%02d:%02d.%06lld",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
        static_cast<long long>(micros));
    buffer.size = written > 0 ? static_cast<std::size_t>(written) : 0;
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
    const auto recovery = co_await _log_manager.recover_active_file(_file_path, _append_writer.alignment());
    _sequence = recovery.sequence;
    _rotation_index = recovery.rotation_index;
    co_await _append_writer.truncate_to(recovery.logical_size, recovery.tail_buffer);
    if (_config.checkpoint_enabled) {
        co_await persist_checkpoint();
    }
}

bool AsyncWriter::use_fast_path() const noexcept {
    return _config.is_fast_path();
}

seastar::future<> AsyncWriter::persist_checkpoint() {
    co_await _log_manager.store_checkpoint(
        _file_path,
        CheckpointState{
            .logical_size = _append_writer.logical_size(),
            .sequence = _sequence,
            .rotation_index = _rotation_index,
        });
}

}  // namespace log_engine
