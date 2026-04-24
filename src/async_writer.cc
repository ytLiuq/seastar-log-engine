#include "log_engine/async_writer.hh"

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <utility>
#include <vector>

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/print.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/log.hh>
#include <seastar/util/defer.hh>

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
    _pending.clear();
    _tail_buffer.clear();
    _sequence = 0;
    _write_offset = 0;
    _logical_size = 0;
    _rotation_index = 0;
    _stopping = false;
    _flush_in_progress = false;
    _started = true;
    _active_file_opened_at = std::chrono::system_clock::now();

    auto shard_id = seastar::this_shard_id();
    std::filesystem::create_directories(_config.log_dir);
    _file_path = seastar::format(
        "{}/{}-{}.log",
        _config.log_dir,
        _config.shard_file_prefix,
        shard_id);

    co_await open_file();
    if (!_config.truncate_on_start) {
        co_await recover_from_checkpoint();
    } else if (_config.checkpoint_enabled) {
        co_await persist_checkpoint();
    }
    _flush_timer.arm_periodic(std::chrono::milliseconds(_config.flush_interval_ms));
}

seastar::future<> AsyncWriter::stop() {
    if (!_started) {
        co_return;
    }
    _stopping = true;
    _flush_timer.cancel();
    co_await seastar::with_gate(_gate, [this] {
        return seastar::repeat([this] {
            if (_pending.empty()) {
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            }
            return flush_once().then([] {
                return seastar::stop_iteration::no;
            });
        });
    });
    co_await _gate.close();
    co_await flush_tail(true);
    co_await close_file();
    _started = false;
}

seastar::future<> AsyncWriter::submit(LogMessage message) {
    if (_stopping) {
        co_return;
    }
    _pending.emplace_back(format_record(std::move(message)));
    if (_pending.size() >= _config.batch_size) {
        co_await flush_background();
    }
}

std::size_t AsyncWriter::pending_entries() const noexcept {
    return _pending.size();
}

std::string AsyncWriter::shard_path() const {
    return _file_path;
}

seastar::future<> AsyncWriter::open_file() {
    if (_file) {
        co_return;
    }

    seastar::open_flags flags = seastar::open_flags::wo | seastar::open_flags::create;
    if (_config.truncate_on_start) {
        flags |= seastar::open_flags::truncate;
    }
    if (_config.use_dsync) {
        flags |= seastar::open_flags::dsync;
    }
    flags = seastar::open_flags::rw | (flags & seastar::open_flags::create) | (flags & seastar::open_flags::truncate) | (flags & seastar::open_flags::dsync);

    _file.emplace(co_await seastar::open_file_dma(_file_path, flags));
    _alignment = std::max<std::size_t>(_file->disk_write_dma_alignment(), 1);
    _active_file_opened_at = std::chrono::system_clock::now();
}

seastar::future<> AsyncWriter::flush_once() {
    if (_pending.empty()) {
        co_return;
    }
    if (_flush_in_progress) {
        co_return;
    }

    _flush_in_progress = true;
    auto guard = seastar::defer([this] { _flush_in_progress = false; });
    co_await open_file();

    std::deque<seastar::sstring> batch;
    batch.swap(_pending);

    std::size_t bytes = 0;
    for (const auto& entry : batch) {
        bytes += entry.size();
    }

    std::string joined;
    joined.reserve(bytes);
    for (auto& entry : batch) {
        joined.append(entry.data(), entry.size());
    }
    _logical_size += joined.size();
    _tail_buffer.append(joined);

    try {
        co_await flush_tail(false);
        co_await maybe_rotate();
    } catch (...) {
        _logical_size -= joined.size();
        _tail_buffer.resize(_tail_buffer.size() - joined.size());
        while (!batch.empty()) {
            _pending.emplace_front(std::move(batch.back()));
            batch.pop_back();
        }
        throw;
    }
}

seastar::future<> AsyncWriter::flush_background() {
    if (_stopping) {
        co_return;
    }
    if (_flush_in_progress || _pending.empty()) {
        co_return;
    }
    co_await seastar::with_gate(_gate, [this] {
        return flush_once();
    });
}

seastar::future<> AsyncWriter::close_file() {
    if (!_file) {
        co_return;
    }
    auto file = std::move(*_file);
    _file.reset();
    co_await file.close();
}

seastar::future<> AsyncWriter::flush_tail(bool closing) {
    if (!_file) {
        co_return;
    }

    std::size_t writable_bytes = 0;
    if (closing) {
        writable_bytes = align_up(_tail_buffer.size(), _alignment);
    } else {
        writable_bytes = (_tail_buffer.size() / _alignment) * _alignment;
    }

    if (writable_bytes == 0) {
        if (closing) {
            co_await _file->truncate(_logical_size);
            co_await _file->flush();
        }
        co_return;
    }

    auto buffer = seastar::temporary_buffer<char>::aligned(_alignment, writable_bytes);
    std::memset(buffer.get_write(), 0, writable_bytes);
    const auto copy_bytes = std::min(writable_bytes, _tail_buffer.size());
    std::memcpy(buffer.get_write(), _tail_buffer.data(), copy_bytes);

    const auto expected = writable_bytes;
    co_await write_aligned_buffer(buffer, expected);
    _write_offset += expected;

    if (expected >= _tail_buffer.size()) {
        _tail_buffer.clear();
    } else {
        _tail_buffer.erase(0, expected);
    }

    if (closing) {
        co_await _file->truncate(_logical_size);
    }
    if (_config.checkpoint_enabled) {
        co_await persist_checkpoint();
    }
}

seastar::future<> AsyncWriter::maybe_rotate() {
    const auto size_ready = _config.rotate_size_bytes > 0 && _logical_size >= _config.rotate_size_bytes;
    const auto time_ready = _config.rotate_interval_seconds > 0 &&
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - _active_file_opened_at).count() >=
            static_cast<long long>(_config.rotate_interval_seconds);
    if (!size_ready && !time_ready) {
        co_return;
    }

    co_await flush_tail(true);
    co_await close_file();
    ++_rotation_index;
    co_await _log_manager.rotate_active_file(
        _config,
        _file_path,
        seastar::this_shard_id(),
        _rotation_index);
    _write_offset = 0;
    _logical_size = 0;
    _tail_buffer.clear();
    co_await open_file();
    if (_config.checkpoint_enabled) {
        co_await persist_checkpoint();
    }
}

seastar::sstring AsyncWriter::format_record(LogMessage&& message) {
    return encode_record(
        seastar::this_shard_id(),
        _sequence++,
        message.level,
        format_timestamp(),
        std::move(message.payload));
}

std::string AsyncWriter::format_timestamp() {
    using clock = std::chrono::system_clock;
    const auto now = clock::now();
    const auto time = clock::to_time_t(now);
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
                            now.time_since_epoch())
                            .count() %
        1000000;

    std::tm tm{};
    localtime_r(&time, &tm);

    char buffer[64];
    std::snprintf(
        buffer,
        sizeof(buffer),
        "%04d-%02d-%02d %02d:%02d:%02d.%06lld",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
        static_cast<long long>(micros));
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
    const auto recovery = co_await _log_manager.recover_active_file(_file_path, _alignment);
    _logical_size = recovery.logical_size;
    _sequence = recovery.sequence;
    _rotation_index = recovery.rotation_index;
    _tail_buffer = recovery.tail_buffer;
    _write_offset = _logical_size - _tail_buffer.size();
    co_await _file->truncate(_logical_size);
    if (_config.checkpoint_enabled) {
        co_await persist_checkpoint();
    }
}

seastar::future<> AsyncWriter::persist_checkpoint() {
    co_await _log_manager.store_checkpoint(
        _file_path,
        CheckpointState{
            .logical_size = _logical_size,
            .sequence = _sequence,
            .rotation_index = _rotation_index,
        });
}

seastar::future<> AsyncWriter::write_aligned_buffer(const seastar::temporary_buffer<char>& buffer, std::size_t expected) {
    std::exception_ptr last_error;
    const auto base_offset = _write_offset;
    const auto chunk_limit = std::max(align_up(_config.stream_buffer_size, _alignment), _alignment);
    std::vector<std::size_t> chunk_offsets;
    chunk_offsets.reserve((expected + chunk_limit - 1) / chunk_limit);
    for (std::size_t offset = 0; offset < expected; offset += chunk_limit) {
        chunk_offsets.push_back(offset);
    }

    for (std::size_t attempt = 0; attempt < _config.write_retry_count; ++attempt) {
        bool success = false;
        try {
            co_await seastar::max_concurrent_for_each(chunk_offsets, _config.write_behind, [this, &buffer, expected, chunk_limit, base_offset](std::size_t chunk_offset) {
                const auto chunk_size = std::min(chunk_limit, expected - chunk_offset);
                return _file->dma_write(base_offset + chunk_offset, buffer.get() + chunk_offset, chunk_size).then([chunk_size](std::size_t written) {
                    if (written != chunk_size) {
                        throw std::runtime_error("short dma_write while flushing log batch");
                    }
                });
            });
            co_await _file->flush();
            success = true;
        } catch (...) {
            last_error = std::current_exception();
        }
        if (success) {
            co_return;
        }
        if (attempt + 1 == _config.write_retry_count) {
            break;
        }
        co_await seastar::sleep(std::chrono::milliseconds(_config.write_retry_backoff_ms));
    }
    std::rethrow_exception(last_error);
}

}  // namespace log_engine
