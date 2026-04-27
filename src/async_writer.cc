#include "log_engine/async_writer.hh"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <cstring>
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

template <typename Visitor>
void for_each_chunk_prefix(
    const std::deque<seastar::temporary_buffer<char>>& first,
    const std::deque<seastar::temporary_buffer<char>>& second,
    std::size_t bytes,
    Visitor&& visitor) {
    auto visit = [&visitor, &bytes](const auto& chunks) {
        for (const auto& chunk : chunks) {
            if (bytes == 0) {
                break;
            }
            const auto span = std::min(bytes, chunk.size());
            visitor(chunk.get(), span);
            bytes -= span;
        }
    };

    visit(first);
    visit(second);
}

std::deque<seastar::temporary_buffer<char>> collect_remaining_chunks(
    std::deque<seastar::temporary_buffer<char>> first,
    std::deque<seastar::temporary_buffer<char>> second,
    std::size_t consumed) {
    std::deque<seastar::temporary_buffer<char>> remaining;

    auto consume = [&remaining, &consumed](auto& chunks) {
        while (!chunks.empty()) {
            auto chunk = std::move(chunks.front());
            chunks.pop_front();
            if (consumed >= chunk.size()) {
                consumed -= chunk.size();
                continue;
            }
            if (consumed > 0) {
                chunk.trim_front(consumed);
                consumed = 0;
            }
            remaining.emplace_back(std::move(chunk));
        }
    };

    consume(first);
    consume(second);
    return remaining;
}

const std::deque<seastar::temporary_buffer<char>> kEmptyChunks;

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
    _tail_chunks.clear();
    _tail_bytes = 0;
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
    if (!use_buffered_io() && !_config.truncate_on_start) {
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
    if (use_plain_payload_mode()) {
        _pending.emplace_back(std::move(message.payload));
    } else {
        _pending.emplace_back(format_record(std::move(message)));
    }
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
    if (_file || _stream) {
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

    auto file = co_await seastar::open_file_dma(_file_path, flags);
    if (use_buffered_io()) {
        seastar::file_output_stream_options options;
        options.buffer_size = static_cast<unsigned>(_config.stream_buffer_size);
        options.write_behind = static_cast<unsigned>(_config.write_behind);
        _stream.emplace(co_await seastar::make_file_output_stream(file, options));
    } else {
        _alignment = std::max<std::size_t>(file.disk_write_dma_alignment(), 1);
        _file.emplace(std::move(file));
    }
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

    std::deque<PendingEntry> batch;
    batch.swap(_pending);

    std::size_t bytes = 0;
    for (const auto& entry : batch) {
        bytes += std::visit([](const auto& value) -> std::size_t {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, seastar::temporary_buffer<char>>) {
                return value.size();
            } else {
                return value.size() + 1;
            }
        }, entry);
    }
    _logical_size += bytes;

    try {
        if (!_stream) {
            std::deque<seastar::temporary_buffer<char>> encoded_batch;
            encoded_batch.resize(0);
            for (auto& entry : batch) {
                encoded_batch.emplace_back(std::get<seastar::temporary_buffer<char>>(std::move(entry)));
            }
            const auto total_bytes = _tail_bytes + bytes;
            const auto writable_bytes = (total_bytes / _alignment) * _alignment;
            if (writable_bytes > 0) {
                auto buffer = seastar::temporary_buffer<char>::aligned(_alignment, writable_bytes);
                char* out = buffer.get_write();
                for_each_chunk_prefix(_tail_chunks, encoded_batch, writable_bytes, [&out](const char* data, std::size_t size) {
                    std::memcpy(out, data, size);
                    out += size;
                });
                co_await write_aligned_buffer(buffer, writable_bytes);
                _write_offset += writable_bytes;
                _tail_chunks = collect_remaining_chunks(std::move(_tail_chunks), std::move(encoded_batch), writable_bytes);
                _tail_bytes = total_bytes - writable_bytes;
            } else {
                while (!encoded_batch.empty()) {
                    _tail_chunks.emplace_back(std::move(encoded_batch.front()));
                    encoded_batch.pop_front();
                }
                _tail_bytes = total_bytes;
            }
        } else {
            for (auto& entry : batch) {
                if (auto* encoded = std::get_if<seastar::temporary_buffer<char>>(&entry)) {
                    co_await _stream->write(encoded->get(), encoded->size());
                } else {
                    auto& payload = std::get<std::string>(entry);
                    co_await _stream->write(payload.data(), payload.size());
                    co_await _stream->write("\n", 1);
                }
            }
        }
        co_await maybe_rotate();
    } catch (...) {
        _logical_size -= bytes;
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
    if (_stream) {
        co_await _stream->close();
        _stream.reset();
    }
    if (_file) {
        co_await _file->close();
        _file.reset();
    }
}

seastar::future<> AsyncWriter::flush_tail(bool closing) {
    if (_stream) {
        co_return;
    }
    if (!_file) {
        co_return;
    }

    const auto writable_bytes = closing ? align_up(_tail_bytes, _alignment) : (_tail_bytes / _alignment) * _alignment;
    if (writable_bytes == 0) {
        if (closing) {
            co_await _file->truncate(_logical_size);
            co_await _file->flush();
        }
        co_return;
    }

    auto buffer = seastar::temporary_buffer<char>::aligned(_alignment, writable_bytes);
    std::memset(buffer.get_write(), 0, writable_bytes);
    char* out = buffer.get_write();
    for_each_chunk_prefix(_tail_chunks, kEmptyChunks, _tail_bytes, [&out](const char* data, std::size_t size) {
        std::memcpy(out, data, size);
        out += size;
    });

    const auto expected = writable_bytes;
    co_await write_aligned_buffer(buffer, expected);
    _write_offset += expected;
    _tail_chunks.clear();
    _tail_bytes = 0;

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

    if (!_stream) {
        co_await flush_tail(true);
    }
    co_await close_file();
    ++_rotation_index;
    co_await _log_manager.rotate_active_file(
        _config,
        _file_path,
        seastar::this_shard_id(),
        _rotation_index);
    _write_offset = 0;
    _logical_size = 0;
    _tail_chunks.clear();
    _tail_bytes = 0;
    co_await open_file();
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
    const auto recovery = co_await _log_manager.recover_active_file(_file_path, _alignment);
    _logical_size = recovery.logical_size;
    _sequence = recovery.sequence;
    _rotation_index = recovery.rotation_index;
    _tail_chunks.clear();
    _tail_bytes = recovery.tail_buffer.size();
    if (_tail_bytes > 0) {
        _tail_chunks.emplace_back(seastar::temporary_buffer<char>::copy_of(recovery.tail_buffer));
    }
    _write_offset = _logical_size - _tail_bytes;
    co_await _file->truncate(_logical_size);
    if (_config.checkpoint_enabled) {
        co_await persist_checkpoint();
    }
}

bool AsyncWriter::use_buffered_io() const noexcept {
    return _config.truncate_on_start && !_config.checkpoint_enabled;
}

bool AsyncWriter::use_plain_payload_mode() const noexcept {
    return use_buffered_io() &&
        !_config.record_crc_enabled &&
        !_config.record_timestamp_enabled &&
        !_config.record_shard_id_enabled &&
        !_config.record_sequence_enabled &&
        !_config.record_level_enabled;
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
