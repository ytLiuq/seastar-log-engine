#include "log_engine/append_writer.hh"

#include <algorithm>
#include <cstring>
#include <utility>
#include <vector>

#include <seastar/core/sleep.hh>
#include <seastar/core/reactor.hh>

namespace log_engine {

namespace {

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

}  // namespace

seastar::future<> AppendWriter::start(const EngineConfig& config, std::string file_path, bool buffered) {
    _config = config;
    _file_path = std::move(file_path);
    _buffered = buffered;
    _tail_chunks.clear();
    _tail_bytes = 0;
    _write_offset = 0;
    _logical_size = 0;
    _alignment = 4096;
    _opened_at = std::chrono::system_clock::now();
    co_await open_file();
}

seastar::future<> AppendWriter::open_file() {
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

    if (_buffered) {
        auto file = co_await seastar::open_file_dma(_file_path, flags);
        seastar::file_output_stream_options stream_options;
        stream_options.buffer_size = static_cast<unsigned>(_config.stream_buffer_size);
        stream_options.write_behind = static_cast<unsigned>(_config.write_behind);
        _stream.emplace(co_await seastar::make_file_output_stream(file, stream_options));
    } else {
        auto file = co_await seastar::open_file_dma(_file_path, flags);
        _alignment = std::max<std::size_t>(file.disk_write_dma_alignment(), 1);
        _file.emplace(std::move(file));
    }
    _opened_at = std::chrono::system_clock::now();
}

seastar::future<> AppendWriter::append_batch(std::deque<seastar::temporary_buffer<char>>& batch) {
    std::size_t bytes = 0;
    for (const auto& entry : batch) {
        bytes += entry.size();
    }
    _logical_size += bytes;

    try {
        if (_buffered) {
            for (const auto& payload : batch) {
                co_await _stream->write(payload.get(), payload.size());
            }
        } else {
            const auto total_bytes = _tail_bytes + bytes;
            const auto writable_bytes = (total_bytes / _alignment) * _alignment;
            if (writable_bytes > 0) {
                auto buffer = seastar::temporary_buffer<char>::aligned(_alignment, writable_bytes);
                char* out = buffer.get_write();
                for_each_chunk_prefix(_tail_chunks, batch, writable_bytes, [&out](const char* data, std::size_t size) {
                    std::memcpy(out, data, size);
                    out += size;
                });
                co_await write_aligned_buffer(buffer, writable_bytes);
                _write_offset += writable_bytes;
                _tail_chunks = collect_remaining_chunks(std::move(_tail_chunks), std::move(batch), writable_bytes);
                _tail_bytes = total_bytes - writable_bytes;
            } else {
                while (!batch.empty()) {
                    _tail_chunks.emplace_back(std::move(batch.front()));
                    batch.pop_front();
                }
                _tail_bytes = total_bytes;
            }
        }
    } catch (...) {
        _logical_size -= bytes;
        throw;
    }
}

seastar::future<> AppendWriter::flush_tail(bool closing) {
    if (_buffered || !_file) {
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

    co_await write_aligned_buffer(buffer, writable_bytes);
    _write_offset += writable_bytes;
    _tail_chunks.clear();
    _tail_bytes = 0;

    if (closing) {
        co_await _file->truncate(_logical_size);
    }
}

seastar::future<> AppendWriter::truncate_to(std::uint64_t logical_size, std::string_view tail_buffer) {
    _logical_size = logical_size;
    _tail_chunks.clear();
    _tail_bytes = tail_buffer.size();
    if (_tail_bytes > 0) {
        _tail_chunks.emplace_back(seastar::temporary_buffer<char>::copy_of(tail_buffer));
    }
    _write_offset = _logical_size - _tail_bytes;
    if (_file) {
        co_await _file->truncate(_logical_size);
    }
}

seastar::future<> AppendWriter::close() {
    if (_stream) {
        co_await _stream->flush();
        co_await _stream->close();
        _stream.reset();
    }
    if (_file) {
        co_await _file->close();
        _file.reset();
    }
}

void AppendWriter::reset_after_rotation() {
    _write_offset = 0;
    _logical_size = 0;
    _tail_chunks.clear();
    _tail_bytes = 0;
}

std::uint64_t AppendWriter::logical_size() const noexcept {
    return _logical_size;
}

std::chrono::system_clock::time_point AppendWriter::opened_at() const noexcept {
    return _opened_at;
}

std::size_t AppendWriter::alignment() const noexcept {
    return _alignment;
}

std::size_t AppendWriter::align_up(std::size_t value, std::size_t alignment) noexcept {
    if (alignment == 0) {
        return value;
    }
    const auto remainder = value % alignment;
    return remainder == 0 ? value : (value + alignment - remainder);
}

seastar::future<> AppendWriter::write_aligned_buffer(const seastar::temporary_buffer<char>& buffer, std::size_t expected) {
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
