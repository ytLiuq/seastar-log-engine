#pragma once

#include <chrono>
#include <deque>
#include <optional>
#include <string>

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "log_engine/config.hh"

namespace log_engine {

class AppendWriter {
public:
    seastar::future<> start(const EngineConfig& config, std::string file_path, bool buffered);
    seastar::future<> close();

    seastar::future<> append_batch(std::deque<seastar::temporary_buffer<char>>& batch);
    seastar::future<> flush_tail(bool closing);
    seastar::future<> truncate_to(std::uint64_t logical_size, std::string_view tail_buffer);

    void reset_after_rotation();

    [[nodiscard]] std::uint64_t logical_size() const noexcept;
    [[nodiscard]] std::chrono::system_clock::time_point opened_at() const noexcept;
    [[nodiscard]] std::size_t alignment() const noexcept;

private:
    seastar::future<> open_file();
    seastar::future<> write_aligned_buffer(const seastar::temporary_buffer<char>& buffer, std::size_t expected);
    static std::size_t align_up(std::size_t value, std::size_t alignment) noexcept;

private:
    EngineConfig _config;
    std::string _file_path;
    bool _buffered = false;
    std::optional<seastar::file> _file;
    std::optional<seastar::output_stream<char>> _stream;
    std::deque<seastar::temporary_buffer<char>> _tail_chunks;
    std::size_t _tail_bytes = 0;
    std::uint64_t _write_offset = 0;
    std::uint64_t _logical_size = 0;
    std::size_t _alignment = 4096;
    std::chrono::system_clock::time_point _opened_at{};
};

}  // namespace log_engine
