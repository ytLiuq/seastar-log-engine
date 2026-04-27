#pragma once

#include <array>
#include <deque>
#include <optional>
#include <string>
#include <chrono>
#include <variant>

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timer.hh>

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
    seastar::future<> open_file();
    seastar::future<> flush_once();
    seastar::future<> flush_background();
    seastar::future<> close_file();
    seastar::future<> flush_tail(bool closing);
    seastar::future<> maybe_rotate();
    seastar::future<> recover_from_checkpoint();
    seastar::future<> persist_checkpoint();
    seastar::future<> write_aligned_buffer(const seastar::temporary_buffer<char>& buffer, std::size_t expected);
    seastar::temporary_buffer<char> format_record(LogMessage&& message);
    [[nodiscard]] bool use_buffered_io() const noexcept;
    [[nodiscard]] bool use_plain_payload_mode() const noexcept;
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
    std::optional<seastar::file> _file;
    std::optional<seastar::output_stream<char>> _stream;
    using PendingEntry = std::variant<seastar::temporary_buffer<char>, std::string>;
    std::deque<PendingEntry> _pending;
    std::string _file_path;
    std::deque<seastar::temporary_buffer<char>> _tail_chunks;
    std::size_t _tail_bytes = 0;
    std::uint64_t _sequence = 0;
    std::uint64_t _write_offset = 0;
    std::uint64_t _logical_size = 0;
    std::uint64_t _rotation_index = 0;
    std::size_t _alignment = 4096;
    std::chrono::system_clock::time_point _active_file_opened_at{};
    LogManager _log_manager;
    bool _started = false;
    bool _stopping = false;
    bool _flush_in_progress = false;
};

}  // namespace log_engine
