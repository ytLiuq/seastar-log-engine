#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <seastar/core/sstring.hh>

#include "log_engine/config.hh"

namespace log_engine {

struct VerifiedLogState {
    std::uint64_t valid_size = 0;
    std::uint64_t next_sequence = 0;
    std::uint64_t valid_records = 0;
    bool clean_end = true;
};

struct ParsedRecord {
    std::uint32_t crc = 0;
    std::string timestamp;
    unsigned shard = 0;
    std::uint64_t sequence = 0;
    LogLevel level = LogLevel::info;
    std::string payload;
    std::string raw_line;
};

seastar::sstring encode_record(
    unsigned shard_id,
    std::uint64_t sequence,
    LogLevel level,
    std::string timestamp,
    std::string payload);

VerifiedLogState scan_log_content(std::string_view content);
bool verify_record_line(std::string_view line);
std::optional<std::uint64_t> extract_sequence(std::string_view line);
std::optional<ParsedRecord> parse_record_line(std::string_view line);
const char* level_to_string(LogLevel level) noexcept;
std::uint32_t crc32(std::string_view data) noexcept;

}  // namespace log_engine
