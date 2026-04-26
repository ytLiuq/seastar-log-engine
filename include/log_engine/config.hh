#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace log_engine {

struct EngineConfig {
    std::string log_dir = "logs";
    std::string archive_dir = "archive";
    std::string shard_file_prefix = "shard";
    std::size_t batch_size = 1024;
    std::size_t flush_interval_ms = 1;
    std::size_t stream_buffer_size = 64 * 1024;
    std::size_t write_behind = 4;
    std::size_t write_retry_count = 3;
    std::size_t write_retry_backoff_ms = 2;
    std::uint64_t rotate_size_bytes = 64 * 1024 * 1024;
    std::uint64_t rotate_interval_seconds = 0;
    std::uint64_t archive_retention_seconds = 0;
    std::size_t max_archived_files_per_shard = 8;
    bool truncate_on_start = true;
    bool checkpoint_enabled = true;
    bool compress_archives = true;
    bool use_dsync = false;
    bool record_crc_enabled = true;
    bool record_timestamp_enabled = true;
    bool record_level_enabled = true;
    bool record_shard_id_enabled = true;

    void validate() const {
        if (log_dir.empty()) {
            throw std::invalid_argument("log_dir must not be empty");
        }
        if (archive_dir.empty()) {
            throw std::invalid_argument("archive_dir must not be empty");
        }
        if (shard_file_prefix.empty()) {
            throw std::invalid_argument("shard_file_prefix must not be empty");
        }
        if (batch_size == 0) {
            throw std::invalid_argument("batch_size must be greater than zero");
        }
        if (flush_interval_ms == 0) {
            throw std::invalid_argument("flush_interval_ms must be greater than zero");
        }
        if (stream_buffer_size == 0) {
            throw std::invalid_argument("stream_buffer_size must be greater than zero");
        }
        if (write_behind == 0) {
            throw std::invalid_argument("write_behind must be greater than zero");
        }
        if (write_retry_count == 0) {
            throw std::invalid_argument("write_retry_count must be greater than zero");
        }
        if (max_archived_files_per_shard == 0) {
            throw std::invalid_argument("max_archived_files_per_shard must be greater than zero");
        }
    }
};

enum class LogLevel {
    info,
    warn,
    error,
};

struct LogMessage {
    LogLevel level = LogLevel::info;
    std::string payload;
    std::string route_key;
};

}  // namespace log_engine
