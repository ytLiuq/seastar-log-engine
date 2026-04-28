#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace log_engine {

enum class WriteMode {
    fast,
    full,
};

enum class AckMode {
    memory_ack,
    write_ack,
    sync_ack,
};

inline const char* ack_mode_to_string(AckMode mode) noexcept {
    switch (mode) {
    case AckMode::memory_ack:
        return "memory_ack";
    case AckMode::write_ack:
        return "write_ack";
    case AckMode::sync_ack:
        return "sync_ack";
    }
    return "memory_ack";
}

inline AckMode parse_ack_mode(std::string_view value) {
    if (value == "memory_ack" || value == "memory-ack" || value == "memory") {
        return AckMode::memory_ack;
    }
    if (value == "write_ack" || value == "write-ack" || value == "write") {
        return AckMode::write_ack;
    }
    if (value == "sync_ack" || value == "sync-ack" || value == "sync") {
        return AckMode::sync_ack;
    }
    throw std::invalid_argument("ack_mode must be memory_ack, write_ack, or sync_ack");
}

enum class RoutingStrategy {
    hash_modulo,
    consistent_hashing,
};

inline const char* routing_strategy_to_string(RoutingStrategy strategy) noexcept {
    switch (strategy) {
    case RoutingStrategy::hash_modulo:
        return "hash_modulo";
    case RoutingStrategy::consistent_hashing:
        return "consistent_hashing";
    }
    return "hash_modulo";
}

inline RoutingStrategy parse_routing_strategy(std::string_view value) {
    if (value == "hash_modulo" || value == "hash-modulo" || value == "modulo") {
        return RoutingStrategy::hash_modulo;
    }
    if (value == "consistent_hashing" || value == "consistent-hashing" || value == "consistent") {
        return RoutingStrategy::consistent_hashing;
    }
    throw std::invalid_argument("routing_strategy must be hash_modulo or consistent_hashing");
}

struct EngineConfig {
    std::string log_dir = "logs";
    std::string archive_dir = "archive";
    std::string shard_file_prefix = "shard";
    WriteMode write_mode = WriteMode::fast;
    AckMode ack_mode = AckMode::memory_ack;
    RoutingStrategy routing_strategy = RoutingStrategy::hash_modulo;
    std::size_t routing_virtual_nodes = 128;
    std::size_t batch_size = 32;
    std::size_t flush_interval_ms = 0;
    std::size_t fast_path_max_pending_bytes = 16 * 1024;
    std::size_t stream_buffer_size = 64 * 1024;
    std::size_t write_behind = 8;
    std::size_t write_retry_count = 3;
    std::size_t write_retry_backoff_ms = 2;
    std::uint64_t rotate_size_bytes = 0;
    std::uint64_t rotate_interval_seconds = 0;
    std::uint64_t archive_retention_seconds = 0;
    std::size_t max_archived_files_per_shard = 8;
    bool truncate_on_start = true;
    bool checkpoint_enabled = false;
    bool compress_archives = false;
    bool use_dsync = false;
    bool record_crc_enabled = false;
    bool record_timestamp_enabled = false;
    bool record_level_enabled = false;
    bool record_shard_id_enabled = false;
    bool record_sequence_enabled = false;

    [[nodiscard]] bool rotation_enabled() const noexcept {
        return rotate_size_bytes > 0 || rotate_interval_seconds > 0;
    }

    [[nodiscard]] bool archive_features_enabled() const noexcept {
        return rotation_enabled() || archive_retention_seconds > 0 || compress_archives;
    }

    [[nodiscard]] bool structured_record_enabled() const noexcept {
        return record_crc_enabled ||
            record_timestamp_enabled ||
            record_level_enabled ||
            record_shard_id_enabled ||
            record_sequence_enabled;
    }

    [[nodiscard]] bool is_fast_path() const noexcept {
        return write_mode == WriteMode::fast;
    }

    [[nodiscard]] bool is_full_path() const noexcept {
        return write_mode == WriteMode::full;
    }

    void validate() const {
        if (log_dir.empty()) {
            throw std::invalid_argument("log_dir must not be empty");
        }
        if (archive_features_enabled() && archive_dir.empty()) {
            throw std::invalid_argument("archive_dir must not be empty");
        }
        if (shard_file_prefix.empty()) {
            throw std::invalid_argument("shard_file_prefix must not be empty");
        }
        if (batch_size == 0) {
            throw std::invalid_argument("batch_size must be greater than zero");
        }
        if (routing_virtual_nodes == 0) {
            throw std::invalid_argument("routing_virtual_nodes must be greater than zero");
        }
        if (stream_buffer_size == 0) {
            throw std::invalid_argument("stream_buffer_size must be greater than zero");
        }
        if (fast_path_max_pending_bytes == 0) {
            throw std::invalid_argument("fast_path_max_pending_bytes must be greater than zero");
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
        if (is_fast_path()) {
            if (!truncate_on_start) {
                throw std::invalid_argument("fast path requires truncate_on_start=true");
            }
            if (checkpoint_enabled) {
                throw std::invalid_argument("fast path does not support checkpoints");
            }
            if (archive_features_enabled()) {
                throw std::invalid_argument("fast path does not support rotation or archive management");
            }
            if (structured_record_enabled()) {
                throw std::invalid_argument("fast path only supports payload-only records");
            }
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
