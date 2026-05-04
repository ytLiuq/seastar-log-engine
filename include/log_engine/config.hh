#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace log_engine {

enum class CrcClass {
    none,    // No CRC (maximum throughput)
    header,  // CRC over metadata fields only (not payload)
    payload_hash,  // CRC over metadata + 64-bit payload hash
    full,    // CRC over entire record body (maximum integrity, current default)
};

inline const char* crc_class_to_string(CrcClass value) noexcept {
    switch (value) {
    case CrcClass::none:
        return "none";
    case CrcClass::header:
        return "header";
    case CrcClass::payload_hash:
        return "payload_hash";
    case CrcClass::full:
        return "full";
    }
    return "full";
}

inline CrcClass parse_crc_class(std::string_view value) {
    if (value == "none" || value == "0") {
        return CrcClass::none;
    }
    if (value == "header" || value == "1") {
        return CrcClass::header;
    }
    if (value == "payload_hash" || value == "payload-hash" || value == "hash" || value == "2") {
        return CrcClass::payload_hash;
    }
    if (value == "full" || value == "3") {
        return CrcClass::full;
    }
    throw std::invalid_argument("crc_class must be none, header, payload_hash, or full");
}

enum class AckMode {
    write_ack,
    sync_ack,
};

inline const char* ack_mode_to_string(AckMode mode) noexcept {
    switch (mode) {
    case AckMode::write_ack:
        return "write_ack";
    case AckMode::sync_ack:
        return "sync_ack";
    }
    return "write_ack";
}

inline AckMode parse_ack_mode(std::string_view value) {
    if (value == "write_ack" || value == "write-ack" || value == "write") {
        return AckMode::write_ack;
    }
    if (value == "sync_ack" || value == "sync-ack" || value == "sync") {
        return AckMode::sync_ack;
    }
    throw std::invalid_argument("ack_mode must be write_ack or sync_ack");
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

enum class EmptyRoutePolicy {
    local,
    round_robin,
};

inline const char* empty_route_policy_to_string(EmptyRoutePolicy policy) noexcept {
    switch (policy) {
    case EmptyRoutePolicy::local:
        return "local";
    case EmptyRoutePolicy::round_robin:
        return "round_robin";
    }
    return "local";
}

inline EmptyRoutePolicy parse_empty_route_policy(std::string_view value) {
    if (value == "local") {
        return EmptyRoutePolicy::local;
    }
    if (value == "round_robin" || value == "round-robin" || value == "rr") {
        return EmptyRoutePolicy::round_robin;
    }
    throw std::invalid_argument("empty_route_policy must be local or round_robin");
}

struct EngineConfig {
    std::string log_dir = "logs";
    std::string archive_dir = "archive";
    std::string shard_file_prefix = "shard";
    AckMode ack_mode = AckMode::write_ack;
    RoutingStrategy routing_strategy = RoutingStrategy::hash_modulo;
    EmptyRoutePolicy empty_route_policy = EmptyRoutePolicy::local;
    std::size_t routing_virtual_nodes = 128;
    std::size_t batch_size = 32;
    std::size_t flush_interval_ms = 0;
    std::size_t stream_buffer_size = 64 * 1024;
    std::size_t write_behind = 8;
    std::size_t write_retry_count = 3;
    std::size_t write_retry_backoff_ms = 2;
    std::size_t max_pending_bytes = 0;
    std::size_t pending_bytes_low_watermark = 0;
    std::uint64_t rotate_size_bytes = 0;
    std::uint64_t rotate_interval_seconds = 0;
    std::uint64_t archive_retention_seconds = 0;
    std::size_t max_archived_files_per_shard = 8;
    bool truncate_on_start = true;
    bool checkpoint_enabled = false;
    bool compress_archives = false;
    bool use_dsync = false;
    bool record_crc_enabled = false;
    CrcClass record_crc_class = CrcClass::full;
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
        if (write_behind == 0) {
            throw std::invalid_argument("write_behind must be greater than zero");
        }
        if (write_retry_count == 0) {
            throw std::invalid_argument("write_retry_count must be greater than zero");
        }
        if (max_pending_bytes > 0 && pending_bytes_low_watermark > max_pending_bytes) {
            throw std::invalid_argument("pending_bytes_low_watermark must not exceed max_pending_bytes");
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
