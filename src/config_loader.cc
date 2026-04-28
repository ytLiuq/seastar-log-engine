#include "log_engine/config_loader.hh"

#include <charconv>
#include <cctype>
#include <fstream>
#include <stdexcept>

namespace log_engine {

namespace {

std::string trim(std::string value) {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
        value.erase(value.begin());
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
        value.pop_back();
    }
    return value;
}

template <typename T>
T parse_integer(const std::string& value, const char* name) {
    T parsed{};
    const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed, 10);
    if (result.ec != std::errc{} || result.ptr != value.data() + value.size()) {
        throw std::invalid_argument(std::string("invalid integer for option ") + name + ": " + value);
    }
    return parsed;
}

bool parse_bool(const std::string& value, const char* name) {
    if (value == "1" || value == "true" || value == "yes" || value == "on") {
        return true;
    }
    if (value == "0" || value == "false" || value == "no" || value == "off") {
        return false;
    }
    throw std::invalid_argument(std::string("invalid bool for option ") + name + ": " + value);
}

WriteMode parse_write_mode(const std::string& value, const char* name) {
    if (value == "fast") {
        return WriteMode::fast;
    }
    if (value == "full") {
        return WriteMode::full;
    }
    throw std::invalid_argument(std::string("invalid mode for option ") + name + ": " + value);
}

AckMode parse_ack_mode_option(const std::string& value, const char* name) {
    try {
        return parse_ack_mode(value);
    } catch (const std::invalid_argument&) {
        throw std::invalid_argument(std::string("invalid ack mode for option ") + name + ": " + value);
    }
}

RoutingStrategy parse_routing_strategy_option(const std::string& value, const char* name) {
    try {
        return parse_routing_strategy(value);
    } catch (const std::invalid_argument&) {
        throw std::invalid_argument(std::string("invalid routing strategy for option ") + name + ": " + value);
    }
}

bool should_take_file_value(
    const boost::program_options::variables_map& cli,
    const std::string& name,
    const ConfigMap& file_values) {
    const auto file_it = file_values.find(name);
    if (file_it == file_values.end()) {
        return false;
    }
    const auto cli_it = cli.find(name);
    return cli_it == cli.end() || cli_it->second.defaulted();
}

}  // namespace

ConfigMap load_config_file(const std::string& path) {
    ConfigMap values;
    if (path.empty()) {
        return values;
    }

    std::ifstream in(path);
    if (!in.good()) {
        throw std::runtime_error("failed to open config file: " + path);
    }

    std::string line;
    while (std::getline(in, line)) {
        auto trimmed = trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        const auto pos = trimmed.find('=');
        if (pos == std::string::npos) {
            continue;
        }
        auto key = trim(trimmed.substr(0, pos));
        auto value = trim(trimmed.substr(pos + 1));
        if (!key.empty()) {
            values[key] = value;
        }
    }
    return values;
}

EngineConfig apply_engine_config_overrides(
    EngineConfig base,
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values) {
    base.write_mode = resolve_write_mode_option(cli, file_values, "mode", base.write_mode);
    base.ack_mode = resolve_ack_mode_option(cli, file_values, "ack-mode", base.ack_mode);
    base.routing_strategy = resolve_routing_strategy_option(cli, file_values, "routing-strategy", base.routing_strategy);
    base.routing_virtual_nodes = resolve_size_option(cli, file_values, "routing-virtual-nodes", base.routing_virtual_nodes);
    base.log_dir = resolve_string_option(cli, file_values, "log-dir", base.log_dir);
    base.archive_dir = resolve_string_option(cli, file_values, "archive-dir", base.archive_dir);
    base.shard_file_prefix = resolve_string_option(cli, file_values, "shard-file-prefix", base.shard_file_prefix);
    base.batch_size = resolve_size_option(cli, file_values, "batch-size", base.batch_size);
    base.flush_interval_ms = resolve_size_option(cli, file_values, "flush-ms", base.flush_interval_ms);
    base.fast_path_max_pending_bytes = resolve_size_option(cli, file_values, "fast-path-max-pending-bytes", base.fast_path_max_pending_bytes);
    base.stream_buffer_size = resolve_size_option(cli, file_values, "stream-buffer-size", base.stream_buffer_size);
    base.write_behind = resolve_size_option(cli, file_values, "write-behind", base.write_behind);
    base.write_retry_count = resolve_size_option(cli, file_values, "write-retry-count", base.write_retry_count);
    base.write_retry_backoff_ms = resolve_size_option(cli, file_values, "write-retry-backoff-ms", base.write_retry_backoff_ms);
    base.rotate_size_bytes = resolve_u64_option(cli, file_values, "rotate-size-bytes", base.rotate_size_bytes);
    base.rotate_interval_seconds = resolve_u64_option(cli, file_values, "rotate-interval-seconds", base.rotate_interval_seconds);
    base.archive_retention_seconds = resolve_u64_option(cli, file_values, "archive-retention-seconds", base.archive_retention_seconds);
    base.max_archived_files_per_shard = resolve_size_option(cli, file_values, "max-archived-files", base.max_archived_files_per_shard);
    base.truncate_on_start = resolve_bool_option(cli, file_values, "truncate-on-start", base.truncate_on_start);
    base.checkpoint_enabled = resolve_bool_option(cli, file_values, "checkpoint-enabled", base.checkpoint_enabled);
    base.compress_archives = resolve_bool_option(cli, file_values, "compress-archives", base.compress_archives);
    base.use_dsync = resolve_bool_option(cli, file_values, "dsync", base.use_dsync);
    base.record_crc_enabled = resolve_bool_option(cli, file_values, "record-crc-enabled", base.record_crc_enabled);
    base.record_timestamp_enabled = resolve_bool_option(cli, file_values, "record-timestamp-enabled", base.record_timestamp_enabled);
    base.record_level_enabled = resolve_bool_option(cli, file_values, "record-level-enabled", base.record_level_enabled);
    base.record_shard_id_enabled = resolve_bool_option(cli, file_values, "record-shard-id-enabled", base.record_shard_id_enabled);
    base.record_sequence_enabled = resolve_bool_option(cli, file_values, "record-sequence-enabled", base.record_sequence_enabled);
    return base;
}

std::string resolve_string_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    const std::string& current_value) {
    if (!should_take_file_value(cli, name, file_values)) {
        return current_value;
    }
    return file_values.at(name);
}

std::uint64_t resolve_u64_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    std::uint64_t current_value) {
    if (!should_take_file_value(cli, name, file_values)) {
        return current_value;
    }
    return parse_integer<std::uint64_t>(file_values.at(name), name.c_str());
}

std::size_t resolve_size_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    std::size_t current_value) {
    if (!should_take_file_value(cli, name, file_values)) {
        return current_value;
    }
    return parse_integer<std::size_t>(file_values.at(name), name.c_str());
}

bool resolve_bool_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    bool current_value) {
    if (!should_take_file_value(cli, name, file_values)) {
        return current_value;
    }
    return parse_bool(file_values.at(name), name.c_str());
}

WriteMode resolve_write_mode_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    WriteMode current_value) {
    if (!should_take_file_value(cli, name, file_values)) {
        return current_value;
    }
    return parse_write_mode(file_values.at(name), name.c_str());
}

AckMode resolve_ack_mode_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    AckMode current_value) {
    if (!should_take_file_value(cli, name, file_values)) {
        return current_value;
    }
    return parse_ack_mode_option(file_values.at(name), name.c_str());
}

RoutingStrategy resolve_routing_strategy_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    RoutingStrategy current_value) {
    if (!should_take_file_value(cli, name, file_values)) {
        return current_value;
    }
    return parse_routing_strategy_option(file_values.at(name), name.c_str());
}

}  // namespace log_engine
