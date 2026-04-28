#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <boost/program_options.hpp>

#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>

#include "log_engine/config_loader.hh"
#include "log_engine/log_engine.hh"

namespace {

log_engine::WriteMode parse_mode(std::string_view value) {
    if (value == "fast") {
        return log_engine::WriteMode::fast;
    }
    if (value == "full") {
        return log_engine::WriteMode::full;
    }
    throw std::invalid_argument("mode must be fast or full");
}

log_engine::RoutingStrategy parse_routing(std::string_view value) {
    return log_engine::parse_routing_strategy(value);
}

std::string make_payload(std::uint64_t index, std::size_t target_size) {
    std::string payload = "demo-log-" + std::to_string(index) + " ";
    if (payload.size() < target_size) {
        payload.append(target_size - payload.size(), 'x');
    }
    return payload;
}

}  // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    namespace bpo = boost::program_options;

    app.add_options()
        ("mode", bpo::value<std::string>()->default_value("fast"), "Write path mode: fast or full")
        ("config", bpo::value<std::string>()->default_value(""), "Path to key=value config file")
        ("log-dir", bpo::value<std::string>()->default_value("logs"), "Directory for shard log files")
        ("archive-dir", bpo::value<std::string>()->default_value("archive"), "Directory for archived log files")
        ("shard-file-prefix", bpo::value<std::string>()->default_value("shard"), "Shard log file prefix")
        ("routing-strategy", bpo::value<std::string>()->default_value("hash_modulo"), "Routing strategy: hash_modulo or consistent_hashing")
        ("routing-virtual-nodes", bpo::value<std::size_t>()->default_value(128), "Virtual nodes per shard for consistent hashing")
        ("batch-size", bpo::value<std::size_t>()->default_value(32), "Number of entries per flush batch")
        ("flush-ms", bpo::value<std::size_t>()->default_value(0), "Periodic flush interval in milliseconds, 0 disables timer-based flush")
        ("fast-path-max-pending-bytes", bpo::value<std::size_t>()->default_value(16384), "Fast path flush threshold in bytes")
        ("messages", bpo::value<std::uint64_t>()->default_value(10000), "Number of demo messages to write")
        ("payload-size", bpo::value<std::size_t>()->default_value(128), "Approximate size of each log payload")
        ("route-keys", bpo::value<std::size_t>()->default_value(0), "Distinct route keys used for shard distribution, 0 keeps writes on the current shard")
        ("emit-delay-ms", bpo::value<std::size_t>()->default_value(0), "Optional delay between demo messages")
        ("write-behind", bpo::value<std::size_t>()->default_value(8), "Output stream write-behind depth")
        ("write-retry-count", bpo::value<std::size_t>()->default_value(3), "Maximum retries for a failed write batch")
        ("write-retry-backoff-ms", bpo::value<std::size_t>()->default_value(2), "Retry backoff in milliseconds")
        ("stream-buffer-size", bpo::value<std::size_t>()->default_value(65536), "Output stream buffer size")
        ("rotate-size-bytes", bpo::value<std::uint64_t>()->default_value(0), "Rotate active file after reaching this size")
        ("rotate-interval-seconds", bpo::value<std::uint64_t>()->default_value(0), "Rotate active file after this many seconds, 0 disables time rotation")
        ("archive-retention-seconds", bpo::value<std::uint64_t>()->default_value(0), "Delete archived files older than this many seconds, 0 disables age cleanup")
        ("max-archived-files", bpo::value<std::size_t>()->default_value(8), "Maximum archived files to keep per shard")
        ("truncate-on-start", bpo::value<bool>()->default_value(true), "Truncate active log files on startup instead of recovering")
        ("checkpoint-enabled", bpo::value<bool>()->default_value(false), "Persist per-shard checkpoint sidecar files")
        ("compress-archives", bpo::value<bool>()->default_value(false), "Compress rotated archives with gzip")
        ("dsync", bpo::value<bool>()->default_value(false), "Enable O_DSYNC when opening shard files")
        ("record-crc-enabled", bpo::value<bool>()->default_value(false), "Emit crc= prefix and verify record checksum")
        ("record-timestamp-enabled", bpo::value<bool>()->default_value(false), "Include ts= field in each record")
        ("record-level-enabled", bpo::value<bool>()->default_value(false), "Include level= field in each record")
        ("record-shard-id-enabled", bpo::value<bool>()->default_value(false), "Include shard= field in each record")
        ("record-sequence-enabled", bpo::value<bool>()->default_value(false), "Include seq= field in each record");

    return app.run(argc, argv, [&app] () -> seastar::future<> {
        log_engine::EngineConfig base;
        base.write_mode = parse_mode(app.configuration()["mode"].as<std::string>());
        base.routing_strategy = parse_routing(app.configuration()["routing-strategy"].as<std::string>());
        base.routing_virtual_nodes = app.configuration()["routing-virtual-nodes"].as<std::size_t>();
        base.log_dir = app.configuration()["log-dir"].as<std::string>();
        base.archive_dir = app.configuration()["archive-dir"].as<std::string>();
        base.shard_file_prefix = app.configuration()["shard-file-prefix"].as<std::string>();
        base.batch_size = app.configuration()["batch-size"].as<std::size_t>();
        base.flush_interval_ms = app.configuration()["flush-ms"].as<std::size_t>();
        base.fast_path_max_pending_bytes = app.configuration()["fast-path-max-pending-bytes"].as<std::size_t>();
        base.stream_buffer_size = app.configuration()["stream-buffer-size"].as<std::size_t>();
        base.write_behind = app.configuration()["write-behind"].as<std::size_t>();
        base.write_retry_count = app.configuration()["write-retry-count"].as<std::size_t>();
        base.write_retry_backoff_ms = app.configuration()["write-retry-backoff-ms"].as<std::size_t>();
        base.rotate_size_bytes = app.configuration()["rotate-size-bytes"].as<std::uint64_t>();
        base.rotate_interval_seconds = app.configuration()["rotate-interval-seconds"].as<std::uint64_t>();
        base.archive_retention_seconds = app.configuration()["archive-retention-seconds"].as<std::uint64_t>();
        base.max_archived_files_per_shard = app.configuration()["max-archived-files"].as<std::size_t>();
        base.truncate_on_start = app.configuration()["truncate-on-start"].as<bool>();
        base.checkpoint_enabled = app.configuration()["checkpoint-enabled"].as<bool>();
        base.compress_archives = app.configuration()["compress-archives"].as<bool>();
        base.use_dsync = app.configuration()["dsync"].as<bool>();
        base.record_crc_enabled = app.configuration()["record-crc-enabled"].as<bool>();
        base.record_timestamp_enabled = app.configuration()["record-timestamp-enabled"].as<bool>();
        base.record_level_enabled = app.configuration()["record-level-enabled"].as<bool>();
        base.record_shard_id_enabled = app.configuration()["record-shard-id-enabled"].as<bool>();
        base.record_sequence_enabled = app.configuration()["record-sequence-enabled"].as<bool>();

        const auto config_file = app.configuration()["config"].as<std::string>();
        const auto file_values = log_engine::load_config_file(config_file);
        auto config = log_engine::apply_engine_config_overrides(base, app.configuration(), file_values);

        const auto total_messages = log_engine::resolve_u64_option(app.configuration(), file_values, "messages", app.configuration()["messages"].as<std::uint64_t>());
        const auto payload_size = log_engine::resolve_size_option(app.configuration(), file_values, "payload-size", app.configuration()["payload-size"].as<std::size_t>());
        const auto route_key_count = log_engine::resolve_size_option(app.configuration(), file_values, "route-keys", app.configuration()["route-keys"].as<std::size_t>());
        const auto emit_delay_ms = log_engine::resolve_size_option(app.configuration(), file_values, "emit-delay-ms", app.configuration()["emit-delay-ms"].as<std::size_t>());

        log_engine::LogEngine engine;
        co_await engine.start(config);

        std::uint64_t index = 0;
        co_await seastar::repeat([&]() -> seastar::future<seastar::stop_iteration> {
            if (index >= total_messages) {
                co_return seastar::stop_iteration::yes;
            }
            const auto route_key = route_key_count == 0
                ? std::string()
                : ("route-" + std::to_string(index % route_key_count));
            co_await engine.info(make_payload(index, payload_size), route_key);
            if (emit_delay_ms > 0) {
                co_await seastar::sleep(std::chrono::milliseconds(emit_delay_ms));
            }
            ++index;
            co_return seastar::stop_iteration::no;
        }).handle_exception([&engine](std::exception_ptr ep) {
            return engine.stop().then([ep] {
                return seastar::make_exception_future<>(ep);
            });
        });

        co_await engine.stop();
    });
}
