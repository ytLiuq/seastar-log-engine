#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include <boost/program_options.hpp>
#include <fmt/format.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/loop.hh>

#include "log_engine/config_loader.hh"
#include "log_engine/log_engine.hh"

namespace {

std::string make_payload(std::uint64_t index, std::size_t target_size) {
    std::string payload = "bench-log-" + std::to_string(index) + " ";
    if (payload.size() < target_size) {
        payload.append(target_size - payload.size(), 'b');
    }
    return payload;
}

}  // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    namespace bpo = boost::program_options;

    app.add_options()
        ("config", bpo::value<std::string>()->default_value(""), "Path to key=value config file")
        ("log-dir", bpo::value<std::string>()->default_value("logs"), "Directory for shard log files")
        ("archive-dir", bpo::value<std::string>()->default_value("archive"), "Directory for archived log files")
        ("shard-file-prefix", bpo::value<std::string>()->default_value("shard"), "Shard log file prefix")
        ("messages", bpo::value<std::uint64_t>()->default_value(200000), "Number of messages to write")
        ("payload-size", bpo::value<std::size_t>()->default_value(256), "Approximate size of each log payload")
        ("route-keys", bpo::value<std::size_t>()->default_value(16), "Distinct route keys used for shard distribution")
        ("batch-size", bpo::value<std::size_t>()->default_value(1024), "Number of entries per flush batch")
        ("flush-ms", bpo::value<std::size_t>()->default_value(1), "Periodic flush interval in milliseconds")
        ("rotate-size-bytes", bpo::value<std::uint64_t>()->default_value(64 * 1024 * 1024), "Rotate active file after reaching this size")
        ("rotate-interval-seconds", bpo::value<std::uint64_t>()->default_value(0), "Rotate active file after this many seconds, 0 disables time rotation")
        ("archive-retention-seconds", bpo::value<std::uint64_t>()->default_value(0), "Delete archived files older than this many seconds, 0 disables age cleanup")
        ("truncate-on-start", bpo::value<bool>()->default_value(true), "Truncate active log files on startup instead of recovering")
        ("checkpoint-enabled", bpo::value<bool>()->default_value(true), "Persist per-shard checkpoint sidecar files")
        ("compress-archives", bpo::value<bool>()->default_value(true), "Compress rotated archives with gzip")
        ("write-retry-count", bpo::value<std::size_t>()->default_value(3), "Maximum retries for a failed write batch")
        ("write-retry-backoff-ms", bpo::value<std::size_t>()->default_value(2), "Retry backoff in milliseconds")
        ("inflight", bpo::value<std::size_t>()->default_value(256), "Maximum writes issued concurrently")
        ("record-crc-enabled", bpo::value<bool>()->default_value(true), "Emit crc= prefix and verify record checksum")
        ("record-timestamp-enabled", bpo::value<bool>()->default_value(true), "Include ts= field in each record")
        ("record-level-enabled", bpo::value<bool>()->default_value(true), "Include level= field in each record")
        ("record-shard-id-enabled", bpo::value<bool>()->default_value(true), "Include shard= field in each record");

    return app.run(argc, argv, [&app] () -> seastar::future<> {
        log_engine::EngineConfig base;
        base.log_dir = app.configuration()["log-dir"].as<std::string>();
        base.archive_dir = app.configuration()["archive-dir"].as<std::string>();
        base.shard_file_prefix = app.configuration()["shard-file-prefix"].as<std::string>();
        base.batch_size = app.configuration()["batch-size"].as<std::size_t>();
        base.flush_interval_ms = app.configuration()["flush-ms"].as<std::size_t>();
        base.rotate_size_bytes = app.configuration()["rotate-size-bytes"].as<std::uint64_t>();
        base.rotate_interval_seconds = app.configuration()["rotate-interval-seconds"].as<std::uint64_t>();
        base.archive_retention_seconds = app.configuration()["archive-retention-seconds"].as<std::uint64_t>();
        base.truncate_on_start = app.configuration()["truncate-on-start"].as<bool>();
        base.checkpoint_enabled = app.configuration()["checkpoint-enabled"].as<bool>();
        base.compress_archives = app.configuration()["compress-archives"].as<bool>();
        base.write_retry_count = app.configuration()["write-retry-count"].as<std::size_t>();
        base.write_retry_backoff_ms = app.configuration()["write-retry-backoff-ms"].as<std::size_t>();
        base.record_crc_enabled = app.configuration()["record-crc-enabled"].as<bool>();
        base.record_timestamp_enabled = app.configuration()["record-timestamp-enabled"].as<bool>();
        base.record_level_enabled = app.configuration()["record-level-enabled"].as<bool>();
        base.record_shard_id_enabled = app.configuration()["record-shard-id-enabled"].as<bool>();

        const auto config_file = app.configuration()["config"].as<std::string>();
        const auto file_values = log_engine::load_config_file(config_file);
        auto config = log_engine::apply_engine_config_overrides(base, app.configuration(), file_values);

        const auto total_messages = log_engine::resolve_u64_option(app.configuration(), file_values, "messages", app.configuration()["messages"].as<std::uint64_t>());
        const auto payload_size = log_engine::resolve_size_option(app.configuration(), file_values, "payload-size", app.configuration()["payload-size"].as<std::size_t>());
        const auto route_key_count = log_engine::resolve_size_option(app.configuration(), file_values, "route-keys", app.configuration()["route-keys"].as<std::size_t>());
        const auto inflight = log_engine::resolve_size_option(app.configuration(), file_values, "inflight", app.configuration()["inflight"].as<std::size_t>());

        log_engine::LogEngine engine;
        co_await engine.start(config);

        std::vector<std::uint64_t> indices;
        indices.reserve(total_messages);
        for (std::uint64_t i = 0; i < total_messages; ++i) {
            indices.push_back(i);
        }

        auto start = std::chrono::steady_clock::now();
        co_await seastar::max_concurrent_for_each(indices, inflight, [&](std::uint64_t current) {
            const auto route_key = "route-" + std::to_string(current % route_key_count);
            return engine.info(make_payload(current, payload_size), route_key);
        });

        co_await engine.stop();

        auto end = std::chrono::steady_clock::now();
        const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        const auto throughput = elapsed_us == 0 ? 0.0 : (static_cast<double>(total_messages) * 1000000.0 / static_cast<double>(elapsed_us));
        const auto avg_latency_us = total_messages == 0 ? 0.0 : (static_cast<double>(elapsed_us) / static_cast<double>(total_messages));

        fmt::print(
            "messages={} elapsed_us={} throughput_msg_per_sec={:.2f} avg_submit_us={:.4f}\n",
            total_messages,
            elapsed_us,
            throughput,
            avg_latency_us);
    });
}
