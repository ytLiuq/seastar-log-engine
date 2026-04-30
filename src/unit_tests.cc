#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

#include <boost/program_options.hpp>

#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>

#include "log_engine/compat_glog.hh"
#include "log_engine/config_loader.hh"
#include "log_engine/log_manager.hh"
#include "log_engine/log_reader.hh"
#include "log_engine/record_codec.hh"
#include "log_engine/routing.hh"

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

seastar::future<> test_record_codec() {
    log_engine::EngineConfig full_config;
    full_config.record_crc_enabled = true;
    full_config.record_timestamp_enabled = true;
    full_config.record_level_enabled = true;
    full_config.record_shard_id_enabled = true;
    full_config.record_sequence_enabled = true;
    const auto encoded = log_engine::encode_record(
        full_config,
        1,
        42,
        log_engine::LogLevel::warn,
        "2026-01-01 00:00:00.000001",
        "payload");
    const auto parsed = log_engine::parse_record_line(encoded.substr(0, encoded.size() - 1));
    require(parsed.has_value(), "parse_record_line should succeed");
    require(parsed->sequence == 42, "sequence mismatch");
    require(parsed->shard == 1, "shard mismatch");
    require(parsed->level == log_engine::LogLevel::warn, "level mismatch");
    require(parsed->payload == "payload", "payload mismatch");
    require(log_engine::verify_record_line(encoded.substr(0, encoded.size() - 1)), "verify_record_line should succeed");

    log_engine::EngineConfig compact_config;
    compact_config.record_crc_enabled = false;
    compact_config.record_timestamp_enabled = false;
    compact_config.record_level_enabled = false;
    compact_config.record_shard_id_enabled = false;
    compact_config.record_sequence_enabled = false;
    const auto compact = log_engine::encode_record(
        compact_config,
        3,
        7,
        log_engine::LogLevel::error,
        "2026-01-01 00:00:00.000002",
        "compact-payload");
    const auto compact_line = compact.substr(0, compact.size() - 1);
    const auto compact_parsed = log_engine::parse_record_line(compact_line);
    require(compact_parsed.has_value(), "parse_record_line should support payload-only records");
    require(!compact_parsed->has_sequence, "compact record should omit sequence");
    require(compact_parsed->payload == "compact-payload", "compact payload mismatch");
    require(compact_parsed->timestamp.empty(), "compact record should omit timestamp");
    require(log_engine::verify_record_line(compact_line), "verify_record_line should support payload-only records");
    co_return;
}

seastar::future<> test_config_loader(const std::string& root_dir) {
    namespace fs = std::filesystem;
    const auto config_path = (fs::path(root_dir) / "engine-test.conf").string();
    {
        std::ofstream out(config_path, std::ios::trunc);
        out << "ack-mode=sync_ack\n";
        out << "routing-strategy=consistent_hashing\n";
        out << "routing-virtual-nodes=33\n";
        out << "log-dir=/tmp/demo-logs\n";
        out << "batch-size=17\n";
        out << "rotate-interval-seconds=9\n";
        out << "compress-archives=false\n";
        out << "record-crc-enabled=false\n";
        out << "record-sequence-enabled=true\n";
    }

    const auto values = log_engine::load_config_file(config_path);
    boost::program_options::variables_map cli;
    auto config = log_engine::apply_engine_config_overrides(log_engine::EngineConfig{}, cli, values);
    require(config.ack_mode == log_engine::AckMode::sync_ack, "config loader should override ack mode");
    require(config.routing_strategy == log_engine::RoutingStrategy::consistent_hashing, "config loader should override routing strategy");
    require(config.routing_virtual_nodes == 33, "config loader should override routing virtual nodes");
    require(config.log_dir == "/tmp/demo-logs", "config loader should override log_dir");
    require(config.batch_size == 17, "config loader should override batch_size");
    require(config.rotate_interval_seconds == 9, "config loader should override rotate interval");
    require(config.compress_archives == false, "config loader should override compress_archives");
    require(config.record_crc_enabled == false, "config loader should override record_crc_enabled");
    require(config.record_sequence_enabled == true, "config loader should override record_sequence_enabled");
    co_return;
}

seastar::future<> test_consistent_hash_routing() {
    log_engine::ShardRouter modulo_router;
    modulo_router.configure(log_engine::RoutingStrategy::hash_modulo, 128, 4);
    const auto modulo_a = modulo_router.route("route-a", 0);
    const auto modulo_b = modulo_router.route("route-a", 3);
    require(modulo_a.shard == modulo_b.shard, "hash modulo routing should be independent of local shard for non-empty keys");
    require(!modulo_a.used_local_fallback, "non-empty route key should not use fallback");

    log_engine::ShardRouter consistent_router;
    consistent_router.configure(log_engine::RoutingStrategy::consistent_hashing, 64, 4);
    const auto consistent_a = consistent_router.route("route-a", 0);
    const auto consistent_b = consistent_router.route("route-a", 2);
    require(consistent_router.ring_size() == 256, "consistent routing should build shard_count * virtual_nodes ring");
    require(consistent_a.shard == consistent_b.shard, "consistent hashing should be stable for the same key");
    require(consistent_a.token != 0, "consistent hashing should return the selected token");

    const auto fallback = consistent_router.route("", 3);
    require(fallback.shard == 3, "empty route key should fall back to local shard");
    require(fallback.used_local_fallback, "empty route key should mark local fallback");
    co_return;
}

seastar::future<> test_compat_logging(const std::string& root_dir) {
    namespace fs = std::filesystem;
    const auto log_dir = (fs::path(root_dir) / "logs").string();
    const auto archive_dir = (fs::path(root_dir) / "archive").string();
    fs::create_directories(log_dir);
    fs::create_directories(archive_dir);
    for (const auto& entry : fs::directory_iterator(log_dir)) {
        fs::remove(entry.path());
    }
    for (const auto& entry : fs::directory_iterator(archive_dir)) {
        fs::remove(entry.path());
    }

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 2;

    log_engine::LogEngine engine;
    co_await engine.start(config);
    log_engine::compat::bind(engine);

    LOG_INFO << "compat-info";
    LOG_WARNING_R("route-a") << "compat-warn";
    co_await log_engine::compat::flush();
    log_engine::compat::unbind();
    co_await engine.stop();

    log_engine::ReadQuery query;
    query.include_archive = false;
    query.limit = 10;
    const auto segments = log_engine::collect_segments(config, query);
    const auto records = log_engine::read_records(segments, query);
    require(records.size() >= 2, "compat logging should emit at least 2 records");

    bool found_info = false;
    bool found_warn = false;
    for (const auto& record : records) {
        if (record.payload.find("compat-info") != std::string::npos) {
            found_info = true;
        }
        if (record.payload.find("compat-warn") != std::string::npos) {
            found_warn = true;
        }
    }
    require(found_info, "missing compat info record");
    require(found_warn, "missing compat warn record");
    co_return;
}

seastar::future<> test_compat_unbound_drops_messages(const std::string& root_dir) {
    namespace fs = std::filesystem;
    const auto log_dir = (fs::path(root_dir) / "compat-unbound-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "compat-unbound-archive").string();
    fs::create_directories(log_dir);
    fs::create_directories(archive_dir);
    for (const auto& entry : fs::directory_iterator(log_dir)) {
        fs::remove(entry.path());
    }
    for (const auto& entry : fs::directory_iterator(archive_dir)) {
        fs::remove(entry.path());
    }

    LOG_INFO << "unbound-message-should-drop";

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 1;
    config.stream_buffer_size = 512;
    config.write_behind = 1;

    log_engine::LogEngine engine;
    co_await engine.start(config);
    log_engine::compat::bind(engine);
    co_await log_engine::compat::flush();
    co_await engine.info("bound-message", "route-b");
    log_engine::compat::unbind();
    co_await engine.stop();

    log_engine::ReadQuery query;
    query.include_archive = false;
    query.limit = 10;
    const auto segments = log_engine::collect_segments(config, query);
    const auto records = log_engine::read_records(segments, query);
    require(records.size() == 1, "unbound compat logging should not leak into later flushes");
    require(records.front().payload == "bound-message", "expected only explicitly bound log message");
    co_return;
}

seastar::future<> test_unified_large_payload_blocks(const std::string& root_dir) {
    namespace fs = std::filesystem;
    const auto log_dir = (fs::path(root_dir) / "fast-large-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "fast-large-archive").string();
    fs::create_directories(log_dir);
    fs::create_directories(archive_dir);
    for (const auto& entry : fs::directory_iterator(log_dir)) {
        fs::remove(entry.path());
    }

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 16;
    config.stream_buffer_size = 4096;

    const std::string payload_a(5000, 'x');
    const std::string payload_b(5200, 'y');
    const std::string payload_c(73, 'z');

    log_engine::LogEngine engine;
    co_await engine.start(config);
    co_await engine.info(payload_a, "route-fast");
    co_await engine.info(payload_b, "route-fast");
    co_await engine.info(payload_c, "route-fast");
    co_await engine.stop();

    std::optional<fs::path> shard_path;
    for (const auto& entry : fs::directory_iterator(log_dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".log") {
            shard_path = entry.path();
            break;
        }
    }
    require(shard_path.has_value(), "unified writer test should find a shard log");

    std::ifstream in(*shard_path, std::ios::binary);
    require(in.is_open(), "unified writer shard log should be readable");

    std::string line;
    require(static_cast<bool>(std::getline(in, line)), "missing first unified writer record");
    require(line.find("route-fast") != std::string::npos, "first unified writer record should contain route key");
    require(static_cast<bool>(std::getline(in, line)), "missing second unified writer record");
    require(line.find("route-fast") != std::string::npos, "second unified writer record should contain route key");
    require(static_cast<bool>(std::getline(in, line)), "missing third unified writer record");
    require(line.find("route-fast") != std::string::npos, "third unified writer record should contain route key");
    require(!static_cast<bool>(std::getline(in, line)), "unified writer test should produce exactly three records");
    co_return;
}

seastar::future<> test_time_rotation_and_archive_read(const std::string& root_dir) {
    namespace fs = std::filesystem;
    const auto log_dir = (fs::path(root_dir) / "time-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "time-archive").string();
    fs::create_directories(log_dir);
    fs::create_directories(archive_dir);
    for (const auto& entry : fs::directory_iterator(log_dir)) {
        fs::remove(entry.path());
    }
    for (const auto& entry : fs::directory_iterator(archive_dir)) {
        fs::remove(entry.path());
    }

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 1;
    config.flush_interval_ms = 1;
    config.rotate_size_bytes = 0;
    config.rotate_interval_seconds = 1;
    config.compress_archives = true;
    config.max_archived_files_per_shard = 8;

    log_engine::LogEngine engine;
    co_await engine.start(config);
    co_await engine.info("first-record", "route-a");
    co_await seastar::sleep(std::chrono::milliseconds(1200));
    co_await engine.info("second-record", "route-a");
    co_await engine.stop();

    std::size_t gz_count = 0;
    for (const auto& entry : fs::directory_iterator(archive_dir)) {
        if (entry.path().extension() == ".gz") {
            ++gz_count;
        }
    }
    require(gz_count >= 1, "time rotation should create at least one gz archive");

    log_engine::ReadQuery query;
    query.include_archive = true;
    query.limit = 20;
    const auto segments = log_engine::collect_segments(config, query);
    const auto records = log_engine::read_records(segments, query);
    bool found_first = false;
    bool found_second = false;
    for (const auto& record : records) {
        found_first = found_first || record.payload.find("first-record") != std::string::npos;
        found_second = found_second || record.payload.find("second-record") != std::string::npos;
    }
    require(found_first, "archive read should include rotated record");
    require(found_second, "archive read should include active record");
    co_return;
}

seastar::future<> test_recovery_scan(const std::string& root_dir) {
    namespace fs = std::filesystem;
    const auto log_dir = (fs::path(root_dir) / "recovery-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "recovery-archive").string();
    fs::create_directories(log_dir);
    fs::create_directories(archive_dir);
    for (const auto& entry : fs::directory_iterator(log_dir)) {
        fs::remove(entry.path());
    }

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 2;
    config.checkpoint_enabled = true;
    config.record_sequence_enabled = false;

    log_engine::LogEngine engine;
    co_await engine.start(config);
    co_await engine.info("recovery-a", "route-r");
    co_await engine.info("recovery-b", "route-r");
    co_await engine.stop();

    std::optional<fs::path> shard_path;
    for (const auto& entry : fs::directory_iterator(log_dir)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto name = entry.path().filename().string();
        if (name.rfind("shard-", 0) != 0 || entry.path().extension() != ".log") {
            continue;
        }
        if (fs::file_size(entry.path()) == 0) {
            continue;
        }
        shard_path = entry.path();
        break;
    }
    require(shard_path.has_value(), "recovery test should find a non-empty shard log");

    {
        std::ofstream out(*shard_path, std::ios::app | std::ios::binary);
        out << "BROKEN_TAIL";
    }

    log_engine::LogManager manager;
    const auto recovery = co_await manager.recover_active_file(shard_path->string(), 4096);
    require(recovery.sequence == 2, "recovery should preserve next sequence");
    require(recovery.logical_size > 0, "recovery should preserve valid_size");
    require(recovery.logical_size < static_cast<std::uint64_t>(fs::file_size(*shard_path)), "recovery should cut broken tail");
    co_return;
}

}  // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    namespace bpo = boost::program_options;
    app.add_options()
        ("root-dir", bpo::value<std::string>()->default_value("/root/workspace/seastar-log-engine/test-tmp"), "Temporary root directory for unit tests");

    return app.run(argc, argv, [&app] () -> seastar::future<> {
        const auto root_dir = app.configuration()["root-dir"].as<std::string>();
        co_await test_record_codec();
        co_await test_config_loader(root_dir);
        co_await test_consistent_hash_routing();
        co_await test_compat_logging(root_dir);
        co_await test_compat_unbound_drops_messages(root_dir);
        co_await test_unified_large_payload_blocks(root_dir);
        co_await test_time_rotation_and_archive_read(root_dir);
        co_await test_recovery_scan(root_dir);
        co_return;
    });
}
