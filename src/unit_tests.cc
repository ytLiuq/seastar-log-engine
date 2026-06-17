#include <filesystem>
#include <fstream>
#include <atomic>
#include <thread>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#include <boost/program_options.hpp>
#include <log_engine_query.pb.h>
#include <zlib.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>

#include "log_engine/agent_support.hh"
#include "log_engine/compat_glog.hh"
#include "log_engine/config_loader.hh"
#include "log_engine/log_layout.hh"
#include "log_engine/log_manager.hh"
#include "log_engine/log_reader.hh"
#include "log_engine/record_codec.hh"
#include "log_engine/routing.hh"

seastar::future<> test_recovery_scan_large_file_streaming(const std::string& root_dir);
seastar::future<> test_health_counter_window_eviction();
seastar::future<> test_limit_zero_returns_no_records(const std::string& root_dir);
seastar::future<> test_query_records_proto_defaults();

namespace {
namespace fs = std::filesystem;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void reset_directory(const fs::path& dir) {
    fs::create_directories(dir);
    for (const auto& entry : fs::directory_iterator(dir)) {
        fs::remove_all(entry.path());
    }
}

std::optional<fs::path> find_non_empty_shard_log(const std::string& log_dir) {
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
        return entry.path();
    }
    return std::nullopt;
}

std::string read_file(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    require(in.good(), "failed to open test file");
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

std::vector<log_engine::ParsedRecord> read_back_records(const log_engine::EngineConfig& config, bool include_archive, std::size_t limit) {
    log_engine::ReadQuery query;
    query.include_archive = include_archive;
    query.limit = limit;
    const auto segments = log_engine::collect_segments(config, query);
    return log_engine::read_records(segments, query);
}

struct FakeHttpSink {
    std::thread worker;
    std::atomic<bool> ready{false};
    std::string received_body;
    int status = 200;
};

void run_fake_http_sink_once(FakeHttpSink& sink, std::uint16_t port) {
    sink.worker = std::thread([&sink, port] {
        const int server = ::socket(AF_INET, SOCK_STREAM, 0);
        if (server < 0) {
            return;
        }
        int reuse = 1;
        ::setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in address {};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        if (::bind(server, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 || ::listen(server, 1) != 0) {
            ::close(server);
            return;
        }
        sink.ready.store(true, std::memory_order_release);
        const int client = ::accept(server, nullptr, nullptr);
        if (client >= 0) {
            std::string request;
            char buffer[1024];
            while (request.find("\r\n\r\n") == std::string::npos) {
                const auto rc = ::recv(client, buffer, sizeof(buffer), 0);
                if (rc <= 0) {
                    break;
                }
                request.append(buffer, static_cast<std::size_t>(rc));
            }
            const auto header_end = request.find("\r\n\r\n");
            std::size_t content_length = 0;
            const auto length_pos = request.find("Content-Length:");
            if (length_pos != std::string::npos) {
                const auto value_start = length_pos + std::string("Content-Length:").size();
                const auto value_end = request.find("\r\n", value_start);
                content_length = static_cast<std::size_t>(std::stoul(request.substr(value_start, value_end - value_start)));
            }
            if (header_end != std::string::npos) {
                sink.received_body = request.substr(header_end + 4);
            }
            while (sink.received_body.size() < content_length) {
                const auto rc = ::recv(client, buffer, sizeof(buffer), 0);
                if (rc <= 0) {
                    break;
                }
                sink.received_body.append(buffer, static_cast<std::size_t>(rc));
            }
            const auto response = std::string("HTTP/1.1 ") +
                std::to_string(sink.status) +
                (sink.status >= 200 && sink.status < 300 ? " OK\r\n" : " Error\r\n") +
                "Content-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}";
            ::send(client, response.data(), response.size(), 0);
            ::close(client);
        }
        ::close(server);
    });
}

void write_gzip_file(const fs::path& path, std::string_view content) {
    gzFile out = gzopen(path.c_str(), "wb");
    require(out != nullptr, "failed to open gzip output in test");
    const auto written = gzwrite(out, content.data(), static_cast<unsigned>(content.size()));
    require(written == static_cast<int>(content.size()), "failed to write gzip content in test");
    require(gzclose(out) == Z_OK, "failed to close gzip output in test");
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

    const auto sanitized = log_engine::encode_record(
        full_config,
        2,
        43,
        log_engine::LogLevel::error,
        "2026-01-01 00:00:00.000003",
        "row-1\nrow-2\trow-3\rrow-4");
    const auto sanitized_line = sanitized.substr(0, sanitized.size() - 1);
    const auto sanitized_parsed = log_engine::parse_record_line(sanitized_line);
    require(sanitized_parsed.has_value(), "parse_record_line should support sanitized payload");
    require(sanitized_parsed->payload == "row-1 row-2 row-3 row-4", "sanitized payload mismatch");
    require(log_engine::verify_record_line(sanitized_line), "verify_record_line should support sanitized payload");
    co_return;
}

seastar::future<> test_config_loader(const std::string& root_dir) {
    const auto config_path = (fs::path(root_dir) / "engine-test.conf").string();
    {
        std::ofstream out(config_path, std::ios::trunc);
        out << "ack-mode=sync_ack\n";
        out << "routing-strategy=consistent_hashing\n";
        out << "empty-route-policy=round_robin\n";
        out << "routing-virtual-nodes=33\n";
        out << "log-dir=/tmp/demo-logs\n";
        out << "batch-size=17\n";
        out << "max-pending-bytes=8192\n";
        out << "pending-bytes-low-watermark=2048\n";
        out << "rotate-interval-seconds=9\n";
        out << "compress-archives=false\n";
        out << "record-crc-enabled=false\n";
        out << "record-crc-class=payload_hash\n";
        out << "record-sequence-enabled=true\n";
    }

    const auto values = log_engine::load_config_file(config_path);
    boost::program_options::variables_map cli;
    auto config = log_engine::apply_engine_config_overrides(log_engine::EngineConfig{}, cli, values);
    require(config.ack_mode == log_engine::AckMode::sync_ack, "config loader should override ack mode");
    require(config.routing_strategy == log_engine::RoutingStrategy::consistent_hashing, "config loader should override routing strategy");
    require(config.empty_route_policy == log_engine::EmptyRoutePolicy::round_robin, "config loader should override empty route policy");
    require(config.routing_virtual_nodes == 33, "config loader should override routing virtual nodes");
    require(config.log_dir == "/tmp/demo-logs", "config loader should override log_dir");
    require(config.batch_size == 17, "config loader should override batch_size");
    require(config.max_pending_bytes == 8192, "config loader should override max_pending_bytes");
    require(config.pending_bytes_low_watermark == 2048, "config loader should override pending_bytes_low_watermark");
    require(config.rotate_interval_seconds == 9, "config loader should override rotate interval");
    require(config.compress_archives == false, "config loader should override compress_archives");
    require(config.record_crc_enabled == false, "config loader should override record_crc_enabled");
    require(config.record_crc_class == log_engine::CrcClass::payload_hash, "config loader should override record_crc_class");
    require(config.record_sequence_enabled == true, "config loader should override record_sequence_enabled");
    co_return;
}

seastar::future<> test_config_validation() {
    log_engine::EngineConfig valid;
    valid.max_pending_bytes = 4096;
    valid.pending_bytes_low_watermark = 2048;
    valid.validate();

    log_engine::EngineConfig invalid;
    invalid.max_pending_bytes = 4096;
    invalid.pending_bytes_low_watermark = 8192;
    bool threw = false;
    try {
        invalid.validate();
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    require(threw, "config validation should reject pending_bytes_low_watermark above max_pending_bytes");
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

    const auto rr0 = consistent_router.route("", 3, log_engine::EmptyRoutePolicy::round_robin, 0);
    const auto rr1 = consistent_router.route("", 3, log_engine::EmptyRoutePolicy::round_robin, 1);
    const auto rr5 = consistent_router.route("", 3, log_engine::EmptyRoutePolicy::round_robin, 5);
    require(rr0.shard == 0, "round-robin empty route should start from shard zero");
    require(rr1.shard == 1, "round-robin empty route should advance across shards");
    require(rr5.shard == 1, "round-robin empty route should wrap by shard count");
    require(!rr0.used_local_fallback, "round-robin empty route should not report local fallback");
    co_return;
}

seastar::future<> test_compat_logging(const std::string& root_dir) {
    const auto log_dir = (fs::path(root_dir) / "logs").string();
    const auto archive_dir = (fs::path(root_dir) / "archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

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

    const auto records = read_back_records(config, false, 10);
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
    const auto log_dir = (fs::path(root_dir) / "compat-unbound-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "compat-unbound-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

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

    const auto records = read_back_records(config, false, 10);
    require(records.size() == 1, "unbound compat logging should not leak into later flushes");
    require(records.front().payload == "bound-message", "expected only explicitly bound log message");
    co_return;
}

seastar::future<> test_unified_large_payload_blocks(const std::string& root_dir) {
    const auto log_dir = (fs::path(root_dir) / "fast-large-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "fast-large-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

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

    std::size_t non_empty_logs = 0;
    for (const auto& entry : fs::directory_iterator(log_dir)) {
        if (!entry.is_regular_file() || entry.path().extension() != ".log") {
            continue;
        }
        if (fs::file_size(entry.path()) > 0) {
            ++non_empty_logs;
        }
    }
    require(non_empty_logs == 1, "same route key should route large payloads to exactly one non-empty shard log");

    const auto records = read_back_records(config, false, 10);
    require(records.size() == 3, "unified writer test should read back exactly three records");
    require(records[0].payload == payload_a, "first unified writer record payload mismatch");
    require(records[1].payload == payload_b, "second unified writer record payload mismatch");
    require(records[2].payload == payload_c, "third unified writer record payload mismatch");
    co_return;
}

seastar::future<> test_append_batch_routes_and_persists(const std::string& root_dir) {
    const auto log_dir = (fs::path(root_dir) / "append-batch-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "append-batch-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 16;
    config.record_sequence_enabled = true;

    std::vector<log_engine::LogMessage> batch;
    batch.push_back({.level = log_engine::LogLevel::info, .payload = "batch-a", .route_key = "route-a"});
    batch.push_back({.level = log_engine::LogLevel::warn, .payload = "batch-b", .route_key = "route-b"});
    batch.push_back({.level = log_engine::LogLevel::error, .payload = "batch-c", .route_key = "route-a"});
    batch.push_back({.level = log_engine::LogLevel::info, .payload = "batch-d", .route_key = "route-c"});

    log_engine::LogEngine engine;
    co_await engine.start(config);
    co_await engine.append_batch(std::move(batch));
    co_await engine.stop();

    const auto records = read_back_records(config, false, 16);
    require(records.size() == 4, "append_batch should persist all batched records");
    bool found_a = false;
    bool found_b = false;
    bool found_c = false;
    bool found_d = false;
    for (const auto& record : records) {
        found_a = found_a || record.payload == "batch-a";
        found_b = found_b || record.payload == "batch-b";
        found_c = found_c || record.payload == "batch-c";
        found_d = found_d || record.payload == "batch-d";
    }
    require(found_a && found_b && found_c && found_d, "append_batch should preserve every payload");
    co_return;
}

seastar::future<> test_time_rotation_and_archive_read(const std::string& root_dir) {
    const auto log_dir = (fs::path(root_dir) / "time-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "time-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

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

    const auto records = read_back_records(config, true, 20);
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

seastar::future<> test_archive_duplicate_prefers_plain_log(const std::string& root_dir) {
    log_engine::reset_reader_stats();
    const auto log_dir = (fs::path(root_dir) / "duplicate-archive-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "duplicate-archive-files").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.record_sequence_enabled = false;

    const auto plain_archive_path = log_engine::layout::archive_log_path(config, 0, 123456789, 7, false);
    const auto gzip_archive_path = log_engine::layout::archive_log_path(config, 0, 123456789, 7, true);
    const auto active_path = log_engine::layout::active_log_path(config, 0);

    {
        std::ofstream out(plain_archive_path, std::ios::binary | std::ios::trunc);
        out << log_engine::encode_record(
            config,
            0,
            0,
            log_engine::LogLevel::info,
            "",
            "plain-archive-record");
    }
    write_gzip_file(
        gzip_archive_path,
        log_engine::encode_record(
            config,
            0,
            0,
            log_engine::LogLevel::info,
            "",
            "gzip-duplicate-record"));
    {
        std::ofstream out(active_path, std::ios::binary | std::ios::trunc);
        out << log_engine::encode_record(
            config,
            0,
            1,
            log_engine::LogLevel::info,
            "",
            "active-record");
    }

    const auto records = read_back_records(config, true, 10);
    require(records.size() == 2, "duplicate archive test should only return one archived record plus one active record");
    require(records[0].payload == "plain-archive-record", "duplicate archive test should prefer plain archive record");
    require(records[1].payload == "active-record", "duplicate archive test should keep active record");
    const auto stats = log_engine::get_reader_stats();
    require(stats.segments_read == 2, "duplicate archive test should read two segments");
    require(stats.archive_segments_read == 1, "duplicate archive test should read one archive segment");
    require(stats.active_segments_read == 1, "duplicate archive test should read one active segment");
    require(stats.records_returned == 2, "duplicate archive test should return two records");
    co_return;
}

seastar::future<> test_reader_stops_after_corrupted_segment_line(const std::string& root_dir) {
    log_engine::reset_reader_stats();
    const auto log_dir = (fs::path(root_dir) / "corrupted-segment-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "corrupted-segment-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.record_sequence_enabled = false;

    const auto archive_path = log_engine::layout::archive_log_path(config, 0, 2233445566, 1, false);
    const auto active_path = log_engine::layout::active_log_path(config, 0);

    {
        std::ofstream out(archive_path, std::ios::binary | std::ios::trunc);
        out << log_engine::encode_record(
            config,
            0,
            0,
            log_engine::LogLevel::info,
            "",
            "archive-before-corruption");
        out << "crc=00000000\tpayload=corrupted-archive-record\n";
        out << log_engine::encode_record(
            config,
            0,
            1,
            log_engine::LogLevel::info,
            "",
            "archive-after-corruption");
    }
    {
        std::ofstream out(active_path, std::ios::binary | std::ios::trunc);
        out << log_engine::encode_record(
            config,
            0,
            2,
            log_engine::LogLevel::info,
            "",
            "active-after-corruption");
    }

    const auto records = read_back_records(config, true, 10);
    require(records.size() == 2, "corrupted segment test should keep only pre-corruption archive record and active record");
    require(records[0].payload == "archive-before-corruption", "corrupted segment test should keep archive records before corruption");
    require(records[1].payload == "active-after-corruption", "corrupted segment test should continue with later segments after corruption");
    const auto stats = log_engine::get_reader_stats();
    require(stats.corrupted_segments == 1, "corrupted segment test should count one corrupted segment");
    require(stats.corrupted_lines == 1, "corrupted segment test should count one corrupted line");
    require(stats.gzip_read_errors == 0, "corrupted segment test should not count gzip read errors");
    co_return;
}

seastar::future<> test_reader_skips_broken_gzip_archive(const std::string& root_dir) {
    log_engine::reset_reader_stats();
    const auto log_dir = (fs::path(root_dir) / "broken-gzip-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "broken-gzip-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.record_sequence_enabled = false;

    const auto broken_gzip_path = log_engine::layout::archive_log_path(config, 0, 3344556677, 1, true);
    const auto active_path = log_engine::layout::active_log_path(config, 0);

    write_gzip_file(
        broken_gzip_path,
        log_engine::encode_record(
            config,
            0,
            0,
            log_engine::LogLevel::info,
            "",
            "archive-before-broken-gzip"));
    require(fs::file_size(broken_gzip_path) > 8, "broken gzip test requires a non-trivial gzip file");
    fs::resize_file(broken_gzip_path, fs::file_size(broken_gzip_path) - 8);
    {
        std::ofstream out(active_path, std::ios::binary | std::ios::trunc);
        out << log_engine::encode_record(
            config,
            0,
            0,
            log_engine::LogLevel::info,
            "",
            "active-after-broken-gzip");
    }

    const auto records = read_back_records(config, true, 10);
    require(records.size() == 2, "broken gzip test should keep archive prefix record and active record");
    require(records[0].payload == "archive-before-broken-gzip", "broken gzip test should preserve readable archive prefix");
    require(records[1].payload == "active-after-broken-gzip", "broken gzip test should preserve active record");
    const auto stats = log_engine::get_reader_stats();
    require(stats.corrupted_segments == 1, "broken gzip test should count one corrupted segment");
    require(stats.corrupted_lines == 0, "broken gzip test should not count parse-corrupted plain lines");
    require(stats.gzip_read_errors == 1, "broken gzip test should count one gzip read error");
    co_return;
}

seastar::future<> test_reader_skips_segment_removed_after_enumeration(const std::string& root_dir) {
    log_engine::reset_reader_stats();
    const auto log_dir = (fs::path(root_dir) / "removed-segment-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "removed-segment-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;

    const auto archive_path = log_engine::layout::archive_log_path(config, 0, 4455667788, 1, false);
    const auto active_path = log_engine::layout::active_log_path(config, 0);

    {
        std::ofstream out(archive_path, std::ios::binary | std::ios::trunc);
        out << log_engine::encode_record(config, 0, 0, log_engine::LogLevel::info, "", "archive-to-be-removed");
    }
    {
        std::ofstream out(active_path, std::ios::binary | std::ios::trunc);
        out << log_engine::encode_record(config, 0, 1, log_engine::LogLevel::info, "", "active-still-readable");
    }

    log_engine::ReadQuery query;
    query.include_archive = true;
    query.limit = 10;
    auto segments = log_engine::collect_segments(config, query);
    require(segments.size() == 2, "removed segment test should enumerate archive and active segments");

    fs::remove(archive_path);

    const auto records = log_engine::read_records(segments, query);
    require(records.size() == 1, "removed segment test should keep only one active record");
    require(records.front().payload == "active-still-readable", "removed segment test should preserve active record");
    const auto stats = log_engine::get_reader_stats();
    require(stats.corrupted_segments == 0, "removed segment test should not count missing files as corrupted segments");
    require(stats.corrupted_lines == 0, "removed segment test should not count missing files as corrupted lines");
    require(stats.gzip_read_errors == 0, "removed segment test should not count missing files as gzip errors");
    co_return;
}

seastar::future<> test_recovery_scan(const std::string& root_dir) {
    const auto log_dir = (fs::path(root_dir) / "recovery-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "recovery-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 2;
    config.checkpoint_enabled = true;
    config.truncate_on_start = false;
    config.record_sequence_enabled = false;

    log_engine::LogEngine engine;
    co_await engine.start(config);
    co_await engine.info("recovery-a", "route-r");
    co_await engine.info("recovery-b", "route-r");
    co_await engine.stop();

    const auto shard_path = find_non_empty_shard_log(log_dir);
    require(shard_path.has_value(), "recovery test should find a non-empty shard log");

    {
        std::ofstream out(*shard_path, std::ios::app | std::ios::binary);
        out << "BROKEN_TAIL";
    }

    log_engine::LogManager manager;
    const auto active_segment = log_engine::layout::describe_path(config, shard_path->string());
    require(active_segment.has_value(), "recovery test should describe active shard log");
    const auto recovery = co_await manager.recover_active_file(*active_segment, 4096);
    require(recovery.sequence == 2, "recovery should preserve next sequence");
    require(recovery.logical_size > 0, "recovery should preserve valid_size");
    require(recovery.logical_size < static_cast<std::uint64_t>(fs::file_size(*shard_path)), "recovery should cut broken tail");
    co_return;
}

seastar::future<> test_partial_checkpoint_ignored(const std::string& root_dir) {
    const auto log_dir = (fs::path(root_dir) / "partial-checkpoint-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "partial-checkpoint-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 2;
    config.checkpoint_enabled = true;
    config.truncate_on_start = false;
    config.record_sequence_enabled = false;

    log_engine::LogEngine engine;
    co_await engine.start(config);
    co_await engine.info("partial-checkpoint-a", "route-pc");
    co_await engine.info("partial-checkpoint-b", "route-pc");
    co_await engine.stop();

    const auto shard_path = find_non_empty_shard_log(log_dir);
    require(shard_path.has_value(), "partial checkpoint test should find a non-empty shard log");

    {
        std::ofstream out(log_engine::layout::checkpoint_path(shard_path->string()), std::ios::binary | std::ios::trunc);
        out << "sequence=999999\n";
    }

    log_engine::LogManager manager;
    const auto active_segment = log_engine::layout::describe_path(config, shard_path->string());
    require(active_segment.has_value(), "partial checkpoint test should describe active shard log");
    const auto recovery = co_await manager.recover_active_file(*active_segment, 4096);
    require(recovery.logical_size > 0, "partial checkpoint should not zero out valid recovery size");
    require(recovery.sequence == 2, "partial checkpoint should fall back to verified sequence");
    co_return;
}

seastar::future<> test_checkpoint_tail_scan_preserves_valid_records_after_checkpoint(const std::string& root_dir) {
    const auto log_dir = (fs::path(root_dir) / "tail-scan-checkpoint-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "tail-scan-checkpoint-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 2;
    config.checkpoint_enabled = true;
    config.truncate_on_start = false;
    config.record_sequence_enabled = false;

    log_engine::LogEngine engine;
    co_await engine.start(config);
    co_await engine.info("tail-scan-checkpoint-a", "route-sc");
    co_await engine.info("tail-scan-checkpoint-b", "route-sc");
    co_await engine.stop();

    const auto shard_path = find_non_empty_shard_log(log_dir);
    require(shard_path.has_value(), "tail scan checkpoint test should find a non-empty shard log");

    const auto content = read_file(*shard_path);
    const auto verified = log_engine::scan_log_content(content);
    require(verified.valid_size > 0, "tail scan checkpoint test should keep valid log content");
    const auto first_record_end = content.find('\n');
    require(first_record_end != std::string::npos, "tail scan checkpoint test should find first record boundary");
    const auto checkpoint_size = static_cast<std::uint64_t>(first_record_end + 1);
    require(checkpoint_size < verified.valid_size, "tail scan checkpoint test should create a smaller logical_size");

    {
        std::ofstream out(*shard_path, std::ios::app | std::ios::binary);
        out << "BROKEN_TAIL";
    }

    log_engine::LogManager writer;
    const auto active_segment = log_engine::layout::describe_path(config, shard_path->string());
    require(active_segment.has_value(), "tail scan checkpoint test should describe active shard log");
    co_await writer.store_checkpoint(
        *active_segment,
        log_engine::CheckpointState{
            .logical_size = checkpoint_size,
            .sequence = 1,
            .rotation_index = 0,
        });

    log_engine::LogManager manager;
    const auto recovery = co_await manager.recover_active_file(*active_segment, 4096);
    require(recovery.logical_size == verified.valid_size, "tail scan checkpoint recovery should preserve valid records after checkpoint");
    require(recovery.sequence == verified.next_sequence, "tail scan checkpoint recovery should advance sequence through valid tail records");
    co_return;
}

seastar::future<> test_corrupted_checkpoint_falls_back_to_scan(const std::string& root_dir) {
    const auto log_dir = (fs::path(root_dir) / "corrupted-checkpoint-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "corrupted-checkpoint-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 2;
    config.checkpoint_enabled = true;
    config.truncate_on_start = false;
    config.record_sequence_enabled = false;

    log_engine::LogEngine engine;
    co_await engine.start(config);
    co_await engine.info("corrupted-checkpoint-a", "route-cc");
    co_await engine.info("corrupted-checkpoint-b", "route-cc");
    co_await engine.stop();

    const auto shard_path = find_non_empty_shard_log(log_dir);
    require(shard_path.has_value(), "corrupted checkpoint test should find a non-empty shard log");
    const auto verified = log_engine::scan_log_content(read_file(*shard_path));

    {
        std::ofstream out(log_engine::layout::checkpoint_path(shard_path->string()), std::ios::binary | std::ios::trunc);
        out << "format_version=2\n";
        out << "logical_size=1\n";
        out << "sequence=999\n";
        out << "rotation_index=0\n";
        out << "checkpoint_crc=0\n";
    }

    log_engine::LogManager manager;
    const auto active_segment = log_engine::layout::describe_path(config, shard_path->string());
    require(active_segment.has_value(), "corrupted checkpoint test should describe active shard log");
    const auto recovery = co_await manager.recover_active_file(*active_segment, 4096);
    require(recovery.logical_size == verified.valid_size, "corrupted checkpoint should fall back to scanned size");
    require(recovery.sequence == verified.next_sequence, "corrupted checkpoint should fall back to scanned sequence");
    co_return;
}

seastar::future<> test_recovery_after_rotate(
    const std::string& root_dir,
    bool checkpoint_enabled,
    bool compress_archives) {
    const auto suffix = std::string(checkpoint_enabled ? "cp1" : "cp0") + "-" + (compress_archives ? "gz1" : "gz0");
    const auto log_dir = (fs::path(root_dir) / ("rotate-recovery-logs-" + suffix)).string();
    const auto archive_dir = (fs::path(root_dir) / ("rotate-recovery-archive-" + suffix)).string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 1;
    config.flush_interval_ms = 1;
    config.rotate_size_bytes = 192;
    config.truncate_on_start = false;
    config.checkpoint_enabled = checkpoint_enabled;
    config.compress_archives = compress_archives;
    config.max_archived_files_per_shard = 8;

    const std::string rotated_payload(256, 'r');
    const std::string active_payload = "active-before-restart-" + suffix;
    const std::string recovered_payload = "after-restart-" + suffix;

    log_engine::LogEngine engine;
    co_await engine.start(config);
    co_await engine.info(rotated_payload, "route-rotate-recovery");
    co_await engine.info(active_payload, "route-rotate-recovery");
    co_await engine.stop();

    std::size_t archive_count = 0;
    for (const auto& entry : fs::directory_iterator(archive_dir)) {
        if (compress_archives) {
            archive_count += entry.path().extension() == ".gz";
        } else {
            archive_count += entry.path().extension() == ".log";
        }
    }
    require(archive_count >= 1, "rotate recovery test should create at least one archive");

    if (checkpoint_enabled) {
        const auto shard_path = find_non_empty_shard_log(log_dir);
        require(shard_path.has_value(), "checkpoint rotate recovery test should keep one active shard log");
        require(fs::exists(log_engine::layout::checkpoint_path(shard_path->string())), "checkpoint-enabled run should persist checkpoint");
    }

    const auto shard_path = find_non_empty_shard_log(log_dir);
    require(shard_path.has_value(), "rotate recovery test should keep one non-empty active shard log");
    {
        std::ofstream out(*shard_path, std::ios::app | std::ios::binary);
        out << "BROKEN_TAIL_AFTER_ROTATE";
    }

    log_engine::LogEngine restarted;
    co_await restarted.start(config);
    co_await restarted.info(recovered_payload, "route-rotate-recovery");
    co_await restarted.stop();

    const auto records = read_back_records(config, true, 20);
    bool found_rotated = false;
    bool found_active = false;
    bool found_recovered = false;
    for (const auto& record : records) {
        found_rotated = found_rotated || record.payload == rotated_payload;
        found_active = found_active || record.payload == active_payload;
        found_recovered = found_recovered || record.payload == recovered_payload;
    }
    require(found_rotated, "rotate recovery test should preserve rotated archive record");
    require(found_active, "rotate recovery test should preserve active record after broken-tail recovery");
    require(found_recovered, "rotate recovery test should append new record after restart");
    co_return;
}

seastar::future<> test_prepare_cleans_temporary_sidecars(const std::string& root_dir) {
    const auto log_dir = (fs::path(root_dir) / "tmp-sidecar-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "tmp-sidecar-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.rotate_size_bytes = 128;
    config.compress_archives = true;

    const auto checkpoint_tmp = fs::path(log_dir) / "shard-0.log.checkpoint.tmp";
    const auto gzip_tmp = fs::path(archive_dir) / "shard-0.123.1.log.gz.tmp";
    {
        std::ofstream out(checkpoint_tmp, std::ios::binary | std::ios::trunc);
        out << "partial checkpoint";
    }
    {
        std::ofstream out(gzip_tmp, std::ios::binary | std::ios::trunc);
        out << "partial gzip";
    }

    log_engine::LogManager manager;
    co_await manager.prepare(config);

    require(!fs::exists(checkpoint_tmp), "prepare should remove checkpoint.tmp leftovers");
    require(!fs::exists(gzip_tmp), "prepare should remove gz.tmp leftovers");
    co_return;
}

}  // namespace

seastar::future<> test_crc_class_roundtrip();
seastar::future<> test_crash_during_write_recovery(const std::string& root_dir);
seastar::future<> test_checkpoint_clean_shutdown_restore(const std::string& root_dir);
seastar::future<> test_agent_offset_roundtrip(const std::string& root_dir);
seastar::future<> test_agent_tail_file_complete_lines_and_resume(const std::string& root_dir);
seastar::future<> test_agent_tail_file_truncate_resets_offset(const std::string& root_dir);
seastar::future<> test_agent_disk_quota_and_http_helpers(const std::string& root_dir);
seastar::future<> test_agent_per_shard_delivery_offsets(const std::string& root_dir);
seastar::future<> test_agent_pending_delivery_batch_roundtrip(const std::string& root_dir);
seastar::future<> test_agent_glob_and_dynamic_backpressure(const std::string& root_dir);
seastar::future<> test_agent_http_sink_fake_server();
seastar::future<> test_agent_file_to_http_sink_delivery_flow(const std::string& root_dir);

int main(int argc, char** argv) {
    seastar::app_template app;
    namespace bpo = boost::program_options;
    app.add_options()
        ("root-dir", bpo::value<std::string>()->default_value("/root/workspace/seastar-log-engine/test-tmp"), "Temporary root directory for unit tests");

    return app.run(argc, argv, [&app] () -> seastar::future<> {
        const auto root_dir = app.configuration()["root-dir"].as<std::string>();
        co_await test_record_codec();
        co_await test_config_loader(root_dir);
        co_await test_config_validation();
        co_await test_health_counter_window_eviction();
        co_await test_consistent_hash_routing();
        co_await test_compat_logging(root_dir);
        co_await test_compat_unbound_drops_messages(root_dir);
        co_await test_unified_large_payload_blocks(root_dir);
        co_await test_append_batch_routes_and_persists(root_dir);
        co_await test_time_rotation_and_archive_read(root_dir);
        co_await test_archive_duplicate_prefers_plain_log(root_dir);
        co_await test_reader_stops_after_corrupted_segment_line(root_dir);
        co_await test_reader_skips_broken_gzip_archive(root_dir);
        co_await test_reader_skips_segment_removed_after_enumeration(root_dir);
        co_await test_recovery_scan(root_dir);
        co_await test_recovery_scan_large_file_streaming(root_dir);
        co_await test_partial_checkpoint_ignored(root_dir);
        co_await test_checkpoint_tail_scan_preserves_valid_records_after_checkpoint(root_dir);
        co_await test_corrupted_checkpoint_falls_back_to_scan(root_dir);
        co_await test_recovery_after_rotate(root_dir, false, false);
        co_await test_recovery_after_rotate(root_dir, true, false);
        co_await test_recovery_after_rotate(root_dir, false, true);
        co_await test_recovery_after_rotate(root_dir, true, true);
        co_await test_prepare_cleans_temporary_sidecars(root_dir);
        co_await test_crc_class_roundtrip();
        co_await test_limit_zero_returns_no_records(root_dir);
        co_await test_query_records_proto_defaults();
        co_await test_crash_during_write_recovery(root_dir);
        co_await test_checkpoint_clean_shutdown_restore(root_dir);
        co_await test_agent_offset_roundtrip(root_dir);
        co_await test_agent_tail_file_complete_lines_and_resume(root_dir);
        co_await test_agent_tail_file_truncate_resets_offset(root_dir);
        co_await test_agent_disk_quota_and_http_helpers(root_dir);
        co_await test_agent_per_shard_delivery_offsets(root_dir);
        co_await test_agent_pending_delivery_batch_roundtrip(root_dir);
        co_await test_agent_glob_and_dynamic_backpressure(root_dir);
        co_await test_agent_http_sink_fake_server();
        co_await test_agent_file_to_http_sink_delivery_flow(root_dir);
        co_return;
    });
}
seastar::future<> test_crc_class_roundtrip() {
    using log_engine::CrcClass;

    const std::string payload = "test-payload-with-\n-newline";
    const std::string timestamp = "2026-05-02 12:00:00.123456";

    // Test CRC class=none: no crc= prefix, payload preserved
    {
        log_engine::EngineConfig config;
        config.record_crc_enabled = true;
        config.record_crc_class = CrcClass::none;
        config.record_timestamp_enabled = true;
        config.record_sequence_enabled = true;
        config.record_shard_id_enabled = true;
        config.record_level_enabled = true;

        auto buffer = log_engine::encode_record_buffer(config, 0, 42, log_engine::LogLevel::info, timestamp, payload);
        std::string_view line(buffer.get(), buffer.size() - 1);
        require(line.rfind("crc=", 0) != 0, "CRC class none should not emit crc prefix");
        require(line.rfind("crc=h:", 0) != 0, "CRC class none should not emit crc=h: prefix");
        const auto parsed = log_engine::parse_record_line(line);
        require(parsed.has_value(), "CRC class none record should be parseable");
        require(parsed->payload == "test-payload-with- -newline", "CRC class none should preserve sanitized payload");
        require(parsed->has_sequence, "CRC class none should parse sequence");
        require(parsed->sequence == 42, "CRC class none should preserve sequence value");
        require(parsed->timestamp == timestamp, "CRC class none should preserve timestamp");
        require(parsed->level == log_engine::LogLevel::info, "CRC class none should preserve level");
        require(log_engine::verify_record_line(line), "CRC class none should pass verification");
    }

    // Test CRC class=header: crc=h: prefix, CRC covers metadata only
    {
        log_engine::EngineConfig config;
        config.record_crc_enabled = true;
        config.record_crc_class = CrcClass::header;
        config.record_timestamp_enabled = true;
        config.record_sequence_enabled = true;

        auto buffer = log_engine::encode_record_buffer(config, 0, 1, log_engine::LogLevel::warn, timestamp, payload);
        std::string_view line(buffer.get(), buffer.size() - 1);
        require(line.rfind("crc=h:", 0) == 0, "CRC class header should emit crc=h: prefix");
        require(log_engine::verify_record_line(line), "CRC class header should pass verification");
        const auto parsed = log_engine::parse_record_line(line);
        require(parsed.has_value(), "CRC class header record should be parseable");
        require(parsed->crc != 0, "CRC class header should have non-zero parsed crc");
    }

    // Test CRC class=full: crc= prefix, CRC covers entire body
    {
        log_engine::EngineConfig config;
        config.record_crc_enabled = true;
        config.record_crc_class = CrcClass::full;
        config.record_timestamp_enabled = true;

        auto buffer = log_engine::encode_record_buffer(config, 0, 2, log_engine::LogLevel::error, timestamp, "simple");
        std::string_view line(buffer.get(), buffer.size() - 1);
        require(line.rfind("crc=", 0) == 0, "CRC class full should emit crc= prefix");
        require(line.rfind("crc=h:", 0) != 0, "CRC class full should not emit crc=h: prefix");
        require(log_engine::verify_record_line(line), "CRC class full should pass verification");
        const auto parsed = log_engine::parse_record_line(line);
        require(parsed.has_value(), "CRC class full record should be parseable");
    }

    // Test CRC class=payload_hash: payload hash plus metadata CRC
    {
        log_engine::EngineConfig config;
        config.record_crc_enabled = true;
        config.record_crc_class = CrcClass::payload_hash;
        config.record_timestamp_enabled = true;
        config.record_sequence_enabled = true;

        auto buffer = log_engine::encode_record_buffer(config, 0, 5, log_engine::LogLevel::info, timestamp, payload);
        std::string_view line(buffer.get(), buffer.size() - 1);
        require(line.rfind("crc=x:", 0) == 0, "CRC class payload_hash should emit crc=x: prefix");
        require(log_engine::verify_record_line(line), "CRC class payload_hash should pass verification");
        const auto parsed = log_engine::parse_record_line(line);
        require(parsed.has_value(), "CRC class payload_hash record should be parseable");
    }

    // Test that header-only CRC detects payload corruption (false negative check)
    {
        log_engine::EngineConfig config;
        config.record_crc_enabled = true;
        config.record_crc_class = CrcClass::header;
        config.record_timestamp_enabled = true;

        auto buffer = log_engine::encode_record_buffer(config, 0, 3, log_engine::LogLevel::info, timestamp, payload);
        std::string line(buffer.get(), buffer.size());

        // Corrupt the payload portion (after "payload=")
        auto payload_pos = line.find("payload=");
        require(payload_pos != std::string::npos, "should find payload field");
        line[payload_pos + 10] = 'Z';  // Corrupt a byte in the payload

        std::string_view corrupted(line.data(), line.size() - 1);
        // Header CRC should still pass since payload is not covered
        require(log_engine::verify_record_line(corrupted), "CRC class header should tolerate payload corruption");
    }

    // Test that full CRC detects payload corruption
    {
        log_engine::EngineConfig config;
        config.record_crc_enabled = true;
        config.record_crc_class = CrcClass::full;
        config.record_timestamp_enabled = true;

        auto buffer = log_engine::encode_record_buffer(config, 0, 4, log_engine::LogLevel::info, timestamp, payload);
        std::string line(buffer.get(), buffer.size());

        auto payload_pos = line.find("payload=");
        require(payload_pos != std::string::npos, "should find payload field");
        line[payload_pos + 10] = 'Y';

        std::string_view corrupted(line.data(), line.size() - 1);
        require(!log_engine::verify_record_line(corrupted), "CRC class full should detect payload corruption");
    }

    // Test that payload-hash CRC detects payload corruption
    {
        log_engine::EngineConfig config;
        config.record_crc_enabled = true;
        config.record_crc_class = CrcClass::payload_hash;
        config.record_timestamp_enabled = true;

        auto buffer = log_engine::encode_record_buffer(config, 0, 6, log_engine::LogLevel::info, timestamp, payload);
        std::string line(buffer.get(), buffer.size());

        auto payload_pos = line.find("payload=");
        require(payload_pos != std::string::npos, "should find payload field");
        line[payload_pos + 10] = 'Q';

        std::string_view corrupted(line.data(), line.size() - 1);
        require(!log_engine::verify_record_line(corrupted), "CRC class payload_hash should detect payload corruption");
    }

    co_return;
}

seastar::future<> test_crash_during_write_recovery(const std::string& root_dir) {
    namespace fs = std::filesystem;
    const auto log_dir = (fs::path(root_dir) / "crash-write-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "crash-write-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 4;
    config.checkpoint_enabled = true;
    config.truncate_on_start = false;

    // Write some records cleanly
    {
        log_engine::LogEngine engine;
        co_await engine.start(config);
        co_await engine.info("crash-test-1", "route-c");
        co_await engine.info("crash-test-2", "route-c");
        co_await engine.stop();
    }

    // Simulate a crash by appending garbage to the active log tail
    const auto shard_path = find_non_empty_shard_log(log_dir);
    require(shard_path.has_value(), "crash test should have active log");
    {
        std::ofstream out(*shard_path, std::ios::app | std::ios::binary);
        out << "INCOMPLETE_RECORD_WITHOUT_NEWLINE";
    }

    // Recover with truncate_on_start=false (scan mode)
    {
        log_engine::LogEngine engine;
        co_await engine.start(config);
        co_await engine.info("crash-test-after-1", "route-c");
        co_await engine.info("crash-test-after-2", "route-c");
        co_await engine.stop();
    }

    // Verify all valid records can be read back
    const auto records = read_back_records(config, true, 10);
    std::vector<std::string> payloads;
    for (const auto& r : records) {
        payloads.push_back(r.payload);
    }
    require(std::find(payloads.begin(), payloads.end(), "crash-test-1") != payloads.end(), "should find pre-crash record 1");
    require(std::find(payloads.begin(), payloads.end(), "crash-test-2") != payloads.end(), "should find pre-crash record 2");
    require(std::find(payloads.begin(), payloads.end(), "crash-test-after-1") != payloads.end(), "should find post-crash record 1");
    require(std::find(payloads.begin(), payloads.end(), "crash-test-after-2") != payloads.end(), "should find post-crash record 2");
    require(std::find(payloads.begin(), payloads.end(), "INCOMPLETE_RECORD_WITHOUT_NEWLINE") == payloads.end(), "should not include incomplete record");

    co_return;
}

seastar::future<> test_checkpoint_clean_shutdown_restore(const std::string& root_dir) {
    namespace fs = std::filesystem;
    const auto log_dir = (fs::path(root_dir) / "clean-checkpoint-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "clean-checkpoint-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 2;
    config.checkpoint_enabled = true;
    config.truncate_on_start = false;
    config.record_sequence_enabled = true;

    // First session: write records with sequence tracking
    {
        log_engine::LogEngine engine;
        co_await engine.start(config);
        for (int i = 0; i < 8; ++i) {
            co_await engine.info("checkpointed-record-" + std::to_string(i), "route-ck");
        }
        co_await engine.stop();
    }

    // Verify checkpoint file exists after clean shutdown
    const auto shard_path = find_non_empty_shard_log(log_dir);
    require(shard_path.has_value(), "checkpoint test should have active log");
    const auto ckpt_path = log_engine::layout::checkpoint_path(shard_path->string());
    require(fs::exists(ckpt_path), "checkpoint should exist after clean shutdown");

    // Second session: start without truncation, should use checkpoint
    {
        log_engine::LogEngine engine;
        co_await engine.start(config);
        co_await engine.info("checkpointed-record-8", "route-ck");
        co_await engine.stop();
    }

    // Verify all records are present with correct sequence continuity
    const auto records = read_back_records(config, true, 20);
    std::size_t ck_count = 0;
    for (const auto& r : records) {
        if (r.payload.rfind("checkpointed-record-", 0) == 0) {
            ++ck_count;
        }
    }
    require(ck_count == 9, "should recover all 8 + 1 checkpointed records");

    // Verify sequence continuity: sequence should be contiguous 0-8
    std::vector<std::uint64_t> seqs;
    for (const auto& r : records) {
        if (r.payload.rfind("checkpointed-record-", 0) == 0) {
            seqs.push_back(r.sequence);
        }
    }
    std::sort(seqs.begin(), seqs.end());
    for (std::size_t i = 0; i < seqs.size(); ++i) {
        require(seqs[i] == i, "checkpoint recovery should preserve contiguous sequence numbering");
    }

    co_return;
}

seastar::future<> test_agent_offset_roundtrip(const std::string& root_dir) {
    const auto dir = fs::path(root_dir) / "agent-offsets";
    reset_directory(dir);
    const auto source_path = (dir / "source.offset").string();
    const auto delivery_path = (dir / "delivery.offset").string();

    log_engine::agent::SourceOffset source;
    source.path = "/var/log/app.log";
    source.inode = 12345;
    source.offset = 67890;
    log_engine::agent::store_source_offset(source_path, source);
    const auto loaded_source = log_engine::agent::load_source_offset(source_path);
    require(loaded_source.has_value(), "agent source offset should load");
    require(loaded_source->path == source.path, "agent source offset path mismatch");
    require(loaded_source->inode == source.inode, "agent source offset inode mismatch");
    require(loaded_source->offset == source.offset, "agent source offset value mismatch");

    log_engine::agent::DeliveryOffset delivery;
    delivery.shard = 2;
    delivery.next_sequence = 99;
    log_engine::agent::store_delivery_offset(delivery_path, delivery);
    const auto loaded_delivery = log_engine::agent::load_delivery_offset(delivery_path);
    require(loaded_delivery.has_value(), "agent delivery offset should load");
    require(loaded_delivery->shard == delivery.shard, "agent delivery offset shard mismatch");
    require(loaded_delivery->next_sequence == delivery.next_sequence, "agent delivery offset sequence mismatch");
    co_return;
}

seastar::future<> test_agent_tail_file_complete_lines_and_resume(const std::string& root_dir) {
    const auto dir = fs::path(root_dir) / "agent-tail";
    reset_directory(dir);
    const auto source = (dir / "app.log").string();
    {
        std::ofstream out(source, std::ios::binary | std::ios::trunc);
        out << "first\nsecond\npartial";
    }

    const auto first = log_engine::agent::tail_file_once(source, std::nullopt, 10);
    require(first.lines.size() == 2, "agent tail should return complete lines only");
    require(first.lines[0] == "first", "agent tail first line mismatch");
    require(first.lines[1] == "second", "agent tail second line mismatch");

    {
        std::ofstream out(source, std::ios::binary | std::ios::app);
        out << "-done\nthird\n";
    }
    const auto second = log_engine::agent::tail_file_once(source, first.next_offset, 10);
    require(second.lines.size() == 2, "agent tail should resume from committed offset");
    require(second.lines[0] == "partial-done", "agent tail should keep incomplete trailing line for next read");
    require(second.lines[1] == "third", "agent tail should read appended complete line");
    require(second.next_offset.offset > first.next_offset.offset, "agent tail offset should advance");
    co_return;
}

seastar::future<> test_agent_tail_file_truncate_resets_offset(const std::string& root_dir) {
    const auto dir = fs::path(root_dir) / "agent-tail-truncate";
    reset_directory(dir);
    const auto source = (dir / "app.log").string();
    {
        std::ofstream out(source, std::ios::binary | std::ios::trunc);
        out << "before\n";
    }

    const auto first = log_engine::agent::tail_file_once(source, std::nullopt, 10);
    require(first.lines.size() == 1, "agent truncate setup should read first line");
    {
        std::ofstream out(source, std::ios::binary | std::ios::trunc);
        out << "after\n";
    }
    const auto second = log_engine::agent::tail_file_once(source, first.next_offset, 10);
    require(second.file_rotated_or_truncated, "agent tail should detect truncate");
    require(second.lines.size() == 1, "agent tail should reread from beginning after truncate");
    require(second.lines[0] == "after", "agent tail truncate line mismatch");
    co_return;
}

seastar::future<> test_agent_disk_quota_and_http_helpers(const std::string& root_dir) {
    const auto dir = fs::path(root_dir) / "agent-quota";
    reset_directory(dir);
    {
        std::ofstream out(dir / "buffer.log", std::ios::binary | std::ios::trunc);
        out << std::string(20, 'x');
    }

    log_engine::agent::DiskQuota quota;
    quota.max_buffer_bytes = 10;
    quota.resume_buffer_bytes = 5;
    require(log_engine::agent::directory_size_bytes(dir.string()) >= 20, "agent quota should sum directory size");
    require(log_engine::agent::disk_quota_exceeded(dir.string(), quota), "agent quota should detect exceeded high watermark");
    require(!log_engine::agent::disk_quota_can_resume(dir.string(), quota), "agent quota should not resume above low watermark");

    const auto endpoint = log_engine::agent::parse_http_endpoint("http://localhost:18081/v1/logs");
    require(endpoint.has_value(), "agent HTTP endpoint should parse");
    require(endpoint->host == "localhost", "agent HTTP endpoint host mismatch");
    require(endpoint->port == 18081, "agent HTTP endpoint port mismatch");
    require(endpoint->path == "/v1/logs", "agent HTTP endpoint path mismatch");
    require(!log_engine::agent::parse_http_endpoint("https://localhost/v1/logs"), "agent HTTP endpoint should reject https MVP URL");

    const auto body = log_engine::agent::render_json_batch({"alpha", "quote\"line", "new\nline"});
    require(body.find("\"records\"") != std::string::npos, "agent JSON batch should contain records key");
    require(body.find("quote\\\"line") != std::string::npos, "agent JSON batch should escape quotes");
    require(body.find("new\\nline") != std::string::npos, "agent JSON batch should escape newlines");
    co_return;
}

seastar::future<> test_agent_per_shard_delivery_offsets(const std::string& root_dir) {
    const auto dir = fs::path(root_dir) / "agent-delivery-offsets";
    reset_directory(dir);
    const auto path = (dir / "delivery.offset").string();

    std::vector<log_engine::agent::DeliveryOffset> offsets = {
        {.shard = 2, .next_sequence = 42},
        {.shard = 0, .next_sequence = 7},
        {.shard = 1, .next_sequence = 19},
    };
    log_engine::agent::store_delivery_offsets(path, offsets);
    const auto loaded = log_engine::agent::load_delivery_offsets(path);
    require(loaded.size() == 3, "agent should load per-shard delivery offsets");
    require(loaded[0].shard == 0 && loaded[0].next_sequence == 7, "agent shard 0 delivery offset mismatch");
    require(loaded[1].shard == 1 && loaded[1].next_sequence == 19, "agent shard 1 delivery offset mismatch");
    require(loaded[2].shard == 2 && loaded[2].next_sequence == 42, "agent shard 2 delivery offset mismatch");

    const auto legacy_path = (dir / "legacy.offset").string();
    {
        std::ofstream out(legacy_path, std::ios::binary | std::ios::trunc);
        out << "shard=3\nnext_sequence=88\n";
    }
    const auto legacy = log_engine::agent::load_delivery_offsets(legacy_path);
    require(legacy.size() == 1, "agent should keep legacy delivery offset compatibility");
    require(legacy[0].shard == 3 && legacy[0].next_sequence == 88, "agent legacy delivery offset mismatch");
    co_return;
}

seastar::future<> test_agent_pending_delivery_batch_roundtrip(const std::string& root_dir) {
    const auto dir = fs::path(root_dir) / "agent-pending-delivery";
    reset_directory(dir);
    const auto path = (dir / "pending.batch").string();

    log_engine::agent::DeliveryBatch batch;
    batch.shard = 4;
    batch.first_sequence = 10;
    batch.next_sequence = 13;
    batch.records = {"plain", "line\nbreak", "quote\"slash\\"};
    log_engine::agent::store_pending_delivery_batch(path, batch);

    const auto loaded = log_engine::agent::load_pending_delivery_batch(path);
    require(loaded.has_value(), "agent pending delivery batch should load");
    require(loaded->shard == batch.shard, "agent pending delivery shard mismatch");
    require(loaded->first_sequence == batch.first_sequence, "agent pending delivery first sequence mismatch");
    require(loaded->next_sequence == batch.next_sequence, "agent pending delivery next sequence mismatch");
    require(loaded->records == batch.records, "agent pending delivery records mismatch");

    const auto body = log_engine::agent::render_delivery_batch_json("agent-A", batch);
    require(body.find("\"agent_id\":\"agent-A\"") != std::string::npos, "agent delivery JSON should include agent id");
    require(body.find("\"shard\":4") != std::string::npos, "agent delivery JSON should include shard");
    require(body.find("\"first_sequence\":10") != std::string::npos, "agent delivery JSON should include first sequence");
    require(body.find("\"next_sequence\":13") != std::string::npos, "agent delivery JSON should include next sequence");
    require(body.find("\"sequence\":11") != std::string::npos, "agent delivery JSON should include per-record sequence");
    require(body.find("line\\nbreak") != std::string::npos, "agent delivery JSON should escape record payload");

    log_engine::agent::remove_pending_delivery_batch(path);
    require(!log_engine::agent::load_pending_delivery_batch(path).has_value(), "agent pending delivery batch should be removable");
    co_return;
}

seastar::future<> test_agent_glob_and_dynamic_backpressure(const std::string& root_dir) {
    const auto dir = fs::path(root_dir) / "agent-glob";
    reset_directory(dir);
    {
        std::ofstream out(dir / "b.log", std::ios::binary | std::ios::trunc);
        out << "b\n";
    }
    {
        std::ofstream out(dir / "a.log", std::ios::binary | std::ios::trunc);
        out << "a\n";
    }
    {
        std::ofstream out(dir / "ignored.txt", std::ios::binary | std::ios::trunc);
        out << "ignored\n";
    }

    const auto paths = log_engine::agent::expand_glob_paths((dir / "*.log").string());
    require(paths.size() == 2, "agent glob should find matching log files");
    require(fs::path(paths[0]).filename() == "a.log", "agent glob should return sorted paths");
    require(fs::path(paths[1]).filename() == "b.log", "agent glob should return second sorted path");

    log_engine::agent::BackpressureState backlog;
    backlog.sink_backlog_records = 10;
    backlog.max_sink_backlog_records = 10;
    const auto backlog_decision = log_engine::agent::evaluate_backpressure(dir.string(), {}, backlog);
    require(backlog_decision.pause && backlog_decision.reason == "sink_backlog", "agent backpressure should pause on sink backlog");

    log_engine::agent::BackpressureState failures;
    failures.recent_sink_failures = 3;
    failures.max_recent_sink_failures = 3;
    const auto failure_decision = log_engine::agent::evaluate_backpressure(dir.string(), {}, failures);
    require(failure_decision.pause && failure_decision.reason == "sink_failures", "agent backpressure should pause on sink failures");

    log_engine::agent::BackpressureState latency;
    latency.last_sink_latency_ms = 5000;
    latency.max_sink_latency_ms = 1000;
    const auto latency_decision = log_engine::agent::evaluate_backpressure(dir.string(), {}, latency);
    require(latency_decision.pause && latency_decision.reason == "sink_latency", "agent backpressure should pause on sink latency");
    co_return;
}

seastar::future<> test_agent_http_sink_fake_server() {
    FakeHttpSink ok_sink;
    run_fake_http_sink_once(ok_sink, 19081);
    while (!ok_sink.ready.load(std::memory_order_acquire)) {
        co_await seastar::sleep(std::chrono::milliseconds(5));
    }
    const auto endpoint = log_engine::agent::parse_http_endpoint("http://127.0.0.1:19081/sink");
    require(endpoint.has_value(), "agent async HTTP sink test should parse endpoint");
    log_engine::agent::DeliveryBatch ok_batch;
    ok_batch.shard = 1;
    ok_batch.first_sequence = 20;
    ok_batch.next_sequence = 22;
    ok_batch.records = {"http-a", "http-b"};
    co_await log_engine::agent::post_http_batch_async(*endpoint, log_engine::agent::render_delivery_batch_json("agent-test", ok_batch));
    ok_sink.worker.join();
    require(ok_sink.received_body.find("\"agent_id\":\"agent-test\"") != std::string::npos, "agent async HTTP sink should include agent id");
    require(ok_sink.received_body.find("\"shard\":1") != std::string::npos, "agent async HTTP sink should include shard");
    require(ok_sink.received_body.find("\"first_sequence\":20") != std::string::npos, "agent async HTTP sink should include first sequence");
    require(ok_sink.received_body.find("http-a") != std::string::npos, "agent async HTTP sink should deliver first record");
    require(ok_sink.received_body.find("http-b") != std::string::npos, "agent async HTTP sink should deliver second record");

    FakeHttpSink fail_sink;
    fail_sink.status = 503;
    run_fake_http_sink_once(fail_sink, 19082);
    while (!fail_sink.ready.load(std::memory_order_acquire)) {
        co_await seastar::sleep(std::chrono::milliseconds(5));
    }
    const auto fail_endpoint = log_engine::agent::parse_http_endpoint("http://127.0.0.1:19082/sink");
    require(fail_endpoint.has_value(), "agent async HTTP sink failure test should parse endpoint");
    bool threw = false;
    try {
        log_engine::agent::DeliveryBatch fail_batch;
        fail_batch.shard = 0;
        fail_batch.first_sequence = 0;
        fail_batch.next_sequence = 1;
        fail_batch.records = {"http-fail"};
        co_await log_engine::agent::post_http_batch_async(*fail_endpoint, log_engine::agent::render_delivery_batch_json("agent-test", fail_batch));
    } catch (const std::exception&) {
        threw = true;
    }
    fail_sink.worker.join();
    require(threw, "agent async HTTP sink should reject non-2xx ACK");
    co_return;
}

seastar::future<> test_agent_file_to_http_sink_delivery_flow(const std::string& root_dir) {
    const auto dir = fs::path(root_dir) / "agent-flow";
    const auto log_dir = (dir / "logs").string();
    const auto archive_dir = (dir / "archive").string();
    reset_directory(dir);
    reset_directory(log_dir);
    reset_directory(archive_dir);

    const auto source_path = (dir / "source.log").string();
    {
        std::ofstream out(source_path, std::ios::binary | std::ios::trunc);
        out << "flow-a\nflow-b\npartial";
    }

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 2;
    config.ack_mode = log_engine::AckMode::sync_ack;
    config.empty_route_policy = log_engine::EmptyRoutePolicy::local;
    config.truncate_on_start = true;
    config.record_crc_enabled = true;
    config.record_sequence_enabled = true;
    config.record_shard_id_enabled = true;

    log_engine::LogEngine engine;
    co_await engine.start(config);

    const auto tailed = log_engine::agent::tail_file_once(source_path, std::nullopt, 10);
    require(tailed.lines.size() == 2, "agent flow should tail only complete source lines");
    std::vector<log_engine::LogMessage> messages;
    for (const auto& line : tailed.lines) {
        messages.push_back(log_engine::LogMessage{.payload = line});
    }
    co_await engine.append_batch(std::move(messages));
    co_await engine.stop();

    const auto records = read_back_records(config, true, 10);
    require(records.size() == 2, "agent flow should persist tailed records");
    require(records[0].payload == "flow-a", "agent flow first persisted payload mismatch");
    require(records[1].payload == "flow-b", "agent flow second persisted payload mismatch");
    require(records[0].has_sequence && records[1].has_sequence, "agent flow records should carry sequences");

    FakeHttpSink sink;
    run_fake_http_sink_once(sink, 19083);
    while (!sink.ready.load(std::memory_order_acquire)) {
        co_await seastar::sleep(std::chrono::milliseconds(5));
    }
    const auto endpoint = log_engine::agent::parse_http_endpoint("http://127.0.0.1:19083/sink");
    require(endpoint.has_value(), "agent flow should parse HTTP sink endpoint");

    std::vector<std::string> payloads;
    payloads.reserve(records.size());
    std::uint64_t next_sequence = 0;
    for (const auto& record : records) {
        payloads.push_back(record.payload);
        next_sequence = std::max(next_sequence, record.sequence + 1);
    }
    log_engine::agent::DeliveryBatch delivery_batch;
    delivery_batch.shard = 0;
    delivery_batch.first_sequence = records.front().sequence;
    delivery_batch.next_sequence = next_sequence;
    delivery_batch.records = payloads;
    const auto pending_path = (dir / "delivery.pending").string();
    log_engine::agent::store_pending_delivery_batch(pending_path, delivery_batch);
    co_await log_engine::agent::post_http_batch_async(*endpoint, log_engine::agent::render_delivery_batch_json("agent-flow", delivery_batch));
    sink.worker.join();
    require(sink.received_body.find("\"agent_id\":\"agent-flow\"") != std::string::npos, "agent flow sink should receive agent id");
    require(sink.received_body.find("\"first_sequence\":0") != std::string::npos, "agent flow sink should receive first sequence");
    require(sink.received_body.find("\"next_sequence\":2") != std::string::npos, "agent flow sink should receive next sequence");
    require(sink.received_body.find("flow-a") != std::string::npos, "agent flow sink should receive first payload");
    require(sink.received_body.find("flow-b") != std::string::npos, "agent flow sink should receive second payload");

    const auto delivery_path = (dir / "delivery.offset").string();
    log_engine::agent::store_delivery_offsets(delivery_path, {
        log_engine::agent::DeliveryOffset{.shard = 0, .next_sequence = next_sequence},
    });
    log_engine::agent::remove_pending_delivery_batch(pending_path);
    require(!log_engine::agent::load_pending_delivery_batch(pending_path).has_value(), "agent flow should remove pending delivery after ACK");
    const auto offsets = log_engine::agent::load_delivery_offsets(delivery_path);
    require(offsets.size() == 1, "agent flow should persist delivery offset");
    require(offsets[0].shard == 0, "agent flow delivery offset shard mismatch");
    require(offsets[0].next_sequence == 2, "agent flow delivery offset should advance after ACK");

    const auto resume = log_engine::agent::tail_file_once(source_path, tailed.next_offset, 10);
    require(resume.lines.empty(), "agent flow should not commit partial source line");
    co_return;
}
