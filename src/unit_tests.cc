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

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

seastar::future<> test_record_codec() {
    const auto encoded = log_engine::encode_record(
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
    co_return;
}

seastar::future<> test_config_loader(const std::string& root_dir) {
    namespace fs = std::filesystem;
    const auto config_path = (fs::path(root_dir) / "engine-test.conf").string();
    {
        std::ofstream out(config_path, std::ios::trunc);
        out << "log-dir=/tmp/demo-logs\n";
        out << "batch-size=17\n";
        out << "rotate-interval-seconds=9\n";
        out << "compress-archives=false\n";
    }

    const auto values = log_engine::load_config_file(config_path);
    boost::program_options::variables_map cli;
    auto config = log_engine::apply_engine_config_overrides(log_engine::EngineConfig{}, cli, values);
    require(config.log_dir == "/tmp/demo-logs", "config loader should override log_dir");
    require(config.batch_size == 17, "config loader should override batch_size");
    require(config.rotate_interval_seconds == 9, "config loader should override rotate interval");
    require(config.compress_archives == false, "config loader should override compress_archives");
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
    const auto files = log_engine::collect_log_files(config, query);
    const auto records = log_engine::read_records(files, query);
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
    const auto files = log_engine::collect_log_files(config, query);
    const auto records = log_engine::read_records(files, query);
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
        co_await test_compat_logging(root_dir);
        co_await test_time_rotation_and_archive_read(root_dir);
        co_await test_recovery_scan(root_dir);
        co_return;
    });
}
