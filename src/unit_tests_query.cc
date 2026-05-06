#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include <seastar/core/future.hh>

#include <log_engine_query.pb.h>

#include "log_engine/log_engine.hh"
#include "log_engine/log_layout.hh"
#include "log_engine/log_reader.hh"

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

}  // namespace

seastar::future<> test_limit_zero_returns_no_records(const std::string& root_dir) {
    const auto log_dir = (fs::path(root_dir) / "limit-zero-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "limit-zero-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

    log_engine::EngineConfig config;
    config.log_dir = log_dir;
    config.archive_dir = archive_dir;
    config.batch_size = 2;

    log_engine::LogEngine engine;
    co_await engine.start(config);
    co_await engine.info("limit-zero-a", "route-limit");
    co_await engine.info("limit-zero-b", "route-limit");
    co_await engine.stop();

    log_engine::ReadQuery query;
    query.include_archive = true;
    query.limit = 0;
    const auto segments = log_engine::collect_segments(config, query);
    const auto records = log_engine::read_records(segments, query);
    require(records.empty(), "limit=0 should return zero records");
    co_return;
}

seastar::future<> test_query_records_proto_defaults() {
    logengine::query::v1::QueryRecordsRequest req;
    require(!req.has_include_archive(), "proto request should track unset include_archive");
    req.set_include_archive(false);
    require(req.has_include_archive(), "proto request should track explicit include_archive");
    require(req.include_archive() == false, "proto request should preserve explicit include_archive value");
    co_return;
}

