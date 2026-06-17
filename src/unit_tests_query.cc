#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include <seastar/core/future.hh>

#include <log_engine_query.pb.h>

#include "log_engine/log_engine.hh"
#include "log_engine/agent_support.hh"
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
    require(!req.has_export_sink_batch(), "proto request should track unset export_sink_batch");
    req.set_include_archive(false);
    req.set_export_sink_batch(true);
    req.set_source_id("source-A");
    req.set_agent_id("agent-A");
    require(req.has_include_archive(), "proto request should track explicit include_archive");
    require(req.include_archive() == false, "proto request should preserve explicit include_archive value");
    require(req.has_export_sink_batch(), "proto request should track explicit export_sink_batch");
    require(req.export_sink_batch() == true, "proto request should preserve explicit export_sink_batch value");
    require(req.source_id() == "source-A", "proto request should preserve source_id filter");
    require(req.agent_id() == "agent-A", "proto request should preserve agent_id filter");
    co_return;
}

seastar::future<> test_agent_metadata_query_and_sink_batch_export(const std::string& root_dir) {
    const auto log_dir = (fs::path(root_dir) / "agent-metadata-query-logs").string();
    const auto archive_dir = (fs::path(root_dir) / "agent-metadata-query-archive").string();
    reset_directory(log_dir);
    reset_directory(archive_dir);

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

    log_engine::agent::AgentRecordEnvelope envelope_a;
    envelope_a.agent_id = "agent-A";
    envelope_a.source_id = "source-A";
    envelope_a.source_offset = 42;
    envelope_a.ingest_timestamp = "2026-06-17T14:30:00Z";
    envelope_a.attributes = {{"host", "edge-1"}, {"trace_id", "trace-1"}};
    envelope_a.message = "metadata-a";

    log_engine::agent::AgentRecordEnvelope envelope_b;
    envelope_b.agent_id = "agent-B";
    envelope_b.source_id = "source-B";
    envelope_b.source_offset = 7;
    envelope_b.message = "metadata-b";

    log_engine::LogEngine engine;
    co_await engine.start(config);
    std::vector<log_engine::LogMessage> messages;
    messages.push_back(log_engine::LogMessage{
        .payload = log_engine::agent::render_agent_record_envelope(envelope_a),
    });
    messages.push_back(log_engine::LogMessage{
        .payload = log_engine::agent::render_agent_record_envelope(envelope_b),
    });
    co_await engine.append_batch(std::move(messages));
    co_await engine.stop();

    log_engine::ReadQuery query;
    query.include_archive = true;
    query.limit = 10;
    query.source_id = "source-A";
    const auto segments = log_engine::collect_segments(config, query);
    const auto records = log_engine::read_records(segments, query);
    require(records.size() == 1, "metadata query should filter by source_id");
    require(records[0].payload == "metadata-a", "metadata query should expose original message payload");
    require(records[0].agent_id == "agent-A", "metadata query should parse agent_id");
    require(records[0].source_id == "source-A", "metadata query should parse source_id");
    require(records[0].source_offset.has_value() && *records[0].source_offset == 42, "metadata query should parse source_offset");
    require(records[0].ingest_timestamp == "2026-06-17T14:30:00Z", "metadata query should parse ingest timestamp");
    require(records[0].attributes.at("host") == "edge-1", "metadata query should parse host attribute");
    require(records[0].attributes.at("trace_id") == "trace-1", "metadata query should parse trace attribute");

    const auto batch = log_engine::agent::build_delivery_batch_from_records(records, 0, 0);
    require(batch.records.size() == 1, "metadata sink export should include filtered record");
    require(batch.records[0] == "metadata-a", "metadata sink export should use normalized payload");
    const auto body = log_engine::agent::render_delivery_batch_json("agent-A", batch);
    require(body.find("\"agent_id\":\"agent-A\"") != std::string::npos, "metadata sink export should include agent id");
    require(body.find("metadata-a") != std::string::npos, "metadata sink export should include payload");
    co_return;
}
