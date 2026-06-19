#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <seastar/core/future.hh>

#include "log_engine/config.hh"
#include "log_engine/record_codec.hh"

namespace log_engine::agent {

struct SourceOffset {
    std::string path;
    std::uint64_t inode = 0;
    std::uint64_t offset = 0;
};

struct DeliveryOffset {
    unsigned shard = 0;
    std::uint64_t next_sequence = 0;
};

struct DeliveryBatch {
    unsigned shard = 0;
    std::uint64_t first_sequence = 0;
    std::uint64_t next_sequence = 0;
    std::vector<std::string> records;
};

struct AgentRecordEnvelope {
    std::string agent_id;
    std::string source_id;
    std::optional<std::uint64_t> source_offset;
    std::string ingest_timestamp;
    std::map<std::string, std::string> attributes;
    std::string message;
};

struct IngestParseOptions {
    std::string default_agent_id;
    std::string default_source_id;
};

struct IngestParseResult {
    std::vector<LogMessage> messages;
    std::size_t malformed_records = 0;
};

struct TailBatch {
    SourceOffset next_offset;
    std::vector<std::string> lines;
    bool file_rotated_or_truncated = false;
};

struct MultilineOptions {
    bool enabled = false;
    std::string start_pattern;
    std::size_t max_lines = 128;
};

struct SourceLimitDecision {
    bool accept = true;
    std::string reason;
};

struct SourceLimits {
    std::size_t max_message_bytes = 0;
    std::size_t max_buffer_bytes = 0;
};

struct DiskQuota {
    std::uint64_t max_buffer_bytes = 0;
    std::uint64_t resume_buffer_bytes = 0;
};

struct BackpressureState {
    std::uint64_t disk_bytes = 0;
    std::uint64_t pending_bytes = 0;
    std::uint64_t sink_backlog_records = 0;
    std::uint64_t recent_sink_failures = 0;
    std::uint64_t last_sink_latency_ms = 0;
    std::uint64_t sink_latency_average_ms = 0;
    std::uint64_t max_sink_backlog_records = 0;
    std::uint64_t max_recent_sink_failures = 0;
    std::uint64_t max_sink_latency_ms = 0;
    std::uint64_t max_pending_bytes = 0;
    std::uint64_t max_sink_latency_average_ms = 0;
};

struct BackpressureDecision {
    bool pause = false;
    std::string reason;
};

struct HttpEndpoint {
    std::string host;
    std::uint16_t port = 80;
    std::string path = "/";
};

struct HttpHeader {
    std::string name;
    std::string value;
};

struct HttpPostOptions {
    std::uint64_t timeout_ms = 5000;
    std::vector<int> retryable_status_codes = {408, 425, 429, 500, 502, 503, 504};
    std::vector<HttpHeader> headers;
};

struct KafkaSidecarOptions {
    std::string topic = "logs";
    std::string bootstrap_servers;
};

struct ObjectStoreOptions {
    std::string bucket;
    std::string prefix = "logs";
    std::string compression = "none";
};

class RetryableHttpStatusError : public std::runtime_error {
public:
    RetryableHttpStatusError(int status, std::string status_line);
    int status() const noexcept;

private:
    int _status = 0;
};

struct ReplayOptions {
    std::string delivery_offset_path;
    std::size_t batch_size = 100;
    bool include_archive = true;
    std::optional<unsigned> shard;
};

std::optional<SourceOffset> load_source_offset(const std::string& path);
void store_source_offset(const std::string& path, const SourceOffset& offset);

std::optional<DeliveryOffset> load_delivery_offset(const std::string& path);
void store_delivery_offset(const std::string& path, const DeliveryOffset& offset);
seastar::future<std::optional<DeliveryOffset>> load_delivery_offset_async(const std::string& path);
seastar::future<> store_delivery_offset_async(const std::string& path, const DeliveryOffset& offset);
seastar::future<std::vector<DeliveryOffset>> load_delivery_offsets_file_async(const std::string& path);
std::vector<DeliveryOffset> load_delivery_offsets(const std::string& path);
void store_delivery_offsets(const std::string& path, const std::vector<DeliveryOffset>& offsets);
std::string shard_state_path(std::string_view base_path, unsigned shard);
std::optional<DeliveryBatch> load_pending_delivery_batch(const std::string& path);
void store_pending_delivery_batch(const std::string& path, const DeliveryBatch& batch);
void remove_pending_delivery_batch(const std::string& path);
seastar::future<std::optional<DeliveryBatch>> load_pending_delivery_batch_async(const std::string& path);
seastar::future<> store_pending_delivery_batch_async(const std::string& path, const DeliveryBatch& batch);
seastar::future<> remove_pending_delivery_batch_async(const std::string& path);
std::vector<DeliveryBatch> build_replay_batches(const EngineConfig& config, const ReplayOptions& options);
DeliveryBatch build_delivery_batch_from_records(
    const std::vector<ParsedRecord>& records,
    unsigned fallback_shard,
    std::uint64_t fallback_first_sequence);

std::uint64_t directory_size_bytes(const std::string& path);
bool disk_quota_exceeded(const std::string& path, const DiskQuota& quota);
bool disk_quota_can_resume(const std::string& path, const DiskQuota& quota);
BackpressureDecision evaluate_backpressure(const std::string& path, const DiskQuota& quota, const BackpressureState& state);

TailBatch tail_file_once(
    const std::string& path,
    const std::optional<SourceOffset>& previous,
    std::size_t max_lines);
std::vector<std::string> expand_glob_paths(std::string_view pattern);
std::vector<std::string> apply_multiline_records(const std::vector<std::string>& lines, const MultilineOptions& options);
SourceLimitDecision evaluate_source_limits(
    std::size_t message_bytes,
    std::size_t buffered_bytes,
    const SourceLimits& limits);

std::optional<HttpEndpoint> parse_http_endpoint(std::string_view url);
std::vector<int> parse_http_status_codes(std::string_view value);
std::vector<HttpHeader> parse_http_headers(std::string_view value);
IngestParseResult parse_ingest_body(std::string_view body, const IngestParseOptions& options);
std::string render_json_batch(const std::vector<std::string>& records);
std::string render_delivery_batch_json(std::string_view agent_id, const DeliveryBatch& batch);
std::string render_agent_record_envelope(const AgentRecordEnvelope& record);
std::string render_kafka_sidecar_batch_json(
    std::string_view agent_id,
    const DeliveryBatch& batch,
    const KafkaSidecarOptions& options);
std::string render_object_store_manifest_json(
    std::string_view agent_id,
    const DeliveryBatch& batch,
    const ObjectStoreOptions& options);
void post_http_batch(const HttpEndpoint& endpoint, std::string_view body);
seastar::future<> post_http_batch_async(const HttpEndpoint& endpoint, std::string body);
seastar::future<> post_http_batch_async(const HttpEndpoint& endpoint, std::string body, const HttpPostOptions& options);
void write_stdout_batch(const std::vector<std::string>& records);

}  // namespace log_engine::agent
