#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <seastar/core/future.hh>

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

struct TailBatch {
    SourceOffset next_offset;
    std::vector<std::string> lines;
    bool file_rotated_or_truncated = false;
};

struct DiskQuota {
    std::uint64_t max_buffer_bytes = 0;
    std::uint64_t resume_buffer_bytes = 0;
};

struct BackpressureState {
    std::uint64_t disk_bytes = 0;
    std::uint64_t sink_backlog_records = 0;
    std::uint64_t recent_sink_failures = 0;
    std::uint64_t last_sink_latency_ms = 0;
    std::uint64_t max_sink_backlog_records = 0;
    std::uint64_t max_recent_sink_failures = 0;
    std::uint64_t max_sink_latency_ms = 0;
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

std::optional<SourceOffset> load_source_offset(const std::string& path);
void store_source_offset(const std::string& path, const SourceOffset& offset);

std::optional<DeliveryOffset> load_delivery_offset(const std::string& path);
void store_delivery_offset(const std::string& path, const DeliveryOffset& offset);
std::vector<DeliveryOffset> load_delivery_offsets(const std::string& path);
void store_delivery_offsets(const std::string& path, const std::vector<DeliveryOffset>& offsets);

std::uint64_t directory_size_bytes(const std::string& path);
bool disk_quota_exceeded(const std::string& path, const DiskQuota& quota);
bool disk_quota_can_resume(const std::string& path, const DiskQuota& quota);
BackpressureDecision evaluate_backpressure(const std::string& path, const DiskQuota& quota, const BackpressureState& state);

TailBatch tail_file_once(
    const std::string& path,
    const std::optional<SourceOffset>& previous,
    std::size_t max_lines);
std::vector<std::string> expand_glob_paths(std::string_view pattern);

std::optional<HttpEndpoint> parse_http_endpoint(std::string_view url);
std::string render_json_batch(const std::vector<std::string>& records);
void post_http_batch(const HttpEndpoint& endpoint, std::string_view body);
seastar::future<> post_http_batch_async(const HttpEndpoint& endpoint, std::string body);
void write_stdout_batch(const std::vector<std::string>& records);

}  // namespace log_engine::agent
