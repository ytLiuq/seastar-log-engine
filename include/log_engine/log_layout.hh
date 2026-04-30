#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "log_engine/config.hh"

namespace log_engine::layout {

struct SegmentDescriptor {
    std::string path;
    unsigned shard_id = 0;
    bool archived = false;
    bool compressed = false;
    std::uint64_t timestamp_ms = 0;
    std::uint64_t rotation_index = 0;
};

std::string shard_prefix(const EngineConfig& config, unsigned shard_id);
SegmentDescriptor active_segment(const EngineConfig& config, unsigned shard_id);
std::string active_log_path(const EngineConfig& config, unsigned shard_id);
std::string checkpoint_path(std::string_view active_path);
std::string checkpoint_path(const SegmentDescriptor& active_segment);
std::string archive_log_path(
    const EngineConfig& config,
    unsigned shard_id,
    std::uint64_t timestamp_ms,
    std::uint64_t rotation_index,
    bool compressed);
std::vector<SegmentDescriptor> collect_active_segments(const EngineConfig& config, const std::optional<unsigned>& shard);
std::vector<SegmentDescriptor> collect_archive_segments(const EngineConfig& config, const std::optional<unsigned>& shard);
std::vector<SegmentDescriptor> collect_query_segments(
    const EngineConfig& config,
    const std::optional<unsigned>& shard,
    bool include_archive);
std::optional<SegmentDescriptor> describe_path(const EngineConfig& config, const std::string& path);
bool matches_query_shard(const SegmentDescriptor& segment, const std::optional<unsigned>& shard);
void sort_segments(std::vector<SegmentDescriptor>& segments);

}  // namespace log_engine::layout
