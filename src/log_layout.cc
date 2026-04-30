#include "log_engine/log_layout.hh"

#include <algorithm>
#include <charconv>
#include <filesystem>

namespace log_engine::layout {

namespace {

bool parse_u64(std::string_view value, std::uint64_t& out) {
    const auto result = std::from_chars(value.data(), value.data() + value.size(), out, 10);
    return result.ec == std::errc{} && result.ptr == value.data() + value.size();
}

}  // namespace

std::string shard_prefix(const EngineConfig& config, unsigned shard_id) {
    return config.shard_file_prefix + "-" + std::to_string(shard_id);
}

SegmentDescriptor active_segment(const EngineConfig& config, unsigned shard_id) {
    return SegmentDescriptor{
        .path = active_log_path(config, shard_id),
        .shard_id = shard_id,
        .archived = false,
        .compressed = false,
    };
}

std::string active_log_path(const EngineConfig& config, unsigned shard_id) {
    return config.log_dir + "/" + shard_prefix(config, shard_id) + ".log";
}

std::string checkpoint_path(std::string_view active_path) {
    return std::string(active_path) + ".checkpoint";
}

std::string checkpoint_path(const SegmentDescriptor& active_segment) {
    return checkpoint_path(active_segment.path);
}

std::string archive_log_path(
    const EngineConfig& config,
    unsigned shard_id,
    std::uint64_t timestamp_ms,
    std::uint64_t rotation_index,
    bool compressed) {
    auto filename = shard_prefix(config, shard_id)
        + "." + std::to_string(timestamp_ms)
        + "." + std::to_string(rotation_index)
        + ".log";
    if (compressed) {
        filename += ".gz";
    }
    return config.archive_dir + "/" + filename;
}

std::vector<SegmentDescriptor> collect_active_segments(const EngineConfig& config, const std::optional<unsigned>& shard) {
    namespace fs = std::filesystem;
    std::vector<SegmentDescriptor> segments;
    if (!fs::exists(config.log_dir)) {
        return segments;
    }

    for (const auto& entry : fs::directory_iterator(config.log_dir)) {
        if (!entry.is_regular_file() || entry.path().extension() != ".log") {
            continue;
        }
        const auto segment = describe_path(config, entry.path().string());
        if (!segment || segment->archived || !matches_query_shard(*segment, shard)) {
            continue;
        }
        segments.push_back(*segment);
    }

    sort_segments(segments);
    return segments;
}

std::vector<SegmentDescriptor> collect_archive_segments(const EngineConfig& config, const std::optional<unsigned>& shard) {
    namespace fs = std::filesystem;
    std::vector<SegmentDescriptor> segments;
    if (!fs::exists(config.archive_dir)) {
        return segments;
    }

    for (const auto& entry : fs::directory_iterator(config.archive_dir)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto segment = describe_path(config, entry.path().string());
        if (!segment || !segment->archived || !matches_query_shard(*segment, shard)) {
            continue;
        }
        segments.push_back(*segment);
    }

    sort_segments(segments);
    return segments;
}

std::vector<SegmentDescriptor> collect_query_segments(
    const EngineConfig& config,
    const std::optional<unsigned>& shard,
    bool include_archive) {
    auto segments = include_archive ? collect_archive_segments(config, shard) : std::vector<SegmentDescriptor>{};
    auto active = collect_active_segments(config, shard);
    segments.insert(segments.end(), std::make_move_iterator(active.begin()), std::make_move_iterator(active.end()));
    sort_segments(segments);
    return segments;
}

std::optional<SegmentDescriptor> describe_path(const EngineConfig& config, const std::string& path) {
    namespace fs = std::filesystem;
    const fs::path file_path(path);
    const auto filename = file_path.filename().string();
    const auto extension = file_path.extension().string();

    SegmentDescriptor segment;
    segment.path = path;

    if (extension == ".log") {
        const auto prefix = config.shard_file_prefix + "-";
        if (filename.rfind(prefix, 0) != 0) {
            return std::nullopt;
        }
        const auto shard_part = filename.substr(prefix.size(), filename.size() - prefix.size() - 4);
        std::uint64_t shard_value = 0;
        if (!parse_u64(shard_part, shard_value)) {
            return std::nullopt;
        }
        segment.shard_id = static_cast<unsigned>(shard_value);
        segment.archived = false;
        return segment;
    }

    segment.compressed = extension == ".gz";
    const auto archive_name = segment.compressed
        ? filename.substr(0, filename.size() - 3)
        : filename;
    if (archive_name.size() <= 4 || archive_name.substr(archive_name.size() - 4) != ".log") {
        return std::nullopt;
    }
    const auto base = archive_name.substr(0, archive_name.size() - 4);
    const auto prefix = config.shard_file_prefix + "-";
    if (base.rfind(prefix, 0) != 0) {
        return std::nullopt;
    }

    const auto first_dot = base.find('.', prefix.size());
    const auto second_dot = first_dot == std::string::npos ? std::string::npos : base.find('.', first_dot + 1);
    if (first_dot == std::string::npos || second_dot == std::string::npos) {
        return std::nullopt;
    }

    std::uint64_t shard_value = 0;
    std::uint64_t timestamp_ms = 0;
    std::uint64_t rotation_index = 0;
    if (!parse_u64(std::string_view(base).substr(prefix.size(), first_dot - prefix.size()), shard_value) ||
        !parse_u64(std::string_view(base).substr(first_dot + 1, second_dot - first_dot - 1), timestamp_ms) ||
        !parse_u64(std::string_view(base).substr(second_dot + 1), rotation_index)) {
        return std::nullopt;
    }

    segment.shard_id = static_cast<unsigned>(shard_value);
    segment.archived = true;
    segment.timestamp_ms = timestamp_ms;
    segment.rotation_index = rotation_index;
    return segment;
}

bool matches_query_shard(const SegmentDescriptor& segment, const std::optional<unsigned>& shard) {
    return !shard || segment.shard_id == *shard;
}

void sort_segments(std::vector<SegmentDescriptor>& segments) {
    std::sort(segments.begin(), segments.end(), [] (const SegmentDescriptor& lhs, const SegmentDescriptor& rhs) {
        if (lhs.shard_id != rhs.shard_id) {
            return lhs.shard_id < rhs.shard_id;
        }
        if (lhs.archived != rhs.archived) {
            return lhs.archived && !rhs.archived;
        }
        if (lhs.archived && rhs.archived) {
            if (lhs.timestamp_ms != rhs.timestamp_ms) {
                return lhs.timestamp_ms < rhs.timestamp_ms;
            }
            if (lhs.rotation_index != rhs.rotation_index) {
                return lhs.rotation_index < rhs.rotation_index;
            }
        }
        return lhs.path < rhs.path;
    });
}

}  // namespace log_engine::layout
