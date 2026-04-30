#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "log_engine/config.hh"
#include "log_engine/log_layout.hh"
#include "log_engine/record_codec.hh"

namespace log_engine {

struct ReadQuery {
    std::optional<unsigned> shard;
    std::optional<std::uint64_t> seq_from;
    std::optional<std::uint64_t> seq_to;
    std::optional<std::string> time_from;
    std::optional<std::string> time_to;
    std::size_t limit = 100;
    bool include_archive = true;
};

std::vector<layout::SegmentDescriptor> collect_segments(const EngineConfig& config, const ReadQuery& query);
std::vector<ParsedRecord> read_records(const std::vector<layout::SegmentDescriptor>& segments, const ReadQuery& query);

}  // namespace log_engine
