#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/metrics_registration.hh>

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
    std::optional<std::string> source_id;
    std::optional<std::string> agent_id;
    std::size_t limit = 100;
    bool include_archive = true;
    bool export_sink_batch = false;
};

struct ReaderStats {
    std::uint64_t segments_read = 0;
    std::uint64_t archive_segments_read = 0;
    std::uint64_t active_segments_read = 0;
    std::uint64_t records_returned = 0;
    std::uint64_t corrupted_segments = 0;
    std::uint64_t corrupted_lines = 0;
    std::uint64_t gzip_read_errors = 0;
};

std::vector<layout::SegmentDescriptor> collect_segments(const EngineConfig& config, const ReadQuery& query);
std::vector<ParsedRecord> read_records(const std::vector<layout::SegmentDescriptor>& segments, const ReadQuery& query);
seastar::future<std::vector<ParsedRecord>> read_records_async(
    const std::vector<layout::SegmentDescriptor>& segments,
    const ReadQuery& query);
ReaderStats get_reader_stats() noexcept;
void reset_reader_stats() noexcept;
void register_reader_metrics();
void unregister_reader_metrics() noexcept;

}  // namespace log_engine
