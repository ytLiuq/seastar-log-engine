#include "log_engine/log_reader.hh"

#include <array>
#include <atomic>
#include <cstring>
#include <fstream>
#include <memory>
#include <utility>

#include <seastar/core/metrics.hh>
#include <seastar/util/log.hh>

#include <zlib.h>

#include "log_engine/health_monitor.hh"

namespace log_engine {

namespace {

seastar::logger readerlog("log-reader");

std::atomic<std::uint64_t> g_corrupted_segments{0};
std::atomic<std::uint64_t> g_corrupted_lines{0};
std::atomic<std::uint64_t> g_gzip_read_errors{0};
std::atomic<std::uint64_t> g_segments_read{0};
std::atomic<std::uint64_t> g_archive_segments_read{0};
std::atomic<std::uint64_t> g_active_segments_read{0};
std::atomic<std::uint64_t> g_records_returned{0};
std::unique_ptr<seastar::metrics::metric_groups> g_reader_metrics;

struct StreamResult {
    bool stopped_early = false;
    bool read_error = false;
};

bool matches_record_query(const ParsedRecord& record, const ReadQuery& query) {
    if (query.seq_from && (!record.has_sequence || record.sequence < *query.seq_from)) {
        return false;
    }
    if (query.seq_to && (!record.has_sequence || record.sequence > *query.seq_to)) {
        return false;
    }
    if (query.time_from && (record.timestamp.empty() || record.timestamp < *query.time_from)) {
        return false;
    }
    if (query.time_to && (record.timestamp.empty() || record.timestamp > *query.time_to)) {
        return false;
    }
    return true;
}

template <typename Consumer>
StreamResult stream_plain_lines(const std::string& path, Consumer&& consume_line) {
    std::ifstream in(path, std::ios::binary);
    std::string line;
    while (std::getline(in, line)) {
        if (!consume_line(line)) {
            return StreamResult{.stopped_early = true};
        }
    }
    return StreamResult{
        .stopped_early = false,
        .read_error = !in.eof() && in.fail(),
    };
}

template <typename Consumer>
StreamResult stream_gzip_lines(const std::string& path, Consumer&& consume_line) {
    gzFile in = gzopen(path.c_str(), "rb");
    if (!in) {
        ++g_gzip_read_errors;
        record_reader_gzip_read_error();
        readerlog.warn("failed to open gzip archive for reading: {}", path);
        return StreamResult{.stopped_early = false, .read_error = true};
    }

    std::array<char, 4096> buffer{};
    std::string line;
    while (gzgets(in, buffer.data(), static_cast<int>(buffer.size())) != Z_NULL) {
        const auto chunk_size = std::strlen(buffer.data());
        const bool ended_with_newline = chunk_size > 0 && buffer[chunk_size - 1] == '\n';
        line.append(buffer.data(), chunk_size);
        if (!ended_with_newline) {
            continue;
        }

        line.pop_back();
        if (!consume_line(line)) {
            gzclose(in);
            return StreamResult{.stopped_early = true};
        }
        line.clear();
    }

    int error_code = Z_OK;
    const char* error_message = gzerror(in, &error_code);
    const bool read_error = error_code != Z_OK && error_code != Z_STREAM_END;
    if (read_error) {
        ++g_gzip_read_errors;
        record_reader_gzip_read_error();
        readerlog.warn("gzip archive read error on {}: {}", path, error_message ? error_message : "unknown error");
    }

    if (!read_error && !line.empty() && !consume_line(line)) {
        gzclose(in);
        return StreamResult{.stopped_early = true};
    }

    gzclose(in);
    return StreamResult{
        .stopped_early = false,
        .read_error = read_error,
    };
}

template <typename Consumer>
StreamResult stream_segment_lines(const layout::SegmentDescriptor& segment, Consumer&& consume_line) {
    if (segment.compressed) {
        return stream_gzip_lines(segment.path, std::forward<Consumer>(consume_line));
    }
    return stream_plain_lines(segment.path, std::forward<Consumer>(consume_line));
}

std::vector<ParsedRecord> read_records_from_segments(
    const std::vector<layout::SegmentDescriptor>& segments,
    const ReadQuery& query) {
    std::vector<ParsedRecord> records;
    records.reserve(query.limit);

    for (const auto& segment : segments) {
        ++g_segments_read;
        if (segment.archived) {
            ++g_archive_segments_read;
        } else {
            ++g_active_segments_read;
        }
        bool segment_corrupted = false;
        const auto result = stream_segment_lines(segment, [&] (std::string_view line) {
            const auto parsed = parse_record_line(line);
            if (!parsed) {
                segment_corrupted = true;
                ++g_corrupted_lines;
                record_reader_corrupted_line();
                return false;
            }
            if (!matches_record_query(*parsed, query)) {
                return true;
            }
            records.push_back(*parsed);
            ++g_records_returned;
            return records.size() < query.limit;
        });
        if (segment_corrupted || result.read_error) {
            ++g_corrupted_segments;
            record_reader_corrupted_segment();
            readerlog.warn(
                "stopped reading corrupted segment: path={}, compressed={}, parse_error={}, read_error={}",
                segment.path,
                segment.compressed,
                segment_corrupted,
                result.read_error);
            continue;
        }
        if (result.stopped_early) {
            break;
        }
    }

    return records;
}

}  // namespace

std::vector<layout::SegmentDescriptor> collect_segments(const EngineConfig& config, const ReadQuery& query) {
    return layout::collect_query_segments(config, query.shard, query.include_archive);
}

std::vector<ParsedRecord> read_records(const std::vector<layout::SegmentDescriptor>& segments, const ReadQuery& query) {
    return read_records_from_segments(segments, query);
}

ReaderStats get_reader_stats() noexcept {
    return ReaderStats{
        .segments_read = g_segments_read.load(std::memory_order_relaxed),
        .archive_segments_read = g_archive_segments_read.load(std::memory_order_relaxed),
        .active_segments_read = g_active_segments_read.load(std::memory_order_relaxed),
        .records_returned = g_records_returned.load(std::memory_order_relaxed),
        .corrupted_segments = g_corrupted_segments.load(std::memory_order_relaxed),
        .corrupted_lines = g_corrupted_lines.load(std::memory_order_relaxed),
        .gzip_read_errors = g_gzip_read_errors.load(std::memory_order_relaxed),
    };
}

void reset_reader_stats() noexcept {
    g_segments_read.store(0, std::memory_order_relaxed);
    g_archive_segments_read.store(0, std::memory_order_relaxed);
    g_active_segments_read.store(0, std::memory_order_relaxed);
    g_records_returned.store(0, std::memory_order_relaxed);
    g_corrupted_segments.store(0, std::memory_order_relaxed);
    g_corrupted_lines.store(0, std::memory_order_relaxed);
    g_gzip_read_errors.store(0, std::memory_order_relaxed);
}

void register_reader_metrics() {
    namespace sm = seastar::metrics;
    static const sm::label component_label("component");
    const auto component = component_label("reader");

    if (!g_reader_metrics) {
        g_reader_metrics = std::make_unique<sm::metric_groups>();
    } else {
        g_reader_metrics->clear();
    }

    std::vector<sm::metric_definition> definitions;
    definitions.reserve(7);
    definitions.push_back(sm::make_counter("segments_read", sm::description("Total query segments read"), [] {
            return g_segments_read.load(std::memory_order_relaxed);
        })(component));
    definitions.push_back(sm::make_counter("archive_segments_read", sm::description("Total archived query segments read"), [] {
            return g_archive_segments_read.load(std::memory_order_relaxed);
        })(component));
    definitions.push_back(sm::make_counter("active_segments_read", sm::description("Total active query segments read"), [] {
            return g_active_segments_read.load(std::memory_order_relaxed);
        })(component));
    definitions.push_back(sm::make_counter("records_returned", sm::description("Total query records returned"), [] {
            return g_records_returned.load(std::memory_order_relaxed);
        })(component));
    definitions.push_back(sm::make_counter("corrupted_segments", sm::description("Total corrupted query segments detected"), [] {
            return g_corrupted_segments.load(std::memory_order_relaxed);
        })(component));
    definitions.push_back(sm::make_counter("corrupted_lines", sm::description("Total corrupted lines detected while reading query segments"), [] {
            return g_corrupted_lines.load(std::memory_order_relaxed);
        })(component));
    definitions.push_back(sm::make_counter("gzip_read_errors", sm::description("Total gzip archive read errors detected by the query reader"), [] {
            return g_gzip_read_errors.load(std::memory_order_relaxed);
        })(component));
    g_reader_metrics->add_group("log_engine_reader", definitions);
}

void unregister_reader_metrics() noexcept {
    if (g_reader_metrics) {
        g_reader_metrics->clear();
    }
}

}  // namespace log_engine
