#include "log_engine/log_reader.hh"

#include <array>
#include <cstring>
#include <fstream>
#include <utility>

#include <zlib.h>

namespace log_engine {

namespace {

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
bool stream_plain_lines(const std::string& path, Consumer&& consume_line) {
    std::ifstream in(path, std::ios::binary);
    std::string line;
    while (std::getline(in, line)) {
        if (!consume_line(line)) {
            return false;
        }
    }
    return true;
}

template <typename Consumer>
bool stream_gzip_lines(const std::string& path, Consumer&& consume_line) {
    gzFile in = gzopen(path.c_str(), "rb");
    if (!in) {
        return true;
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
            return false;
        }
        line.clear();
    }

    if (!line.empty() && !consume_line(line)) {
        gzclose(in);
        return false;
    }

    gzclose(in);
    return true;
}

template <typename Consumer>
bool stream_segment_lines(const layout::SegmentDescriptor& segment, Consumer&& consume_line) {
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
        const bool should_continue = stream_segment_lines(segment, [&] (std::string_view line) {
            const auto parsed = parse_record_line(line);
            if (!parsed || !matches_record_query(*parsed, query)) {
                return true;
            }
            records.push_back(*parsed);
            return records.size() < query.limit;
        });
        if (!should_continue) {
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

}  // namespace log_engine
