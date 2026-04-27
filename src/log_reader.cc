#include "log_engine/log_reader.hh"

#include <algorithm>
#include <array>
#include <filesystem>
#include <fstream>
#include <sstream>

#include <zlib.h>

namespace log_engine {

std::vector<std::string> collect_log_files(const EngineConfig& config, const ReadQuery& query) {
    namespace fs = std::filesystem;
    std::vector<std::string> files;

    const auto maybe_push = [&](const fs::path& path) {
        if (!path.has_filename()) {
            return;
        }
        const auto name = path.filename().string();
        if (query.shard) {
            const auto shard_prefix = config.shard_file_prefix + "-" + std::to_string(*query.shard);
            if (name.rfind(shard_prefix, 0) != 0) {
                return;
            }
        }
        files.push_back(path.string());
    };

    if (fs::exists(config.archive_dir) && query.include_archive) {
        for (const auto& entry : fs::directory_iterator(config.archive_dir)) {
            if (entry.is_regular_file()) {
                maybe_push(entry.path());
            }
        }
    }
    if (fs::exists(config.log_dir)) {
        for (const auto& entry : fs::directory_iterator(config.log_dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".log") {
                maybe_push(entry.path());
            }
        }
    }

    std::sort(files.begin(), files.end());
    return files;
}

std::vector<ParsedRecord> read_records(const std::vector<std::string>& files, const ReadQuery& query) {
    std::vector<ParsedRecord> records;
    records.reserve(query.limit);

    for (const auto& path : files) {
        std::vector<std::string> lines;
        if (path.size() >= 3 && path.substr(path.size() - 3) == ".gz") {
            gzFile in = gzopen(path.c_str(), "rb");
            if (!in) {
                continue;
            }
            std::string line;
            std::array<char, 4096> buffer{};
            while (gzgets(in, buffer.data(), static_cast<int>(buffer.size())) != Z_NULL) {
                line.assign(buffer.data());
                if (!line.empty() && line.back() == '\n') {
                    line.pop_back();
                }
                lines.push_back(line);
            }
            gzclose(in);
        } else {
            std::ifstream in(path, std::ios::binary);
            std::string line;
            while (std::getline(in, line)) {
                lines.push_back(line);
            }
        }

        for (const auto& line : lines) {
            const auto parsed = parse_record_line(line);
            if (!parsed) {
                continue;
            }
            if (query.seq_from && (!parsed->has_sequence || parsed->sequence < *query.seq_from)) {
                continue;
            }
            if (query.seq_to && (!parsed->has_sequence || parsed->sequence > *query.seq_to)) {
                continue;
            }
            if (query.time_from && (parsed->timestamp.empty() || parsed->timestamp < *query.time_from)) {
                continue;
            }
            if (query.time_to && (parsed->timestamp.empty() || parsed->timestamp > *query.time_to)) {
                continue;
            }
            records.push_back(*parsed);
            if (records.size() >= query.limit) {
                return records;
            }
        }
    }

    return records;
}

}  // namespace log_engine
