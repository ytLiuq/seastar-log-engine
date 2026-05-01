#include "log_engine/log_manager.hh"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <zlib.h>

#include <seastar/core/thread.hh>

#include "log_engine/log_layout.hh"
#include "log_engine/record_codec.hh"

namespace log_engine {

namespace {

std::optional<CheckpointState> read_checkpoint_file(const layout::SegmentDescriptor& active_segment) {
    const auto path = layout::checkpoint_path(active_segment);
    if (!std::filesystem::exists(path)) {
        return std::nullopt;
    }

    std::ifstream in(path, std::ios::binary);
    if (!in.good()) {
        return std::nullopt;
    }

    CheckpointState checkpoint;
    std::string line;
    while (std::getline(in, line)) {
        const auto pos = line.find('=');
        if (pos == std::string::npos) {
            continue;
        }
        const auto key = line.substr(0, pos);
        const auto value = line.substr(pos + 1);
        std::uint64_t parsed = 0;
        const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed, 10);
        if (result.ec != std::errc{} || result.ptr != value.data() + value.size()) {
            continue;
        }
        if (key == "logical_size") {
            checkpoint.logical_size = parsed;
        } else if (key == "sequence") {
            checkpoint.sequence = parsed;
        } else if (key == "rotation_index") {
            checkpoint.rotation_index = parsed;
        }
    }
    return checkpoint;
}

std::string read_file_to_string(const std::string& path) {
    namespace fs = std::filesystem;
    const auto file_size = fs::file_size(path);
    if (file_size == 0) {
        return {};
    }

    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("failed to open active log for recovery: " + path);
    }

    std::string content(static_cast<std::size_t>(file_size), '\0');
    in.read(content.data(), static_cast<std::streamsize>(content.size()));
    const auto bytes_read = static_cast<std::size_t>(in.gcount());
    if (bytes_read != content.size()) {
        throw std::runtime_error("failed to read active log for recovery: " + path);
    }
    return content;
}

}

seastar::future<> LogManager::prepare(const EngineConfig& config) {
    return seastar::async([config] {
        std::filesystem::create_directories(config.log_dir);
        if (config.archive_features_enabled()) {
            std::filesystem::create_directories(config.archive_dir);
        }
    });
}

seastar::future<> LogManager::rotate_active_file(
    const EngineConfig& config,
    const layout::SegmentDescriptor& active_segment,
    std::uint64_t rotation_index) {
    return seastar::async([config, active_segment, rotation_index] {
        namespace fs = std::filesystem;
        const fs::path active(active_segment.path);
        if (!fs::exists(active)) {
            return;
        }

        const auto now = std::chrono::system_clock::now();
        const auto epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
        auto archive_path = layout::archive_log_path(config, active_segment.shard_id, epoch_ms, rotation_index, false);
        fs::rename(active, archive_path);
        if (config.compress_archives) {
            gzip_file(archive_path);
        }
        cleanup_archives(config, active_segment.shard_id);
    });
}

seastar::future<> LogManager::store_checkpoint(const layout::SegmentDescriptor& active_segment, const CheckpointState& checkpoint) {
    return seastar::async([active_segment, checkpoint] {
        const auto final_path = layout::checkpoint_path(active_segment);
        const auto tmp_path = final_path + ".tmp";
        {
            std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
            out << "logical_size=" << checkpoint.logical_size << "\n";
            out << "sequence=" << checkpoint.sequence << "\n";
            out << "rotation_index=" << checkpoint.rotation_index << "\n";
            out.flush();
            if (!out.good()) {
                throw std::runtime_error("failed to write checkpoint");
            }
        }
        std::filesystem::rename(tmp_path, final_path);
    });
}

seastar::future<std::optional<CheckpointState>> LogManager::load_checkpoint(const layout::SegmentDescriptor& active_segment) {
    return seastar::async([active_segment] () -> std::optional<CheckpointState> {
        return read_checkpoint_file(active_segment);
    });
}

seastar::future<RecoveryState> LogManager::recover_active_file(const layout::SegmentDescriptor& active_segment, std::size_t alignment) {
    return seastar::async([active_segment, alignment] {
        namespace fs = std::filesystem;
        RecoveryState recovery;
        if (!fs::exists(active_segment.path)) {
            return recovery;
        }

        const auto content = read_file_to_string(active_segment.path);
        const auto verified = scan_log_content(content);
        auto valid_size = verified.valid_size;
        auto sequence = verified.next_sequence;
        auto rotation_index = std::uint64_t{0};

        const auto checkpoint = read_checkpoint_file(active_segment);
        if (checkpoint) {
            valid_size = std::min(valid_size, checkpoint->logical_size);
            if (checkpoint->logical_size <= verified.valid_size) {
                sequence = checkpoint->sequence;
                rotation_index = checkpoint->rotation_index;
            }
        }

        recovery.logical_size = valid_size;
        recovery.sequence = sequence;
        recovery.rotation_index = rotation_index;

        if (valid_size == 0) {
            return recovery;
        }

        const auto tail_size = alignment == 0 ? 0 : static_cast<std::size_t>(valid_size % alignment);
        if (tail_size > 0 && valid_size <= content.size()) {
            recovery.tail_buffer = content.substr(valid_size - tail_size, tail_size);
        }
        return recovery;
    });
}

void LogManager::cleanup_archives(const EngineConfig& config, unsigned shard_id) {
    namespace fs = std::filesystem;
    const auto now = fs::file_time_type::clock::now();
    auto archived = layout::collect_archive_segments(config, shard_id);
    archived.erase(std::remove_if(archived.begin(), archived.end(), [&] (const auto& segment) {
        if (config.archive_retention_seconds > 0) {
            const auto age = std::chrono::duration_cast<std::chrono::seconds>(now - fs::last_write_time(segment.path)).count();
            if (age > static_cast<long long>(config.archive_retention_seconds)) {
                fs::remove(segment.path);
                return true;
            }
        }
        return false;
    }), archived.end());

    std::sort(archived.begin(), archived.end(), [] (const auto& lhs, const auto& rhs) {
        if (lhs.timestamp_ms != rhs.timestamp_ms) {
            return lhs.timestamp_ms > rhs.timestamp_ms;
        }
        return lhs.rotation_index > rhs.rotation_index;
    });

    for (std::size_t i = config.max_archived_files_per_shard; i < archived.size(); ++i) {
        fs::remove(archived[i].path);
    }
}

void LogManager::gzip_file(const std::string& path) {
    namespace fs = std::filesystem;
    const auto gz_path = path + ".gz";
    std::ifstream in(path, std::ios::binary);
    if (!in.good()) {
        throw std::runtime_error("failed to open archive for gzip: " + path);
    }
    gzFile out = gzopen(gz_path.c_str(), "wb");
    if (!out) {
        throw std::runtime_error("failed to open gzip output: " + gz_path);
    }

    std::array<char, 64 * 1024> buffer{};
    while (in.good()) {
        in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const auto bytes = in.gcount();
        if (bytes <= 0) {
            break;
        }
        if (gzwrite(out, buffer.data(), static_cast<unsigned>(bytes)) == 0) {
            gzclose(out);
            throw std::runtime_error("gzwrite failed for: " + gz_path);
        }
    }
    gzclose(out);
    fs::remove(path);
}

}  // namespace log_engine
