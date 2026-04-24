#include "log_engine/log_manager.hh"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <zlib.h>

#include <seastar/core/thread.hh>

#include "log_engine/record_codec.hh"

namespace log_engine {

namespace {

std::optional<CheckpointState> read_checkpoint_file(const std::string& active_path) {
    const auto path = active_path + ".checkpoint";
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

}

seastar::future<> LogManager::prepare(const EngineConfig& config) {
    return seastar::async([config] {
        std::filesystem::create_directories(config.log_dir);
        std::filesystem::create_directories(config.archive_dir);
    });
}

seastar::future<> LogManager::rotate_active_file(
    const EngineConfig& config,
    const std::string& active_path,
    unsigned shard_id,
    std::uint64_t rotation_index) {
    return seastar::async([config, active_path, shard_id, rotation_index] {
        namespace fs = std::filesystem;
        const fs::path active(active_path);
        if (!fs::exists(active)) {
            return;
        }

        auto archive_path = make_archive_path(config, active_path, shard_id, rotation_index);
        fs::rename(active, archive_path);
        if (config.compress_archives) {
            gzip_file(archive_path);
            archive_path += ".gz";
        }
        cleanup_archives(config, shard_id);
    });
}

seastar::future<> LogManager::store_checkpoint(const std::string& active_path, const CheckpointState& checkpoint) {
    return seastar::async([active_path, checkpoint] {
        const auto final_path = checkpoint_path(active_path);
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

seastar::future<std::optional<CheckpointState>> LogManager::load_checkpoint(const std::string& active_path) {
    return seastar::async([active_path] () -> std::optional<CheckpointState> {
        return read_checkpoint_file(active_path);
    });
}

seastar::future<RecoveryState> LogManager::recover_active_file(const std::string& active_path, std::size_t alignment) {
    return seastar::async([this, active_path, alignment] {
        namespace fs = std::filesystem;
        RecoveryState recovery;
        if (!fs::exists(active_path)) {
            return recovery;
        }

        std::ifstream in(active_path, std::ios::binary);
        std::stringstream buffer;
        buffer << in.rdbuf();
        const auto content = buffer.str();
        const auto verified = scan_log_content(content);
        auto valid_size = verified.valid_size;
        auto sequence = verified.next_sequence;
        auto rotation_index = std::uint64_t{0};

        const auto checkpoint = read_checkpoint_file(active_path);
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

std::string LogManager::make_archive_path(
    const EngineConfig& config,
    const std::string& active_path,
    unsigned shard_id,
    std::uint64_t rotation_index) {
    namespace fs = std::filesystem;
    const auto now = std::chrono::system_clock::now();
    const auto epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    const auto extension = fs::path(active_path).extension().string();
    const auto filename = config.shard_file_prefix + "-" + std::to_string(shard_id)
        + "." + std::to_string(epoch_ms)
        + "." + std::to_string(rotation_index)
        + extension;
    return (fs::path(config.archive_dir) / filename).string();
}

std::string LogManager::checkpoint_path(const std::string& active_path) {
    return active_path + ".checkpoint";
}

void LogManager::cleanup_archives(const EngineConfig& config, unsigned shard_id) {
    namespace fs = std::filesystem;
    std::vector<fs::directory_entry> archived;
    const auto prefix = config.shard_file_prefix + "-" + std::to_string(shard_id) + ".";
    const auto now = fs::file_time_type::clock::now();
    for (const auto& entry : fs::directory_iterator(config.archive_dir)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto name = entry.path().filename().string();
        if (name.rfind(prefix, 0) != 0) {
            continue;
        }
        if (config.archive_retention_seconds > 0) {
            const auto age = std::chrono::duration_cast<std::chrono::seconds>(now - entry.last_write_time()).count();
            if (age > static_cast<long long>(config.archive_retention_seconds)) {
                fs::remove(entry.path());
                continue;
            }
        }
        archived.push_back(entry);
    }

    std::sort(archived.begin(), archived.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.last_write_time() > rhs.last_write_time();
    });

    for (std::size_t i = config.max_archived_files_per_shard; i < archived.size(); ++i) {
        fs::remove(archived[i].path());
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
