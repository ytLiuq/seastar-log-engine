#include "log_engine/log_manager.hh"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <zlib.h>

#include <seastar/core/metrics.hh>
#include <seastar/core/thread.hh>

#include "log_engine/log_layout.hh"
#include "log_engine/record_codec.hh"

namespace log_engine {

namespace {

std::atomic<std::uint64_t> g_rotate_operations{0};
std::atomic<std::uint64_t> g_checkpoint_write_successes{0};
std::atomic<std::uint64_t> g_checkpoint_write_failures{0};
std::atomic<std::uint64_t> g_recovery_fallbacks{0};
std::atomic<std::uint64_t> g_gzip_archive_successes{0};
std::atomic<std::uint64_t> g_gzip_archive_failures{0};
std::unique_ptr<seastar::metrics::metric_groups> g_log_manager_metrics;

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
    bool saw_logical_size = false;
    bool saw_sequence = false;
    bool saw_rotation_index = false;
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
            saw_logical_size = true;
        } else if (key == "sequence") {
            checkpoint.sequence = parsed;
            saw_sequence = true;
        } else if (key == "rotation_index") {
            checkpoint.rotation_index = parsed;
            saw_rotation_index = true;
        }
    }

    if (!saw_logical_size || !saw_sequence || !saw_rotation_index) {
        return std::nullopt;
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
            try {
                gzip_file(archive_path);
                ++g_gzip_archive_successes;
            } catch (...) {
                ++g_gzip_archive_failures;
                throw;
            }
        }
        cleanup_archives(config, active_segment.shard_id);
        ++g_rotate_operations;
    });
}

seastar::future<> LogManager::store_checkpoint(const layout::SegmentDescriptor& active_segment, const CheckpointState& checkpoint) {
    return seastar::async([active_segment, checkpoint] {
        try {
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
            ++g_checkpoint_write_successes;
        } catch (...) {
            ++g_checkpoint_write_failures;
            throw;
        }
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

        const auto checkpoint_path = layout::checkpoint_path(active_segment);
        const bool checkpoint_file_exists = fs::exists(checkpoint_path);
        const auto checkpoint = read_checkpoint_file(active_segment);
        if (checkpoint && checkpoint->logical_size == verified.valid_size) {
            sequence = checkpoint->sequence;
            rotation_index = checkpoint->rotation_index;
        } else if (checkpoint_file_exists) {
            ++g_recovery_fallbacks;
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
    const auto tmp_gz_path = gz_path + ".tmp";
    std::ifstream in(path, std::ios::binary);
    if (!in.good()) {
        throw std::runtime_error("failed to open archive for gzip: " + path);
    }
    gzFile out = gzopen(tmp_gz_path.c_str(), "wb");
    if (!out) {
        throw std::runtime_error("failed to open gzip output: " + tmp_gz_path);
    }

    try {
        std::array<char, 64 * 1024> buffer{};
        while (in.good()) {
            in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const auto bytes = in.gcount();
            if (bytes <= 0) {
                break;
            }
            if (gzwrite(out, buffer.data(), static_cast<unsigned>(bytes)) == 0) {
                throw std::runtime_error("gzwrite failed for: " + tmp_gz_path);
            }
        }
        if (gzclose(out) != Z_OK) {
            out = nullptr;
            throw std::runtime_error("gzclose failed for: " + tmp_gz_path);
        }
        out = nullptr;
        fs::rename(tmp_gz_path, gz_path);
        fs::remove(path);
    } catch (...) {
        if (out) {
            gzclose(out);
        }
        fs::remove(tmp_gz_path);
        throw;
    }
}

LogManagerStats get_log_manager_stats() noexcept {
    return LogManagerStats{
        .rotate_operations = g_rotate_operations.load(std::memory_order_relaxed),
        .checkpoint_write_successes = g_checkpoint_write_successes.load(std::memory_order_relaxed),
        .checkpoint_write_failures = g_checkpoint_write_failures.load(std::memory_order_relaxed),
        .recovery_fallbacks = g_recovery_fallbacks.load(std::memory_order_relaxed),
        .gzip_archive_successes = g_gzip_archive_successes.load(std::memory_order_relaxed),
        .gzip_archive_failures = g_gzip_archive_failures.load(std::memory_order_relaxed),
    };
}

void reset_log_manager_stats() noexcept {
    g_rotate_operations.store(0, std::memory_order_relaxed);
    g_checkpoint_write_successes.store(0, std::memory_order_relaxed);
    g_checkpoint_write_failures.store(0, std::memory_order_relaxed);
    g_recovery_fallbacks.store(0, std::memory_order_relaxed);
    g_gzip_archive_successes.store(0, std::memory_order_relaxed);
    g_gzip_archive_failures.store(0, std::memory_order_relaxed);
}

void register_log_manager_metrics() {
    namespace sm = seastar::metrics;
    static const sm::label component_label("component");
    const auto component = component_label("log_manager");

    if (!g_log_manager_metrics) {
        g_log_manager_metrics = std::make_unique<sm::metric_groups>();
    } else {
        g_log_manager_metrics->clear();
    }

    std::vector<sm::metric_definition> definitions;
    definitions.reserve(6);
    definitions.push_back(sm::make_counter("rotate_operations", sm::description("Total archive rotation operations"), [] {
            return g_rotate_operations.load(std::memory_order_relaxed);
        })(component));
    definitions.push_back(sm::make_counter("checkpoint_write_successes", sm::description("Total successful checkpoint writes"), [] {
            return g_checkpoint_write_successes.load(std::memory_order_relaxed);
        })(component));
    definitions.push_back(sm::make_counter("checkpoint_write_failures", sm::description("Total failed checkpoint writes"), [] {
            return g_checkpoint_write_failures.load(std::memory_order_relaxed);
        })(component));
    definitions.push_back(sm::make_counter("recovery_fallbacks", sm::description("Total recovery fallback events caused by unusable checkpoint files"), [] {
            return g_recovery_fallbacks.load(std::memory_order_relaxed);
        })(component));
    definitions.push_back(sm::make_counter("gzip_archive_successes", sm::description("Total successful gzip archive compressions"), [] {
            return g_gzip_archive_successes.load(std::memory_order_relaxed);
        })(component));
    definitions.push_back(sm::make_counter("gzip_archive_failures", sm::description("Total failed gzip archive compressions"), [] {
            return g_gzip_archive_failures.load(std::memory_order_relaxed);
        })(component));
    g_log_manager_metrics->add_group("log_engine_log_manager", definitions);
}

void unregister_log_manager_metrics() noexcept {
    if (g_log_manager_metrics) {
        g_log_manager_metrics->clear();
    }
}

}  // namespace log_engine
