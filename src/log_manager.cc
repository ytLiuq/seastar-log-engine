#include "log_engine/log_manager.hh"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include <zlib.h>

#include <seastar/core/metrics.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>

#include "log_engine/health_monitor.hh"
#include "log_engine/log_layout.hh"
#include "log_engine/record_codec.hh"

namespace log_engine {

namespace {

seastar::logger mgrlog("log-manager");

std::atomic<std::uint64_t> g_rotate_operations{0};
std::atomic<std::uint64_t> g_checkpoint_write_successes{0};
std::atomic<std::uint64_t> g_checkpoint_write_failures{0};
std::atomic<std::uint64_t> g_recovery_fallbacks{0};
std::atomic<std::uint64_t> g_gzip_archive_successes{0};
std::atomic<std::uint64_t> g_gzip_archive_failures{0};
std::atomic<std::uint64_t> g_recovery_fallback_incomplete_checkpoint{0};
std::atomic<std::uint64_t> g_recovery_fallback_stale_checkpoint{0};
std::atomic<int> g_last_recovery_fallback_reason{
    static_cast<int>(RecoveryFallbackReason::none)};
std::unique_ptr<seastar::metrics::metric_groups> g_log_manager_metrics;

constexpr std::string_view kCheckpointFormatVersion = "2";

std::string checkpoint_body(const CheckpointState& checkpoint) {
    std::ostringstream out;
    out << "format_version=" << kCheckpointFormatVersion << "\n";
    out << "logical_size=" << checkpoint.logical_size << "\n";
    out << "sequence=" << checkpoint.sequence << "\n";
    out << "rotation_index=" << checkpoint.rotation_index << "\n";
    return out.str();
}

void fsync_path(const std::filesystem::path& path, int flags) {
    const int fd = ::open(path.c_str(), flags);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(), "failed to open for fsync: " + path.string());
    }
    const int result = ::fsync(fd);
    const int saved_errno = errno;
    ::close(fd);
    if (result != 0) {
        throw std::system_error(saved_errno, std::generic_category(), "failed to fsync: " + path.string());
    }
}

void fsync_file(const std::filesystem::path& path) {
    fsync_path(path, O_RDONLY);
}

void fsync_directory(const std::filesystem::path& path) {
    fsync_path(path, O_RDONLY | O_DIRECTORY);
}

std::optional<std::uint64_t> parse_u64(std::string_view value) {
    std::uint64_t parsed = 0;
    const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed, 10);
    if (result.ec != std::errc{} || result.ptr != value.data() + value.size()) {
        return std::nullopt;
    }
    return parsed;
}

std::optional<CheckpointState> read_checkpoint_file(const layout::SegmentDescriptor& active_segment) {
    const auto path = layout::checkpoint_path(active_segment);
    if (!std::filesystem::exists(path)) {
        return std::nullopt;
    }

    std::ifstream in(path, std::ios::binary);
    if (!in.good()) {
        return std::nullopt;
    }
    const std::string content{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};

    CheckpointState checkpoint;
    bool saw_version = false;
    bool saw_logical_size = false;
    bool saw_sequence = false;
    bool saw_rotation_index = false;
    bool saw_checkpoint_crc = false;
    std::uint32_t checkpoint_crc = 0;
    std::string signed_body;
    std::string line;
    std::istringstream lines(content);
    while (std::getline(lines, line)) {
        const auto pos = line.find('=');
        if (pos == std::string::npos) {
            continue;
        }
        const auto key = line.substr(0, pos);
        const auto value = line.substr(pos + 1);
        if (key == "checkpoint_crc") {
            const auto parsed = parse_u64(value);
            if (!parsed || *parsed > std::numeric_limits<std::uint32_t>::max()) {
                return std::nullopt;
            }
            checkpoint_crc = static_cast<std::uint32_t>(*parsed);
            saw_checkpoint_crc = true;
            continue;
        }

        signed_body += line;
        signed_body += '\n';

        if (key == "format_version") {
            if (value != kCheckpointFormatVersion) {
                return std::nullopt;
            }
            saw_version = true;
        } else if (key == "logical_size") {
            const auto parsed = parse_u64(value);
            if (!parsed) {
                return std::nullopt;
            }
            checkpoint.logical_size = *parsed;
            saw_logical_size = true;
        } else if (key == "sequence") {
            const auto parsed = parse_u64(value);
            if (!parsed) {
                return std::nullopt;
            }
            checkpoint.sequence = *parsed;
            saw_sequence = true;
        } else if (key == "rotation_index") {
            const auto parsed = parse_u64(value);
            if (!parsed) {
                return std::nullopt;
            }
            checkpoint.rotation_index = *parsed;
            saw_rotation_index = true;
        }
    }

    if (!saw_version || !saw_logical_size || !saw_sequence || !saw_rotation_index || !saw_checkpoint_crc) {
        return std::nullopt;
    }
    if (crc32(signed_body) != checkpoint_crc) {
        return std::nullopt;
    }
    return checkpoint;
}

void remove_file_if_exists(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
}

void cleanup_temporary_sidecars(const EngineConfig& config) {
    namespace fs = std::filesystem;
    std::size_t removed_checkpoint_tmps = 0;
    std::size_t removed_gzip_tmps = 0;

    if (fs::exists(config.log_dir)) {
        for (const auto& entry : fs::directory_iterator(config.log_dir)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            const auto path = entry.path().string();
            if (path.size() >= 15 && path.substr(path.size() - 15) == ".checkpoint.tmp") {
                fs::remove(entry.path());
                ++removed_checkpoint_tmps;
            }
        }
    }

    if (config.archive_features_enabled() && fs::exists(config.archive_dir)) {
        for (const auto& entry : fs::directory_iterator(config.archive_dir)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            const auto path = entry.path().string();
            if (path.size() >= 7 && path.substr(path.size() - 7) == ".gz.tmp") {
                fs::remove(entry.path());
                ++removed_gzip_tmps;
            }
        }
    }

    if (removed_checkpoint_tmps > 0 || removed_gzip_tmps > 0) {
        mgrlog.warn(
            "startup cleanup removed {} checkpoint.tmp and {} gz.tmp sidecars",
            removed_checkpoint_tmps,
            removed_gzip_tmps);
    }
}

struct StreamedVerifiedLogState {
    VerifiedLogState verified;
    std::string trailing_bytes;
};

StreamedVerifiedLogState scan_log_file_streaming(
    const std::string& path,
    std::uint64_t start_offset,
    std::uint64_t next_sequence,
    std::size_t trailing_capacity) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("failed to open active log for recovery: " + path);
    }
    in.seekg(static_cast<std::streamoff>(start_offset));
    if (!in.good()) {
        throw std::runtime_error("failed to seek active log for recovery: " + path);
    }

    StreamedVerifiedLogState state;
    state.verified.valid_size = start_offset;
    state.verified.next_sequence = next_sequence;
    std::array<char, 64 * 1024> buffer{};
    std::string pending_line;
    pending_line.reserve(4096);

    auto append_to_trailing = [&](std::string_view bytes) {
        if (trailing_capacity == 0 || bytes.empty()) {
            return;
        }
        if (bytes.size() >= trailing_capacity) {
            state.trailing_bytes.assign(bytes.substr(bytes.size() - trailing_capacity));
            return;
        }
        const auto overflow = state.trailing_bytes.size() + bytes.size() > trailing_capacity
            ? state.trailing_bytes.size() + bytes.size() - trailing_capacity
            : 0;
        if (overflow > 0) {
            state.trailing_bytes.erase(0, overflow);
        }
        state.trailing_bytes.append(bytes.data(), bytes.size());
    };

    while (in.good()) {
        in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const auto bytes_read = static_cast<std::size_t>(in.gcount());
        if (bytes_read == 0) {
            break;
        }

        std::string_view chunk(buffer.data(), bytes_read);
        std::size_t offset = 0;
        while (offset < chunk.size()) {
            const auto newline = chunk.find('\n', offset);
            if (newline == std::string_view::npos) {
                pending_line.append(chunk.substr(offset));
                break;
            }

            if (pending_line.empty()) {
                const auto line = chunk.substr(offset, newline - offset);
                if (!line.empty()) {
                    if (!verify_record_line(line)) {
                        state.verified.clean_end = false;
                        return state;
                    }
                    const auto sequence = extract_sequence(line);
                    state.verified.next_sequence = sequence ? (*sequence + 1) : (state.verified.next_sequence + 1);
                    ++state.verified.valid_records;
                }
                state.verified.valid_size += static_cast<std::uint64_t>(newline - offset + 1);
                append_to_trailing(chunk.substr(offset, newline - offset + 1));
            } else {
                pending_line.append(chunk.substr(offset, newline - offset));
                if (!pending_line.empty()) {
                    if (!verify_record_line(pending_line)) {
                        state.verified.clean_end = false;
                        return state;
                    }
                    const auto sequence = extract_sequence(pending_line);
                    state.verified.next_sequence = sequence ? (*sequence + 1) : (state.verified.next_sequence + 1);
                    ++state.verified.valid_records;
                }
                state.verified.valid_size += static_cast<std::uint64_t>(pending_line.size() + 1);
                append_to_trailing(pending_line);
                append_to_trailing("\n");
                pending_line.clear();
            }
            offset = newline + 1;
        }
    }

    if (!in.eof() && in.fail()) {
        throw std::runtime_error("failed while reading active log for recovery: " + path);
    }

    if (!pending_line.empty()) {
        state.verified.clean_end = false;
    }
    return state;
}

StreamedVerifiedLogState scan_log_file_streaming(const std::string& path, std::size_t trailing_capacity) {
    return scan_log_file_streaming(path, 0, 0, trailing_capacity);
}

std::string read_tail_before_offset(const std::string& path, std::uint64_t offset, std::size_t tail_size) {
    if (tail_size == 0) {
        return {};
    }
    if (offset < tail_size) {
        throw std::runtime_error("checkpoint tail size is larger than checkpoint logical size");
    }

    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("failed to open active log for checkpoint tail recovery: " + path);
    }
    in.seekg(static_cast<std::streamoff>(offset - tail_size));
    std::string tail(tail_size, '\0');
    in.read(tail.data(), static_cast<std::streamsize>(tail.size()));
    if (static_cast<std::size_t>(in.gcount()) != tail.size()) {
        throw std::runtime_error("failed to read checkpoint tail from active log: " + path);
    }
    return tail;
}

}

const char* recovery_fallback_reason_to_string(RecoveryFallbackReason reason) noexcept {
    switch (reason) {
    case RecoveryFallbackReason::none:
        return "none";
    case RecoveryFallbackReason::incomplete_checkpoint:
        return "incomplete_checkpoint";
    case RecoveryFallbackReason::stale_checkpoint:
        return "stale_checkpoint";
    }
    return "none";
}

seastar::future<> LogManager::prepare(const EngineConfig& config) {
    return seastar::async([config] {
        std::filesystem::create_directories(config.log_dir);
        if (config.archive_features_enabled()) {
            std::filesystem::create_directories(config.archive_dir);
        }
        cleanup_temporary_sidecars(config);
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
                record_gzip_archive_failure();
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
                const auto body = checkpoint_body(checkpoint);
                out << body;
                out << "checkpoint_crc=" << crc32(body) << "\n";
                out.flush();
                if (!out.good()) {
                    throw std::runtime_error("failed to write checkpoint");
                }
            }
            fsync_file(tmp_path);
            std::filesystem::rename(tmp_path, final_path);
            fsync_directory(std::filesystem::path(final_path).parent_path());
            ++g_checkpoint_write_successes;
        } catch (...) {
            remove_file_if_exists(layout::checkpoint_path(active_segment) + ".tmp");
            ++g_checkpoint_write_failures;
            record_checkpoint_write_failure();
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

        const auto checkpoint_path = layout::checkpoint_path(active_segment);
        const bool checkpoint_file_exists = fs::exists(checkpoint_path);
        const auto checkpoint = read_checkpoint_file(active_segment);
        if (checkpoint) {
            const auto file_size = fs::file_size(active_segment.path);
            if (file_size >= checkpoint->logical_size) {
                const auto trailing_capacity = alignment == 0 ? std::size_t{0} : alignment;
                const auto streamed = scan_log_file_streaming(
                    active_segment.path,
                    checkpoint->logical_size,
                    checkpoint->sequence,
                    trailing_capacity);
                const auto& verified = streamed.verified;
                recovery.logical_size = verified.valid_size;
                recovery.sequence = verified.next_sequence;
                recovery.rotation_index = checkpoint->rotation_index;
                const auto tail_size = alignment == 0 ? 0 : static_cast<std::size_t>(recovery.logical_size % alignment);
                if (tail_size > 0) {
                    if (streamed.trailing_bytes.size() >= tail_size) {
                        recovery.tail_buffer = streamed.trailing_bytes.substr(streamed.trailing_bytes.size() - tail_size);
                    } else {
                        recovery.tail_buffer = read_tail_before_offset(active_segment.path, recovery.logical_size, tail_size);
                    }
                }
                return recovery;
            }

            ++g_recovery_fallbacks;
            ++g_recovery_fallback_stale_checkpoint;
            record_recovery_fallback();
            g_last_recovery_fallback_reason.store(
                static_cast<int>(RecoveryFallbackReason::stale_checkpoint),
                std::memory_order_relaxed);
            mgrlog.warn(
                "recovery fallback: checkpoint logical_size={} exceeds active log file size={}",
                checkpoint->logical_size,
                file_size);
        } else if (checkpoint_file_exists) {
            ++g_recovery_fallbacks;
            ++g_recovery_fallback_incomplete_checkpoint;
            record_recovery_fallback();
            g_last_recovery_fallback_reason.store(
                static_cast<int>(RecoveryFallbackReason::incomplete_checkpoint),
                std::memory_order_relaxed);
            mgrlog.warn("recovery fallback: checkpoint file {} exists but is incomplete, corrupted, or unsupported", checkpoint_path);
        }

        const auto trailing_capacity = alignment == 0 ? std::size_t{0} : alignment;
        const auto streamed = scan_log_file_streaming(active_segment.path, trailing_capacity);
        const auto& verified = streamed.verified;
        auto valid_size = verified.valid_size;
        auto sequence = verified.next_sequence;
        auto rotation_index = std::uint64_t{0};

        recovery.logical_size = valid_size;
        recovery.sequence = sequence;
        recovery.rotation_index = rotation_index;

        if (valid_size == 0) {
            return recovery;
        }

        const auto tail_size = alignment == 0 ? 0 : static_cast<std::size_t>(valid_size % alignment);
        if (tail_size > 0) {
            if (streamed.trailing_bytes.size() < tail_size) {
                throw std::runtime_error("recovery trailing buffer shorter than expected valid tail");
            }
            recovery.tail_buffer = streamed.trailing_bytes.substr(streamed.trailing_bytes.size() - tail_size);
        }
        return recovery;
    });
}

void LogManager::cleanup_archives(const EngineConfig& config, unsigned shard_id) {
    namespace fs = std::filesystem;
    const auto now = fs::file_time_type::clock::now();
    auto archived = layout::collect_archive_segments(config, shard_id);

    std::size_t retention_deleted = 0;
    archived.erase(std::remove_if(archived.begin(), archived.end(), [&] (const auto& segment) {
        if (config.archive_retention_seconds > 0) {
            const auto age = std::chrono::duration_cast<std::chrono::seconds>(now - fs::last_write_time(segment.path)).count();
            if (age > static_cast<long long>(config.archive_retention_seconds)) {
                mgrlog.info("archive cleanup (retention): removing aged-out segment shard={} path={} age={}s", shard_id, segment.path, age);
                fs::remove(segment.path);
                ++retention_deleted;
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

    std::size_t count_deleted = 0;
    for (std::size_t i = config.max_archived_files_per_shard; i < archived.size(); ++i) {
        mgrlog.info("archive cleanup (count): removing excess segment shard={} path={}", shard_id, archived[i].path);
        fs::remove(archived[i].path);
        ++count_deleted;
    }
    if (retention_deleted > 0 || count_deleted > 0) {
        mgrlog.info("archive cleanup shard={}: removed {} retention + {} count-excess segments, {} remaining", shard_id, retention_deleted, count_deleted, archived.size() - count_deleted);
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
        .recovery_fallback_incomplete_checkpoint = g_recovery_fallback_incomplete_checkpoint.load(std::memory_order_relaxed),
        .recovery_fallback_stale_checkpoint = g_recovery_fallback_stale_checkpoint.load(std::memory_order_relaxed),
    };
}

RecoveryFallbackReason get_last_recovery_fallback_reason() noexcept {
    return static_cast<RecoveryFallbackReason>(
        g_last_recovery_fallback_reason.load(std::memory_order_relaxed));
}

void reset_log_manager_stats() noexcept {
    g_rotate_operations.store(0, std::memory_order_relaxed);
    g_checkpoint_write_successes.store(0, std::memory_order_relaxed);
    g_checkpoint_write_failures.store(0, std::memory_order_relaxed);
    g_recovery_fallbacks.store(0, std::memory_order_relaxed);
    g_gzip_archive_successes.store(0, std::memory_order_relaxed);
    g_gzip_archive_failures.store(0, std::memory_order_relaxed);
    g_recovery_fallback_incomplete_checkpoint.store(0, std::memory_order_relaxed);
    g_recovery_fallback_stale_checkpoint.store(0, std::memory_order_relaxed);
    g_last_recovery_fallback_reason.store(
        static_cast<int>(RecoveryFallbackReason::none),
        std::memory_order_relaxed);
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
    definitions.reserve(8);
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
    definitions.push_back(sm::make_counter("recovery_fallback_incomplete_checkpoint", sm::description("Recovery fallbacks caused by incomplete or truncated checkpoint files"), [] {
            return g_recovery_fallback_incomplete_checkpoint.load(std::memory_order_relaxed);
        })(component));
    definitions.push_back(sm::make_counter("recovery_fallback_stale_checkpoint", sm::description("Recovery fallbacks caused by checkpoint files with size mismatch"), [] {
            return g_recovery_fallback_stale_checkpoint.load(std::memory_order_relaxed);
        })(component));
    g_log_manager_metrics->add_group("log_engine_log_manager", definitions);
}

void unregister_log_manager_metrics() noexcept {
    if (g_log_manager_metrics) {
        g_log_manager_metrics->clear();
    }
}

}  // namespace log_engine
