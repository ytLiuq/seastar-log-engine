#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <functional>
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <boost/program_options.hpp>
#include <fmt/format.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/reply.hh>
#include <seastar/net/api.hh>
#include <seastar/util/defer.hh>

#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "log_engine/agent_support.hh"
#include "log_engine/config_loader.hh"
#include "log_engine/health_monitor.hh"
#include "log_engine/log_engine.hh"
#include "log_engine/log_manager.hh"
#include "log_engine/log_reader.hh"

namespace {

std::string json_escape(std::string_view value) {
    std::string escaped;
    escaped.reserve(value.size() + 8);
    for (const char ch : value) {
        switch (ch) {
        case '\\':
            escaped += "\\\\";
            break;
        case '"':
            escaped += "\\\"";
            break;
        case '\n':
            escaped += "\\n";
            break;
        case '\r':
            escaped += "\\r";
            break;
        case '\t':
            escaped += "\\t";
            break;
        default:
            escaped.push_back(ch);
            break;
        }
    }
    return escaped;
}

std::optional<std::string> extract_json_string(std::string_view body, std::string_view key) {
    const auto quoted_key = fmt::format("\"{}\"", key);
    auto pos = body.find(quoted_key);
    if (pos == std::string_view::npos) {
        return std::nullopt;
    }
    pos = body.find(':', pos + quoted_key.size());
    if (pos == std::string_view::npos) {
        return std::nullopt;
    }
    ++pos;
    while (pos < body.size() && (body[pos] == ' ' || body[pos] == '\t' || body[pos] == '\n' || body[pos] == '\r')) {
        ++pos;
    }
    if (pos >= body.size() || body[pos] != '"') {
        return std::nullopt;
    }
    ++pos;

    std::string value;
    while (pos < body.size()) {
        const char ch = body[pos++];
        if (ch == '"') {
            return value;
        }
        if (ch != '\\') {
            value.push_back(ch);
            continue;
        }
        if (pos >= body.size()) {
            break;
        }
        const char escaped = body[pos++];
        switch (escaped) {
        case '"':
        case '\\':
        case '/':
            value.push_back(escaped);
            break;
        case 'n':
            value.push_back('\n');
            break;
        case 'r':
            value.push_back('\r');
            break;
        case 't':
            value.push_back('\t');
            break;
        default:
            value.push_back(escaped);
            break;
        }
    }
    return std::nullopt;
}

log_engine::LogLevel parse_level_or_default(std::string_view value) {
    if (value == "warn" || value == "warning") {
        return log_engine::LogLevel::warn;
    }
    if (value == "error" || value == "err") {
        return log_engine::LogLevel::error;
    }
    return log_engine::LogLevel::info;
}

log_engine::LogMessage parse_ingest_message(std::string_view body) {
    log_engine::LogMessage message;
    if (auto payload = extract_json_string(body, "payload")) {
        message.payload = std::move(*payload);
    } else if (auto text = extract_json_string(body, "message")) {
        message.payload = std::move(*text);
    } else {
        message.payload = std::string(body);
    }
    if (auto level = extract_json_string(body, "level")) {
        message.level = parse_level_or_default(*level);
    }
    if (auto route_key = extract_json_string(body, "route_key")) {
        message.route_key = std::move(*route_key);
    } else if (auto service = extract_json_string(body, "service")) {
        message.route_key = std::move(*service);
    }
    return message;
}

class StopSignal {
public:
    StopSignal() {
        seastar::engine().handle_signal(SIGINT, [this] {
            signal();
        });
        seastar::engine().handle_signal(SIGTERM, [this] {
            signal();
        });
    }

    seastar::future<> wait() {
        return _stopped.wait([this] {
            return _signaled;
        });
    }

private:
    void signal() {
        if (_signaled) {
            return;
        }
        _signaled = true;
        _stopped.broadcast();
    }

private:
    bool _signaled = false;
    seastar::condition_variable _stopped;
};

struct AgentStats {
    std::atomic<std::uint64_t> accepted{0};
    std::atomic<std::uint64_t> rejected{0};
    std::atomic<std::uint64_t> source_read{0};
    std::atomic<std::uint64_t> source_committed{0};
    std::atomic<std::uint64_t> sink_sent{0};
    std::atomic<std::uint64_t> sink_failed{0};
    std::atomic<std::uint64_t> sink_retries{0};
    std::atomic<std::uint64_t> sink_backlog_records{0};
    std::atomic<std::uint64_t> last_sink_latency_ms{0};
    std::atomic<std::uint64_t> disk_backpressure{0};
    std::atomic<std::uint64_t> dynamic_backpressure{0};
};

struct AgentContext {
    log_engine::LogEngine engine;
    log_engine::EngineConfig engine_config;
    AgentStats stats;
    std::size_t max_request_bytes = 1024 * 1024;

    std::string render_status_json() const {
        const auto health_snapshot = log_engine::collect_health_snapshot();
        const auto health = log_engine::compute_health_status(health_snapshot);
        const auto manager_stats = log_engine::get_log_manager_stats();
        return fmt::format(
            "{{\"health\":\"{}\",\"accepted\":{},\"rejected\":{},\"source_read\":{},\"source_committed\":{},\"sink_sent\":{},\"sink_failed\":{},\"sink_retries\":{},\"sink_backlog_records\":{},\"last_sink_latency_ms\":{},\"disk_backpressure\":{},\"dynamic_backpressure\":{},\"checkpoint_write_successes\":{},\"checkpoint_write_failures\":{},\"recovery_fallbacks\":{},\"recovery_from_checkpoints\":{},\"recovery_full_scans\":{},\"recovery_empty_files\":{},\"last_recovery_fallback_reason\":\"{}\"}}",
            log_engine::health_status_to_string(health),
            stats.accepted.load(std::memory_order_relaxed),
            stats.rejected.load(std::memory_order_relaxed),
            stats.source_read.load(std::memory_order_relaxed),
            stats.source_committed.load(std::memory_order_relaxed),
            stats.sink_sent.load(std::memory_order_relaxed),
            stats.sink_failed.load(std::memory_order_relaxed),
            stats.sink_retries.load(std::memory_order_relaxed),
            stats.sink_backlog_records.load(std::memory_order_relaxed),
            stats.last_sink_latency_ms.load(std::memory_order_relaxed),
            stats.disk_backpressure.load(std::memory_order_relaxed),
            stats.dynamic_backpressure.load(std::memory_order_relaxed),
            manager_stats.checkpoint_write_successes,
            manager_stats.checkpoint_write_failures,
            manager_stats.recovery_fallbacks,
            manager_stats.recovery_from_checkpoints,
            manager_stats.recovery_full_scans,
            manager_stats.recovery_empty_files,
            log_engine::recovery_fallback_reason_to_string(log_engine::get_last_recovery_fallback_reason()));
    }
};

enum class SinkKind {
    none,
    http,
    stdout,
    kafka,
    object_store,
};

SinkKind parse_sink_kind(std::string_view value) {
    if (value.empty() || value == "none") {
        return SinkKind::none;
    }
    if (value == "http") {
        return SinkKind::http;
    }
    if (value == "stdout") {
        return SinkKind::stdout;
    }
    if (value == "kafka") {
        return SinkKind::kafka;
    }
    if (value == "object_store" || value == "object-store" || value == "s3") {
        return SinkKind::object_store;
    }
    throw std::invalid_argument("sink-kind must be none, http, stdout, kafka, or object_store");
}

struct AgentRuntimeOptions {
    std::string agent_id = "seastar-log-agent";
    std::string file_source_path;
    std::string file_source_glob;
    bool stdin_source_enabled = false;
    std::string unix_socket_path;
    std::uint16_t tcp_source_port = 0;
    std::uint16_t udp_source_port = 0;
    std::string source_offset_path = "agent-source.offset";
    std::size_t source_poll_ms = 1000;
    std::size_t source_max_lines = 1024;
    SinkKind sink_kind = SinkKind::none;
    std::string sink_http_url;
    log_engine::agent::HttpPostOptions sink_http_options;
    std::string delivery_offset_path = "agent-delivery.offset";
    std::string pending_delivery_path = "agent-delivery.pending";
    std::size_t sink_batch_size = 100;
    std::size_t sink_retry_backoff_ms = 1000;
    std::size_t sink_retry_max_backoff_ms = 30000;
    std::uint64_t max_sink_backlog_records = 0;
    std::uint64_t max_recent_sink_failures = 0;
    std::uint64_t max_sink_latency_ms = 0;
    log_engine::agent::DiskQuota disk_quota;
};

class FdGuard {
public:
    explicit FdGuard(int fd = -1) noexcept : _fd(fd) {}
    FdGuard(const FdGuard&) = delete;
    FdGuard& operator=(const FdGuard&) = delete;
    ~FdGuard() {
        if (_fd >= 0) {
            ::close(_fd);
        }
    }
    int get() const noexcept {
        return _fd;
    }
    int release() noexcept {
        const int fd = _fd;
        _fd = -1;
        return fd;
    }

private:
    int _fd = -1;
};

void set_nonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw std::runtime_error("failed to set socket nonblocking");
    }
}

std::vector<log_engine::LogMessage> lines_to_messages(const std::vector<std::string>& lines) {
    std::vector<log_engine::LogMessage> messages;
    messages.reserve(lines.size());
    for (const auto& line : lines) {
        log_engine::LogMessage message;
        message.payload = line;
        messages.push_back(std::move(message));
    }
    return messages;
}

std::string source_offset_path_for(const AgentRuntimeOptions& options, const std::string& source_path) {
    const auto hash = std::hash<std::string>{}(source_path);
    return fmt::format("{}.{}", options.source_offset_path, hash);
}

log_engine::agent::BackpressureDecision evaluate_agent_backpressure(
    const AgentContext& context,
    const AgentRuntimeOptions& options) {
    log_engine::agent::BackpressureState state;
    state.sink_backlog_records = context.stats.sink_backlog_records.load(std::memory_order_relaxed);
    state.recent_sink_failures = context.stats.sink_failed.load(std::memory_order_relaxed);
    state.last_sink_latency_ms = context.stats.last_sink_latency_ms.load(std::memory_order_relaxed);
    state.max_sink_backlog_records = options.max_sink_backlog_records;
    state.max_recent_sink_failures = options.max_recent_sink_failures;
    state.max_sink_latency_ms = options.max_sink_latency_ms;
    return log_engine::agent::evaluate_backpressure(context.engine_config.log_dir, options.disk_quota, state);
}

void append_source_lines(
    AgentContext& context,
    const AgentRuntimeOptions& options,
    const std::vector<std::string>& lines) {
    if (lines.empty()) {
        return;
    }
    const auto backpressure = evaluate_agent_backpressure(context, options);
    if (backpressure.pause) {
        if (backpressure.reason == "disk_quota") {
            ++context.stats.disk_backpressure;
        } else {
            ++context.stats.dynamic_backpressure;
        }
        throw std::runtime_error("agent source paused by backpressure: " + backpressure.reason);
    }

    context.stats.source_read.fetch_add(lines.size(), std::memory_order_relaxed);
    context.engine.append_batch(lines_to_messages(lines)).get();
    context.stats.source_committed.fetch_add(lines.size(), std::memory_order_relaxed);
}

seastar::future<> run_file_source_loop(
    AgentContext& context,
    AgentRuntimeOptions options,
    std::atomic<bool>& stopping) {
    if (options.file_source_path.empty() && options.file_source_glob.empty()) {
        return seastar::make_ready_future<>();
    }

    return seastar::async([&context, options = std::move(options), &stopping] {
        std::map<std::string, log_engine::agent::SourceOffset> offsets;
        while (!stopping.load(std::memory_order_relaxed)) {
            try {
                std::vector<std::string> paths;
                if (!options.file_source_path.empty()) {
                    paths.push_back(options.file_source_path);
                }
                const auto glob_paths = log_engine::agent::expand_glob_paths(options.file_source_glob);
                paths.insert(paths.end(), glob_paths.begin(), glob_paths.end());
                std::sort(paths.begin(), paths.end());
                paths.erase(std::unique(paths.begin(), paths.end()), paths.end());

                bool consumed = false;
                for (const auto& path : paths) {
                    auto& offset = offsets[path];
                    if (offset.path.empty()) {
                        if (auto loaded = log_engine::agent::load_source_offset(source_offset_path_for(options, path))) {
                            offset = *loaded;
                        }
                    }
                    const auto previous = offset.path.empty() ? std::optional<log_engine::agent::SourceOffset>{} : std::optional(offset);
                    auto batch = log_engine::agent::tail_file_once(path, previous, options.source_max_lines);
                    if (batch.lines.empty()) {
                        offset = batch.next_offset;
                        continue;
                    }
                    append_source_lines(context, options, batch.lines);
                    log_engine::agent::store_source_offset(source_offset_path_for(options, path), batch.next_offset);
                    offset = batch.next_offset;
                    consumed = true;
                }

                if (!consumed) {
                    seastar::sleep(std::chrono::milliseconds(options.source_poll_ms)).get();
                }
            } catch (...) {
                ++context.stats.rejected;
                seastar::sleep(std::chrono::milliseconds(options.source_poll_ms)).get();
            }
        }
    });
}

seastar::future<> run_stdin_source_loop(
    AgentContext& context,
    AgentRuntimeOptions options,
    std::atomic<bool>& stopping) {
    if (!options.stdin_source_enabled) {
        return seastar::make_ready_future<>();
    }

    return seastar::async([&context, options = std::move(options), &stopping] {
        std::vector<std::string> lines;
        lines.reserve(options.source_max_lines);
        std::string line;
        while (!stopping.load(std::memory_order_relaxed) && std::getline(std::cin, line)) {
            lines.push_back(line);
            if (lines.size() >= options.source_max_lines) {
                append_source_lines(context, options, lines);
                lines.clear();
            }
        }
        if (!lines.empty()) {
            append_source_lines(context, options, lines);
        }
    });
}

void consume_line_stream_fd(
    int fd,
    AgentContext& context,
    const AgentRuntimeOptions& options,
    std::atomic<bool>& stopping) {
    FdGuard guard(fd);
    std::string pending;
    char buffer[4096];
    while (!stopping.load(std::memory_order_relaxed)) {
        const auto rc = ::recv(guard.get(), buffer, sizeof(buffer), 0);
        if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            seastar::sleep(std::chrono::milliseconds(10)).get();
            continue;
        }
        if (rc <= 0) {
            break;
        }
        pending.append(buffer, static_cast<std::size_t>(rc));
        std::size_t start = 0;
        std::vector<std::string> lines;
        while (true) {
            const auto newline = pending.find('\n', start);
            if (newline == std::string::npos) {
                break;
            }
            auto line = pending.substr(start, newline - start);
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            lines.push_back(std::move(line));
            start = newline + 1;
            if (lines.size() >= options.source_max_lines) {
                append_source_lines(context, options, lines);
                lines.clear();
            }
        }
        if (!lines.empty()) {
            append_source_lines(context, options, lines);
        }
        pending.erase(0, start);
    }
}

seastar::future<> run_tcp_source_loop(
    AgentContext& context,
    AgentRuntimeOptions options,
    std::atomic<bool>& stopping) {
    if (options.tcp_source_port == 0) {
        return seastar::make_ready_future<>();
    }

    return seastar::async([&context, options = std::move(options), &stopping] {
        FdGuard server(::socket(AF_INET, SOCK_STREAM, 0));
        if (server.get() < 0) {
            throw std::runtime_error("failed to create TCP source socket");
        }
        set_nonblocking(server.get());
        int reuse = 1;
        ::setsockopt(server.get(), SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in address {};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_ANY);
        address.sin_port = htons(options.tcp_source_port);
        if (::bind(server.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 || ::listen(server.get(), 16) != 0) {
            throw std::runtime_error("failed to bind/listen TCP source socket");
        }
        while (!stopping.load(std::memory_order_relaxed)) {
            const int client = ::accept(server.get(), nullptr, nullptr);
            if (client < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    seastar::sleep(std::chrono::milliseconds(50)).get();
                }
                continue;
            }
            set_nonblocking(client);
            try {
                consume_line_stream_fd(client, context, options, stopping);
            } catch (...) {
                ++context.stats.rejected;
                ::close(client);
            }
        }
    });
}

seastar::future<> run_udp_source_loop(
    AgentContext& context,
    AgentRuntimeOptions options,
    std::atomic<bool>& stopping) {
    if (options.udp_source_port == 0) {
        return seastar::make_ready_future<>();
    }

    return seastar::async([&context, options = std::move(options), &stopping] {
        FdGuard server(::socket(AF_INET, SOCK_DGRAM, 0));
        if (server.get() < 0) {
            throw std::runtime_error("failed to create UDP source socket");
        }
        set_nonblocking(server.get());
        sockaddr_in address {};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_ANY);
        address.sin_port = htons(options.udp_source_port);
        if (::bind(server.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
            throw std::runtime_error("failed to bind UDP source socket");
        }
        char buffer[65535];
        while (!stopping.load(std::memory_order_relaxed)) {
            const auto rc = ::recv(server.get(), buffer, sizeof(buffer), 0);
            if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                seastar::sleep(std::chrono::milliseconds(50)).get();
                continue;
            }
            if (rc <= 0) {
                continue;
            }
            append_source_lines(context, options, {std::string(buffer, static_cast<std::size_t>(rc))});
        }
    });
}

seastar::future<> run_unix_socket_source_loop(
    AgentContext& context,
    AgentRuntimeOptions options,
    std::atomic<bool>& stopping) {
    if (options.unix_socket_path.empty()) {
        return seastar::make_ready_future<>();
    }

    return seastar::async([&context, options = std::move(options), &stopping] {
        ::unlink(options.unix_socket_path.c_str());
        FdGuard server(::socket(AF_UNIX, SOCK_STREAM, 0));
        if (server.get() < 0) {
            throw std::runtime_error("failed to create Unix source socket");
        }
        set_nonblocking(server.get());
        sockaddr_un address {};
        address.sun_family = AF_UNIX;
        if (options.unix_socket_path.size() >= sizeof(address.sun_path)) {
            throw std::runtime_error("unix-socket-source-path is too long");
        }
        std::strncpy(address.sun_path, options.unix_socket_path.c_str(), sizeof(address.sun_path) - 1);
        if (::bind(server.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 || ::listen(server.get(), 16) != 0) {
            throw std::runtime_error("failed to bind/listen Unix source socket");
        }
        while (!stopping.load(std::memory_order_relaxed)) {
            const int client = ::accept(server.get(), nullptr, nullptr);
            if (client < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    seastar::sleep(std::chrono::milliseconds(50)).get();
                }
                continue;
            }
            set_nonblocking(client);
            try {
                consume_line_stream_fd(client, context, options, stopping);
            } catch (...) {
                ++context.stats.rejected;
                ::close(client);
            }
        }
        ::unlink(options.unix_socket_path.c_str());
    });
}

seastar::future<> deliver_batch(
    const AgentRuntimeOptions& options,
    const std::optional<log_engine::agent::HttpEndpoint>& endpoint,
    const log_engine::agent::DeliveryBatch& batch) {
    switch (options.sink_kind) {
    case SinkKind::none:
        return seastar::make_ready_future<>();
    case SinkKind::stdout:
        log_engine::agent::write_stdout_batch(batch.records);
        return seastar::make_ready_future<>();
    case SinkKind::http:
        if (!endpoint) {
            throw std::runtime_error("sink-kind=http requires sink-http-url");
        }
        return log_engine::agent::post_http_batch_async(
            *endpoint,
            log_engine::agent::render_delivery_batch_json(options.agent_id, batch),
            options.sink_http_options);
    case SinkKind::kafka:
        throw std::runtime_error("sink-kind=kafka is configured but Kafka sink is not linked in this build");
    case SinkKind::object_store:
        throw std::runtime_error("sink-kind=object_store is configured but object store sink is not linked in this build");
    }
    return seastar::make_ready_future<>();
}

seastar::future<> run_http_sink_loop(
    AgentContext& context,
    AgentRuntimeOptions options,
    std::atomic<bool>& stopping) {
    if (options.sink_kind == SinkKind::none) {
        return seastar::make_ready_future<>();
    }
    auto endpoint = log_engine::agent::parse_http_endpoint(options.sink_http_url);
    if (options.sink_kind == SinkKind::http && !endpoint) {
        throw std::invalid_argument("sink-kind=http requires a valid http:// sink-http-url");
    }

    return seastar::async([&context, options = std::move(options), endpoint, &stopping] {
        std::map<unsigned, std::uint64_t> next_by_shard;
        for (const auto& offset : log_engine::agent::load_delivery_offsets(options.delivery_offset_path)) {
            next_by_shard[offset.shard] = offset.next_sequence;
        }
        for (unsigned shard = 0; shard < seastar::smp::count; ++shard) {
            next_by_shard.try_emplace(shard, 0);
        }

        auto pending = log_engine::agent::load_pending_delivery_batch(options.pending_delivery_path);
        std::size_t current_backoff_ms = options.sink_retry_backoff_ms;
        while (!stopping.load(std::memory_order_relaxed)) {
            try {
                if (!pending) {
                    for (unsigned shard = 0; shard < seastar::smp::count && !pending; ++shard) {
                        log_engine::ReadQuery query;
                        query.include_archive = true;
                        query.limit = options.sink_batch_size;
                        query.shard = shard;
                        query.seq_from = next_by_shard[shard];
                        const auto segments = log_engine::collect_segments(context.engine_config, query);
                        const auto records = log_engine::read_records(segments, query);
                        if (records.empty()) {
                            continue;
                        }

                        log_engine::agent::DeliveryBatch batch;
                        batch.shard = shard;
                        batch.first_sequence = next_by_shard[shard];
                        batch.next_sequence = next_by_shard[shard];
                        batch.records.reserve(records.size());
                        for (const auto& record : records) {
                            batch.records.push_back(record.payload);
                            if (record.has_sequence) {
                                batch.next_sequence = std::max(batch.next_sequence, record.sequence + 1);
                            }
                        }
                        pending = std::move(batch);
                        log_engine::agent::store_pending_delivery_batch(options.pending_delivery_path, *pending);
                        context.stats.sink_backlog_records.store(pending->records.size(), std::memory_order_relaxed);
                    }
                }

                if (!pending) {
                    context.stats.sink_backlog_records.store(0, std::memory_order_relaxed);
                    seastar::sleep(std::chrono::milliseconds(options.sink_retry_backoff_ms)).get();
                    continue;
                }

                const auto start = std::chrono::steady_clock::now();
                deliver_batch(options, endpoint, *pending).get();
                const auto end = std::chrono::steady_clock::now();
                context.stats.last_sink_latency_ms.store(
                    static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()),
                    std::memory_order_relaxed);

                next_by_shard[pending->shard] = pending->next_sequence;
                std::vector<log_engine::agent::DeliveryOffset> offsets;
                offsets.reserve(next_by_shard.size());
                for (const auto& [shard, next] : next_by_shard) {
                    offsets.push_back(log_engine::agent::DeliveryOffset{.shard = shard, .next_sequence = next});
                }
                log_engine::agent::store_delivery_offsets(options.delivery_offset_path, offsets);
                log_engine::agent::remove_pending_delivery_batch(options.pending_delivery_path);
                context.stats.sink_sent.fetch_add(pending->records.size(), std::memory_order_relaxed);
                context.stats.sink_backlog_records.store(0, std::memory_order_relaxed);
                pending.reset();
                current_backoff_ms = options.sink_retry_backoff_ms;
            } catch (...) {
                ++context.stats.sink_failed;
                ++context.stats.sink_retries;
                seastar::sleep(std::chrono::milliseconds(current_backoff_ms)).get();
                current_backoff_ms = std::min(current_backoff_ms * 2, options.sink_retry_max_backoff_ms);
            }
        }
    });
}

void set_agent_routes(seastar::httpd::routes& routes, AgentContext& context) {
    using namespace seastar::httpd;

    routes.add(operation_type::GET, url("/healthz"), new function_handler([](const_req) {
        return seastar::sstring("{\"ok\":true}");
    }, "json"));

    routes.add(operation_type::GET, url("/v1/status"), new function_handler([&context](const_req) {
        return seastar::sstring(context.render_status_json());
    }, "json"));

    future_handler_function ingest_handler = [&context](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
        return seastar::do_with(std::move(req->content), std::move(rep), [&context](seastar::sstring& body, std::unique_ptr<seastar::http::reply>& response) {
            if (body.empty()) {
                ++context.stats.rejected;
                response->set_status(seastar::http::reply::status_type::bad_request);
                response->write_body("json", "{\"error\":\"empty request body\"}");
                return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(response));
            }
            if (body.size() > context.max_request_bytes) {
                ++context.stats.rejected;
                response->set_status(seastar::http::reply::status_type::payload_too_large);
                response->write_body("json", "{\"error\":\"request body too large\"}");
                return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(response));
            }

            auto message = parse_ingest_message(std::string_view(body.data(), body.size()));
            return context.engine.append(std::move(message)).then([&context, &response] {
                ++context.stats.accepted;
                response->write_body("json", "{\"accepted\":1}");
                return std::move(response);
            }).handle_exception([&context, &response](std::exception_ptr ep) {
                ++context.stats.rejected;
                response->set_status(seastar::http::reply::status_type::service_unavailable);
                try {
                    std::rethrow_exception(ep);
                } catch (const std::exception& ex) {
                    response->write_body("json", fmt::format("{{\"error\":\"{}\"}}", json_escape(ex.what())));
                }
                return std::move(response);
            });
        });
    };
    routes.add(operation_type::POST, url("/v1/logs"), new function_handler(ingest_handler, "json"));
}

}  // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    namespace bpo = boost::program_options;

    app.add_options()
        ("config", bpo::value<std::string>()->default_value(""), "Path to key=value config file")
        ("log-dir", bpo::value<std::string>()->default_value("agent-logs"), "Directory for shard log files")
        ("archive-dir", bpo::value<std::string>()->default_value("agent-archive"), "Directory for archived log files")
        ("shard-file-prefix", bpo::value<std::string>()->default_value("shard"), "Shard log file prefix")
        ("ack-mode", bpo::value<std::string>()->default_value("sync_ack"), "Agent ingest ack mode: write_ack or sync_ack")
        ("routing-strategy", bpo::value<std::string>()->default_value("hash_modulo"), "Routing strategy")
        ("empty-route-policy", bpo::value<std::string>()->default_value("round_robin"), "Empty route policy")
        ("routing-virtual-nodes", bpo::value<std::size_t>()->default_value(128), "Virtual nodes per shard for consistent hashing")
        ("batch-size", bpo::value<std::size_t>()->default_value(32), "Writer batch size")
        ("flush-ms", bpo::value<std::size_t>()->default_value(100), "Periodic flush interval")
        ("stream-buffer-size", bpo::value<std::size_t>()->default_value(64 * 1024), "Stream buffer size")
        ("write-behind", bpo::value<std::size_t>()->default_value(8), "Concurrent DMA writes per flush")
        ("write-retry-count", bpo::value<std::size_t>()->default_value(3), "Write retry count")
        ("write-retry-backoff-ms", bpo::value<std::size_t>()->default_value(2), "Write retry backoff")
        ("max-pending-bytes", bpo::value<std::size_t>()->default_value(64 * 1024 * 1024), "Backpressure high watermark")
        ("pending-bytes-low-watermark", bpo::value<std::size_t>()->default_value(32 * 1024 * 1024), "Backpressure resume watermark")
        ("rotate-size-bytes", bpo::value<std::uint64_t>()->default_value(256ULL * 1024ULL * 1024ULL), "Rotate active log at this size")
        ("rotate-interval-seconds", bpo::value<std::uint64_t>()->default_value(0), "Rotate active log after this many seconds")
        ("archive-retention-seconds", bpo::value<std::uint64_t>()->default_value(0), "Archive retention")
        ("max-archived-files", bpo::value<std::size_t>()->default_value(32), "Max archived files per shard")
        ("truncate-on-start", bpo::value<bool>()->default_value(false), "Truncate active log on start")
        ("checkpoint-enabled", bpo::value<bool>()->default_value(true), "Enable checkpoint recovery")
        ("compress-archives", bpo::value<bool>()->default_value(false), "Compress archives")
        ("dsync", bpo::value<bool>()->default_value(false), "Open active log with O_DSYNC")
        ("record-crc-enabled", bpo::value<bool>()->default_value(true), "Enable record CRC")
        ("record-crc-class", bpo::value<std::string>()->default_value("full"), "CRC class")
        ("record-timestamp-enabled", bpo::value<bool>()->default_value(true), "Include timestamp")
        ("record-level-enabled", bpo::value<bool>()->default_value(true), "Include level")
        ("record-shard-id-enabled", bpo::value<bool>()->default_value(true), "Include shard id")
        ("record-sequence-enabled", bpo::value<bool>()->default_value(true), "Include sequence")
        ("http-ingest-address", bpo::value<std::string>()->default_value("0.0.0.0"), "HTTP ingest listen address")
        ("http-ingest-port", bpo::value<uint16_t>()->default_value(18081), "HTTP ingest listen port")
        ("max-request-bytes", bpo::value<std::size_t>()->default_value(1024 * 1024), "Max HTTP ingest request body bytes")
        ("agent-id", bpo::value<std::string>()->default_value("seastar-log-agent"), "Stable agent id included in sink delivery batches")
        ("file-source-path", bpo::value<std::string>()->default_value(""), "Optional file source to tail")
        ("file-source-glob", bpo::value<std::string>()->default_value(""), "Optional glob pattern for multiple file sources")
        ("stdin-source-enabled", bpo::value<bool>()->default_value(false), "Read newline-delimited logs from stdin")
        ("unix-socket-source-path", bpo::value<std::string>()->default_value(""), "Unix stream socket source path")
        ("tcp-source-port", bpo::value<std::uint16_t>()->default_value(0), "TCP newline-delimited source port")
        ("udp-source-port", bpo::value<std::uint16_t>()->default_value(0), "UDP datagram source port")
        ("source-offset-path", bpo::value<std::string>()->default_value("agent-source.offset"), "File source offset checkpoint")
        ("source-poll-ms", bpo::value<std::size_t>()->default_value(1000), "File source poll interval")
        ("source-max-lines", bpo::value<std::size_t>()->default_value(1024), "Max source lines per poll")
        ("sink-kind", bpo::value<std::string>()->default_value("none"), "Sink kind: none, http, stdout, kafka, object_store")
        ("sink-http-url", bpo::value<std::string>()->default_value(""), "Optional HTTP sink URL, e.g. http://127.0.0.1:9000/ingest")
        ("sink-http-headers", bpo::value<std::string>()->default_value(""), "Semicolon-separated HTTP sink headers, e.g. Authorization: Bearer token; X-Agent: edge")
        ("sink-http-timeout-ms", bpo::value<std::uint64_t>()->default_value(5000), "HTTP sink request timeout in milliseconds, 0 disables timeout")
        ("sink-http-retryable-statuses", bpo::value<std::string>()->default_value("408,425,429,500,502,503,504"), "Comma-separated HTTP sink status codes treated as retryable")
        ("delivery-offset-path", bpo::value<std::string>()->default_value("agent-delivery.offset"), "HTTP sink delivery offset checkpoint")
        ("pending-delivery-path", bpo::value<std::string>()->default_value("agent-delivery.pending"), "Durable pending sink batch file")
        ("sink-batch-size", bpo::value<std::size_t>()->default_value(100), "HTTP sink batch size")
        ("sink-retry-backoff-ms", bpo::value<std::size_t>()->default_value(1000), "HTTP sink idle/retry backoff")
        ("sink-retry-max-backoff-ms", bpo::value<std::size_t>()->default_value(30000), "HTTP sink max retry backoff")
        ("max-sink-backlog-records", bpo::value<std::uint64_t>()->default_value(0), "Pause sources when sink backlog reaches this many records")
        ("max-recent-sink-failures", bpo::value<std::uint64_t>()->default_value(0), "Pause sources after this many sink failures")
        ("max-sink-latency-ms", bpo::value<std::uint64_t>()->default_value(0), "Pause sources when latest sink latency reaches this value")
        ("max-buffer-bytes", bpo::value<std::uint64_t>()->default_value(0), "Pause file source when log buffer reaches this size")
        ("resume-buffer-bytes", bpo::value<std::uint64_t>()->default_value(0), "Resume file source when log buffer drops to this size");

    return app.run(argc, argv, [&app] {
        return seastar::async([&app] {
            StopSignal stop_signal;
            auto& options = app.configuration();
            const auto file_values = log_engine::load_config_file(options["config"].as<std::string>());

            log_engine::EngineConfig base;
            base.log_dir = options["log-dir"].as<std::string>();
            base.archive_dir = options["archive-dir"].as<std::string>();
            base.shard_file_prefix = options["shard-file-prefix"].as<std::string>();
            base.ack_mode = log_engine::parse_ack_mode(options["ack-mode"].as<std::string>());
            base.routing_strategy = log_engine::parse_routing_strategy(options["routing-strategy"].as<std::string>());
            base.empty_route_policy = log_engine::parse_empty_route_policy(options["empty-route-policy"].as<std::string>());
            base.routing_virtual_nodes = options["routing-virtual-nodes"].as<std::size_t>();
            base.batch_size = options["batch-size"].as<std::size_t>();
            base.flush_interval_ms = options["flush-ms"].as<std::size_t>();
            base.stream_buffer_size = options["stream-buffer-size"].as<std::size_t>();
            base.write_behind = options["write-behind"].as<std::size_t>();
            base.write_retry_count = options["write-retry-count"].as<std::size_t>();
            base.write_retry_backoff_ms = options["write-retry-backoff-ms"].as<std::size_t>();
            base.max_pending_bytes = options["max-pending-bytes"].as<std::size_t>();
            base.pending_bytes_low_watermark = options["pending-bytes-low-watermark"].as<std::size_t>();
            base.rotate_size_bytes = options["rotate-size-bytes"].as<std::uint64_t>();
            base.rotate_interval_seconds = options["rotate-interval-seconds"].as<std::uint64_t>();
            base.archive_retention_seconds = options["archive-retention-seconds"].as<std::uint64_t>();
            base.max_archived_files_per_shard = options["max-archived-files"].as<std::size_t>();
            base.truncate_on_start = options["truncate-on-start"].as<bool>();
            base.checkpoint_enabled = options["checkpoint-enabled"].as<bool>();
            base.compress_archives = options["compress-archives"].as<bool>();
            base.use_dsync = options["dsync"].as<bool>();
            base.record_crc_enabled = options["record-crc-enabled"].as<bool>();
            base.record_crc_class = log_engine::parse_crc_class(options["record-crc-class"].as<std::string>());
            base.record_timestamp_enabled = options["record-timestamp-enabled"].as<bool>();
            base.record_level_enabled = options["record-level-enabled"].as<bool>();
            base.record_shard_id_enabled = options["record-shard-id-enabled"].as<bool>();
            base.record_sequence_enabled = options["record-sequence-enabled"].as<bool>();
            auto config = log_engine::apply_engine_config_overrides(base, options, file_values);

            AgentContext context;
            context.engine_config = config;
            context.max_request_bytes = log_engine::resolve_size_option(
                options,
                file_values,
                "max-request-bytes",
                options["max-request-bytes"].as<std::size_t>());

            AgentRuntimeOptions runtime_options;
            runtime_options.agent_id = log_engine::resolve_string_option(
                options, file_values, "agent-id", options["agent-id"].as<std::string>());
            runtime_options.file_source_path = log_engine::resolve_string_option(
                options, file_values, "file-source-path", options["file-source-path"].as<std::string>());
            runtime_options.file_source_glob = log_engine::resolve_string_option(
                options, file_values, "file-source-glob", options["file-source-glob"].as<std::string>());
            runtime_options.stdin_source_enabled = log_engine::resolve_bool_option(
                options, file_values, "stdin-source-enabled", options["stdin-source-enabled"].as<bool>());
            runtime_options.unix_socket_path = log_engine::resolve_string_option(
                options, file_values, "unix-socket-source-path", options["unix-socket-source-path"].as<std::string>());
            runtime_options.tcp_source_port = static_cast<std::uint16_t>(log_engine::resolve_u64_option(
                options, file_values, "tcp-source-port", options["tcp-source-port"].as<std::uint16_t>()));
            runtime_options.udp_source_port = static_cast<std::uint16_t>(log_engine::resolve_u64_option(
                options, file_values, "udp-source-port", options["udp-source-port"].as<std::uint16_t>()));
            runtime_options.source_offset_path = log_engine::resolve_string_option(
                options, file_values, "source-offset-path", options["source-offset-path"].as<std::string>());
            runtime_options.source_poll_ms = log_engine::resolve_size_option(
                options, file_values, "source-poll-ms", options["source-poll-ms"].as<std::size_t>());
            runtime_options.source_max_lines = log_engine::resolve_size_option(
                options, file_values, "source-max-lines", options["source-max-lines"].as<std::size_t>());
            runtime_options.sink_kind = parse_sink_kind(log_engine::resolve_string_option(
                options, file_values, "sink-kind", options["sink-kind"].as<std::string>()));
            runtime_options.sink_http_url = log_engine::resolve_string_option(
                options, file_values, "sink-http-url", options["sink-http-url"].as<std::string>());
            runtime_options.sink_http_options.headers = log_engine::agent::parse_http_headers(
                log_engine::resolve_string_option(
                    options,
                    file_values,
                    "sink-http-headers",
                    options["sink-http-headers"].as<std::string>()));
            runtime_options.sink_http_options.timeout_ms = log_engine::resolve_u64_option(
                options, file_values, "sink-http-timeout-ms", options["sink-http-timeout-ms"].as<std::uint64_t>());
            runtime_options.sink_http_options.retryable_status_codes = log_engine::agent::parse_http_status_codes(
                log_engine::resolve_string_option(
                    options,
                    file_values,
                    "sink-http-retryable-statuses",
                    options["sink-http-retryable-statuses"].as<std::string>()));
            runtime_options.delivery_offset_path = log_engine::resolve_string_option(
                options, file_values, "delivery-offset-path", options["delivery-offset-path"].as<std::string>());
            runtime_options.pending_delivery_path = log_engine::resolve_string_option(
                options, file_values, "pending-delivery-path", options["pending-delivery-path"].as<std::string>());
            runtime_options.sink_batch_size = log_engine::resolve_size_option(
                options, file_values, "sink-batch-size", options["sink-batch-size"].as<std::size_t>());
            runtime_options.sink_retry_backoff_ms = log_engine::resolve_size_option(
                options, file_values, "sink-retry-backoff-ms", options["sink-retry-backoff-ms"].as<std::size_t>());
            runtime_options.sink_retry_max_backoff_ms = log_engine::resolve_size_option(
                options, file_values, "sink-retry-max-backoff-ms", options["sink-retry-max-backoff-ms"].as<std::size_t>());
            runtime_options.max_sink_backlog_records = log_engine::resolve_u64_option(
                options, file_values, "max-sink-backlog-records", options["max-sink-backlog-records"].as<std::uint64_t>());
            runtime_options.max_recent_sink_failures = log_engine::resolve_u64_option(
                options, file_values, "max-recent-sink-failures", options["max-recent-sink-failures"].as<std::uint64_t>());
            runtime_options.max_sink_latency_ms = log_engine::resolve_u64_option(
                options, file_values, "max-sink-latency-ms", options["max-sink-latency-ms"].as<std::uint64_t>());
            runtime_options.disk_quota.max_buffer_bytes = log_engine::resolve_u64_option(
                options, file_values, "max-buffer-bytes", options["max-buffer-bytes"].as<std::uint64_t>());
            runtime_options.disk_quota.resume_buffer_bytes = log_engine::resolve_u64_option(
                options, file_values, "resume-buffer-bytes", options["resume-buffer-bytes"].as<std::uint64_t>());

            log_engine::register_health_metrics();
            auto unregister_health_metrics = seastar::defer([] () noexcept {
                log_engine::unregister_health_metrics();
            });
            log_engine::register_log_manager_metrics();
            auto unregister_manager_metrics = seastar::defer([] () noexcept {
                log_engine::unregister_log_manager_metrics();
            });

            context.engine.start(config).get();
            auto stop_engine = seastar::defer([&context] () noexcept {
                context.engine.stop().get();
            });

            seastar::httpd::http_server_control http_server;
            http_server.start("seastar-log-agent").get();
            auto stop_http = seastar::defer([&http_server] () noexcept {
                http_server.stop().get();
            });
            http_server.set_routes([&context](seastar::httpd::routes& routes) {
                set_agent_routes(routes, context);
            }).get();
            http_server.listen(
                seastar::socket_address{
                    seastar::net::inet_address(options["http-ingest-address"].as<std::string>()),
                    options["http-ingest-port"].as<uint16_t>()}).get();

            std::atomic<bool> stopping{false};
            auto file_source_done = run_file_source_loop(context, runtime_options, stopping);
            auto stdin_source_done = run_stdin_source_loop(context, runtime_options, stopping);
            auto tcp_source_done = run_tcp_source_loop(context, runtime_options, stopping);
            auto udp_source_done = run_udp_source_loop(context, runtime_options, stopping);
            auto unix_source_done = run_unix_socket_source_loop(context, runtime_options, stopping);
            auto sink_done = run_http_sink_loop(context, runtime_options, stopping);

            stop_signal.wait().get();
            stopping.store(true, std::memory_order_relaxed);
            file_source_done.get();
            stdin_source_done.get();
            tcp_source_done.get();
            udp_source_done.get();
            unix_source_done.get();
            sink_done.get();
        });
    });
}
