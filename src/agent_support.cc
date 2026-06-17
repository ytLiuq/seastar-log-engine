#include "log_engine/agent_support.hh"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>
#include <glob.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include <netdb.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/net/api.hh>
#include <seastar/net/dns.hh>

#include "log_engine/log_reader.hh"

namespace log_engine::agent {

namespace {
namespace fs = std::filesystem;

constexpr std::string_view kBase64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string trim_copy(std::string_view value) {
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
        value.remove_prefix(1);
    }
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) {
        value.remove_suffix(1);
    }
    return std::string(value);
}

std::string lower_ascii(std::string_view value) {
    std::string lowered;
    lowered.reserve(value.size());
    for (const char ch : value) {
        if (ch >= 'A' && ch <= 'Z') {
            lowered.push_back(static_cast<char>(ch - 'A' + 'a'));
        } else {
            lowered.push_back(ch);
        }
    }
    return lowered;
}

bool valid_http_header_name(std::string_view name) {
    if (name.empty()) {
        return false;
    }
    for (const char ch : name) {
        const bool alpha = (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z');
        const bool digit = ch >= '0' && ch <= '9';
        const bool token_char = ch == '!' || ch == '#' || ch == '$' || ch == '%' || ch == '&' ||
            ch == '\'' || ch == '*' || ch == '+' || ch == '-' || ch == '.' || ch == '^' ||
            ch == '_' || ch == '`' || ch == '|' || ch == '~';
        if (!alpha && !digit && !token_char) {
            return false;
        }
    }
    return true;
}

bool valid_http_header_value(std::string_view value) {
    for (const char ch : value) {
        if (ch == '\r' || ch == '\n' || ch == '\0') {
            return false;
        }
    }
    return true;
}

bool reserved_http_header_name(std::string_view name) {
    const auto lowered = lower_ascii(name);
    return lowered == "host" ||
        lowered == "content-length" ||
        lowered == "content-type" ||
        lowered == "connection";
}

std::string render_http_request(const HttpEndpoint& endpoint, std::string body, const std::vector<HttpHeader>& headers) {
    std::string request =
        "POST " + endpoint.path + " HTTP/1.1\r\n" +
        "Host: " + endpoint.host + "\r\n" +
        "Content-Type: application/json\r\n";
    for (const auto& header : headers) {
        request += header.name;
        request += ": ";
        request += header.value;
        request += "\r\n";
    }
    request +=
        "Content-Length: " + std::to_string(body.size()) + "\r\n" +
        "Connection: close\r\n\r\n";
    request += std::move(body);
    return request;
}

std::uint64_t parse_u64(std::string_view value, const char* field) {
    std::uint64_t parsed = 0;
    const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (result.ec != std::errc{} || result.ptr != value.data() + value.size()) {
        throw std::runtime_error(std::string("invalid integer in offset file: ") + field);
    }
    return parsed;
}

std::map<std::string, std::string> load_kv_file(const std::string& path) {
    std::ifstream in(path);
    if (!in.good()) {
        return {};
    }

    std::map<std::string, std::string> values;
    std::string line;
    while (std::getline(in, line)) {
        const auto pos = line.find('=');
        if (pos == std::string::npos) {
            continue;
        }
        values[line.substr(0, pos)] = line.substr(pos + 1);
    }
    return values;
}

void store_text_file(const std::string& path, const std::string& content) {
    const fs::path target(path);
    if (target.has_parent_path()) {
        fs::create_directories(target.parent_path());
    }

    const auto tmp = target.string() + ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out.good()) {
            throw std::runtime_error("failed to open temp offset file: " + tmp);
        }
        out << content;
        out.flush();
        if (!out.good()) {
            throw std::runtime_error("failed to write temp offset file: " + tmp);
        }
    }
    fs::rename(tmp, target);
}

std::uint64_t inode_of(const std::string& path) {
    struct stat st {};
    if (::stat(path.c_str(), &st) != 0) {
        throw std::runtime_error("failed to stat source file: " + path);
    }
    return static_cast<std::uint64_t>(st.st_ino);
}

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

std::string base64_encode(std::string_view input) {
    std::string output;
    output.reserve(((input.size() + 2) / 3) * 4);
    std::size_t i = 0;
    while (i < input.size()) {
        const auto b0 = static_cast<unsigned char>(input[i++]);
        const auto has_b1 = i < input.size();
        const auto b1 = has_b1 ? static_cast<unsigned char>(input[i++]) : 0;
        const auto has_b2 = i < input.size();
        const auto b2 = has_b2 ? static_cast<unsigned char>(input[i++]) : 0;

        output.push_back(kBase64Alphabet[(b0 >> 2) & 0x3f]);
        output.push_back(kBase64Alphabet[((b0 & 0x03) << 4) | ((b1 >> 4) & 0x0f)]);
        output.push_back(has_b1 ? kBase64Alphabet[((b1 & 0x0f) << 2) | ((b2 >> 6) & 0x03)] : '=');
        output.push_back(has_b2 ? kBase64Alphabet[b2 & 0x3f] : '=');
    }
    return output;
}

int base64_value(char ch) {
    if (ch >= 'A' && ch <= 'Z') {
        return ch - 'A';
    }
    if (ch >= 'a' && ch <= 'z') {
        return ch - 'a' + 26;
    }
    if (ch >= '0' && ch <= '9') {
        return ch - '0' + 52;
    }
    if (ch == '+') {
        return 62;
    }
    if (ch == '/') {
        return 63;
    }
    return -1;
}

std::string base64_decode(std::string_view input) {
    if (input.size() % 4 != 0) {
        throw std::runtime_error("invalid base64 pending delivery record");
    }
    std::string output;
    output.reserve((input.size() / 4) * 3);
    for (std::size_t i = 0; i < input.size(); i += 4) {
        const auto v0 = base64_value(input[i]);
        const auto v1 = base64_value(input[i + 1]);
        const bool pad2 = input[i + 2] == '=';
        const bool pad3 = input[i + 3] == '=';
        const auto v2 = pad2 ? 0 : base64_value(input[i + 2]);
        const auto v3 = pad3 ? 0 : base64_value(input[i + 3]);
        if (v0 < 0 || v1 < 0 || v2 < 0 || v3 < 0 || (pad2 && !pad3)) {
            throw std::runtime_error("invalid base64 pending delivery record");
        }
        output.push_back(static_cast<char>((v0 << 2) | (v1 >> 4)));
        if (!pad2) {
            output.push_back(static_cast<char>(((v1 & 0x0f) << 4) | (v2 >> 2)));
        }
        if (!pad3) {
            output.push_back(static_cast<char>(((v2 & 0x03) << 6) | v3));
        }
    }
    return output;
}

class SocketFd {
public:
    explicit SocketFd(int fd) noexcept : _fd(fd) {}
    SocketFd(const SocketFd&) = delete;
    SocketFd& operator=(const SocketFd&) = delete;
    ~SocketFd() {
        if (_fd >= 0) {
            ::close(_fd);
        }
    }

    int get() const noexcept {
        return _fd;
    }

private:
    int _fd = -1;
};

}  // namespace

RetryableHttpStatusError::RetryableHttpStatusError(int status, std::string status_line)
    : std::runtime_error("retryable HTTP sink status: " + status_line)
    , _status(status) {
}

int RetryableHttpStatusError::status() const noexcept {
    return _status;
}

std::optional<SourceOffset> load_source_offset(const std::string& path) {
    const auto values = load_kv_file(path);
    if (values.empty()) {
        return std::nullopt;
    }
    const auto path_it = values.find("path");
    const auto inode_it = values.find("inode");
    const auto offset_it = values.find("offset");
    if (path_it == values.end() || inode_it == values.end() || offset_it == values.end()) {
        return std::nullopt;
    }
    return SourceOffset{
        .path = path_it->second,
        .inode = parse_u64(inode_it->second, "inode"),
        .offset = parse_u64(offset_it->second, "offset"),
    };
}

void store_source_offset(const std::string& path, const SourceOffset& offset) {
    store_text_file(
        path,
        "path=" + offset.path + "\n" +
            "inode=" + std::to_string(offset.inode) + "\n" +
            "offset=" + std::to_string(offset.offset) + "\n");
}

std::optional<DeliveryOffset> load_delivery_offset(const std::string& path) {
    const auto offsets = load_delivery_offsets(path);
    if (!offsets.empty()) {
        return offsets.front();
    }
    return std::nullopt;
}

void store_delivery_offset(const std::string& path, const DeliveryOffset& offset) {
    store_delivery_offsets(path, {offset});
}

std::vector<DeliveryOffset> load_delivery_offsets(const std::string& path) {
    std::ifstream in(path);
    if (!in.good()) {
        return {};
    }

    std::vector<DeliveryOffset> offsets;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }
        const auto comma = line.find(',');
        if (comma != std::string::npos) {
            const auto shard_part = line.substr(0, comma);
            const auto seq_part = line.substr(comma + 1);
            const auto shard_eq = shard_part.find('=');
            const auto seq_eq = seq_part.find('=');
            if (shard_eq != std::string::npos && seq_eq != std::string::npos) {
                offsets.push_back(DeliveryOffset{
                    .shard = static_cast<unsigned>(parse_u64(std::string_view(shard_part).substr(shard_eq + 1), "shard")),
                    .next_sequence = parse_u64(std::string_view(seq_part).substr(seq_eq + 1), "next_sequence"),
                });
            }
            continue;
        }
    }
    if (!offsets.empty()) {
        std::sort(offsets.begin(), offsets.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs.shard < rhs.shard;
        });
        return offsets;
    }

    const auto values = load_kv_file(path);
    if (values.empty()) {
        return {};
    }
    const auto shard_it = values.find("shard");
    const auto sequence_it = values.find("next_sequence");
    if (shard_it == values.end() || sequence_it == values.end()) {
        return {};
    }
    return {DeliveryOffset{
        .shard = static_cast<unsigned>(parse_u64(shard_it->second, "shard")),
        .next_sequence = parse_u64(sequence_it->second, "next_sequence"),
    }};
}

void store_delivery_offsets(const std::string& path, const std::vector<DeliveryOffset>& offsets) {
    std::vector<DeliveryOffset> sorted = offsets;
    std::sort(sorted.begin(), sorted.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.shard < rhs.shard;
    });

    std::string content = "format_version=2\n";
    for (const auto& offset : sorted) {
        content += "shard=" + std::to_string(offset.shard) +
            ",next_sequence=" + std::to_string(offset.next_sequence) + "\n";
    }
    store_text_file(path, content);
}

std::optional<DeliveryBatch> load_pending_delivery_batch(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in.good()) {
        return std::nullopt;
    }

    DeliveryBatch batch;
    bool saw_version = false;
    bool saw_shard = false;
    bool saw_first_sequence = false;
    bool saw_next_sequence = false;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }
        const auto pos = line.find('=');
        if (pos == std::string::npos) {
            continue;
        }
        const auto key = line.substr(0, pos);
        const auto value = line.substr(pos + 1);
        if (key == "format_version") {
            if (value != "1") {
                return std::nullopt;
            }
            saw_version = true;
        } else if (key == "shard") {
            batch.shard = static_cast<unsigned>(parse_u64(value, "shard"));
            saw_shard = true;
        } else if (key == "first_sequence") {
            batch.first_sequence = parse_u64(value, "first_sequence");
            saw_first_sequence = true;
        } else if (key == "next_sequence") {
            batch.next_sequence = parse_u64(value, "next_sequence");
            saw_next_sequence = true;
        } else if (key == "record_b64") {
            batch.records.push_back(base64_decode(value));
        }
    }

    if (!saw_version || !saw_shard || !saw_first_sequence || !saw_next_sequence || batch.records.empty()) {
        return std::nullopt;
    }
    if (batch.next_sequence < batch.first_sequence) {
        return std::nullopt;
    }
    return batch;
}

void store_pending_delivery_batch(const std::string& path, const DeliveryBatch& batch) {
    std::string content = "format_version=1\n";
    content += "shard=" + std::to_string(batch.shard) + "\n";
    content += "first_sequence=" + std::to_string(batch.first_sequence) + "\n";
    content += "next_sequence=" + std::to_string(batch.next_sequence) + "\n";
    for (const auto& record : batch.records) {
        content += "record_b64=" + base64_encode(record) + "\n";
    }
    store_text_file(path, content);
}

void remove_pending_delivery_batch(const std::string& path) {
    std::error_code ec;
    fs::remove(path, ec);
    fs::remove(path + ".tmp", ec);
}

std::vector<DeliveryBatch> build_replay_batches(const EngineConfig& config, const ReplayOptions& options) {
    std::map<unsigned, std::uint64_t> next_by_shard;
    for (const auto& offset : load_delivery_offsets(options.delivery_offset_path)) {
        next_by_shard[offset.shard] = offset.next_sequence;
    }

    std::vector<unsigned> shards;
    if (options.shard) {
        shards.push_back(*options.shard);
    } else if (!next_by_shard.empty()) {
        for (const auto& [shard, _] : next_by_shard) {
            shards.push_back(shard);
        }
    } else {
        for (const auto& entry : fs::directory_iterator(config.log_dir, fs::directory_options::skip_permission_denied)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            const auto name = entry.path().filename().string();
            const auto prefix = config.shard_file_prefix + "-";
            if (name.rfind(prefix, 0) != 0 || entry.path().extension() != ".log") {
                continue;
            }
            const auto shard_text = name.substr(prefix.size(), name.size() - prefix.size() - entry.path().extension().string().size());
            try {
                shards.push_back(static_cast<unsigned>(parse_u64(shard_text, "shard")));
            } catch (...) {
            }
        }
        std::sort(shards.begin(), shards.end());
        shards.erase(std::unique(shards.begin(), shards.end()), shards.end());
    }

    std::vector<DeliveryBatch> batches;
    for (const auto shard : shards) {
        ReadQuery query;
        query.include_archive = options.include_archive;
        query.limit = options.batch_size;
        query.shard = shard;
        query.seq_from = next_by_shard.contains(shard) ? next_by_shard[shard] : 0;

        const auto segments = collect_segments(config, query);
        const auto records = read_records(segments, query);
        if (records.empty()) {
            continue;
        }

        DeliveryBatch batch;
        batch.shard = shard;
        batch.first_sequence = query.seq_from.value_or(0);
        batch.next_sequence = batch.first_sequence;
        batch.records.reserve(records.size());
        for (const auto& record : records) {
            batch.records.push_back(record.payload);
            if (record.has_sequence) {
                if (batch.records.size() == 1) {
                    batch.first_sequence = record.sequence;
                }
                batch.next_sequence = std::max(batch.next_sequence, record.sequence + 1);
            } else {
                ++batch.next_sequence;
            }
        }
        batches.push_back(std::move(batch));
    }
    return batches;
}

std::uint64_t directory_size_bytes(const std::string& path) {
    if (path.empty() || !fs::exists(path)) {
        return 0;
    }

    std::uint64_t total = 0;
    for (const auto& entry : fs::recursive_directory_iterator(path, fs::directory_options::skip_permission_denied)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        std::error_code ec;
        const auto size = entry.file_size(ec);
        if (!ec) {
            total += size;
        }
    }
    return total;
}

bool disk_quota_exceeded(const std::string& path, const DiskQuota& quota) {
    return quota.max_buffer_bytes > 0 && directory_size_bytes(path) >= quota.max_buffer_bytes;
}

bool disk_quota_can_resume(const std::string& path, const DiskQuota& quota) {
    if (quota.max_buffer_bytes == 0) {
        return true;
    }
    const auto resume = quota.resume_buffer_bytes == 0 ? quota.max_buffer_bytes : quota.resume_buffer_bytes;
    return directory_size_bytes(path) <= resume;
}

BackpressureDecision evaluate_backpressure(const std::string& path, const DiskQuota& quota, const BackpressureState& state) {
    const auto disk_bytes = state.disk_bytes == 0 ? directory_size_bytes(path) : state.disk_bytes;
    if (quota.max_buffer_bytes > 0 && disk_bytes >= quota.max_buffer_bytes) {
        return BackpressureDecision{.pause = true, .reason = "disk_quota"};
    }
    if (state.max_sink_backlog_records > 0 && state.sink_backlog_records >= state.max_sink_backlog_records) {
        return BackpressureDecision{.pause = true, .reason = "sink_backlog"};
    }
    if (state.max_recent_sink_failures > 0 && state.recent_sink_failures >= state.max_recent_sink_failures) {
        return BackpressureDecision{.pause = true, .reason = "sink_failures"};
    }
    if (state.max_sink_latency_ms > 0 && state.last_sink_latency_ms >= state.max_sink_latency_ms) {
        return BackpressureDecision{.pause = true, .reason = "sink_latency"};
    }
    return BackpressureDecision{};
}

TailBatch tail_file_once(
    const std::string& path,
    const std::optional<SourceOffset>& previous,
    std::size_t max_lines) {
    TailBatch batch;
    batch.next_offset.path = path;
    batch.next_offset.inode = inode_of(path);

    std::uint64_t start_offset = 0;
    const auto file_size = fs::file_size(path);
    if (previous && previous->path == path && previous->inode == batch.next_offset.inode && previous->offset <= file_size) {
        start_offset = previous->offset;
    } else if (previous) {
        batch.file_rotated_or_truncated = true;
    }
    batch.next_offset.offset = start_offset;

    if (max_lines == 0 || start_offset >= file_size) {
        return batch;
    }

    std::ifstream in(path, std::ios::binary);
    if (!in.good()) {
        throw std::runtime_error("failed to open source file: " + path);
    }
    in.seekg(static_cast<std::streamoff>(start_offset));

    std::string line;
    std::uint64_t committed_offset = start_offset;
    while (batch.lines.size() < max_lines && std::getline(in, line)) {
        const auto after_line = in.tellg();
        if (after_line == std::streampos(-1)) {
            if (in.eof()) {
                break;
            }
            throw std::runtime_error("failed to read source file: " + path);
        }
        batch.lines.push_back(line);
        committed_offset = static_cast<std::uint64_t>(after_line);
    }
    batch.next_offset.offset = committed_offset;
    return batch;
}

std::vector<std::string> expand_glob_paths(std::string_view pattern) {
    if (pattern.empty()) {
        return {};
    }
    glob_t glob_result {};
    const std::string pattern_text(pattern);
    const int rc = ::glob(pattern_text.c_str(), GLOB_NOSORT, nullptr, &glob_result);
    std::vector<std::string> paths;
    if (rc == 0) {
        paths.reserve(glob_result.gl_pathc);
        for (std::size_t i = 0; i < glob_result.gl_pathc; ++i) {
            if (glob_result.gl_pathv[i] != nullptr && fs::is_regular_file(glob_result.gl_pathv[i])) {
                paths.emplace_back(glob_result.gl_pathv[i]);
            }
        }
        std::sort(paths.begin(), paths.end());
    } else if (rc == GLOB_NOMATCH && fs::is_regular_file(pattern_text)) {
        paths.push_back(pattern_text);
    }
    ::globfree(&glob_result);
    return paths;
}

std::optional<HttpEndpoint> parse_http_endpoint(std::string_view url) {
    constexpr std::string_view prefix = "http://";
    if (url.substr(0, prefix.size()) != prefix) {
        return std::nullopt;
    }
    url.remove_prefix(prefix.size());
    if (url.empty()) {
        return std::nullopt;
    }

    const auto slash = url.find('/');
    const auto authority = slash == std::string_view::npos ? url : url.substr(0, slash);
    if (authority.empty()) {
        return std::nullopt;
    }

    HttpEndpoint endpoint;
    endpoint.path = slash == std::string_view::npos ? "/" : std::string(url.substr(slash));
    const auto colon = authority.rfind(':');
    if (colon == std::string_view::npos) {
        endpoint.host = std::string(authority);
        return endpoint;
    }

    endpoint.host = std::string(authority.substr(0, colon));
    if (endpoint.host.empty()) {
        return std::nullopt;
    }
    std::uint16_t port = 0;
    const auto port_text = authority.substr(colon + 1);
    const auto result = std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
    if (result.ec != std::errc{} || result.ptr != port_text.data() + port_text.size() || port == 0) {
        return std::nullopt;
    }
    endpoint.port = port;
    return endpoint;
}

std::vector<int> parse_http_status_codes(std::string_view value) {
    std::vector<int> statuses;
    while (!value.empty()) {
        while (!value.empty() && (value.front() == ' ' || value.front() == '\t' || value.front() == ',')) {
            value.remove_prefix(1);
        }
        if (value.empty()) {
            break;
        }
        const auto comma = value.find(',');
        auto token = comma == std::string_view::npos ? value : value.substr(0, comma);
        while (!token.empty() && (token.back() == ' ' || token.back() == '\t')) {
            token.remove_suffix(1);
        }
        int status = 0;
        const auto result = std::from_chars(token.data(), token.data() + token.size(), status);
        if (result.ec != std::errc{} || result.ptr != token.data() + token.size() || status < 100 || status > 599) {
            throw std::invalid_argument("invalid HTTP status code list");
        }
        statuses.push_back(status);
        if (comma == std::string_view::npos) {
            break;
        }
        value.remove_prefix(comma + 1);
    }
    std::sort(statuses.begin(), statuses.end());
    statuses.erase(std::unique(statuses.begin(), statuses.end()), statuses.end());
    return statuses;
}

std::vector<HttpHeader> parse_http_headers(std::string_view value) {
    std::vector<HttpHeader> headers;
    while (!value.empty()) {
        const auto separator = value.find(';');
        auto token = separator == std::string_view::npos ? value : value.substr(0, separator);
        const auto colon = token.find(':');
        if (colon != std::string_view::npos) {
            auto name = trim_copy(token.substr(0, colon));
            auto header_value = trim_copy(token.substr(colon + 1));
            if (!valid_http_header_name(name) || reserved_http_header_name(name)) {
                throw std::invalid_argument("invalid HTTP sink header name");
            }
            if (!valid_http_header_value(header_value)) {
                throw std::invalid_argument("invalid HTTP sink header value");
            }
            headers.push_back(HttpHeader{
                .name = std::move(name),
                .value = std::move(header_value),
            });
        } else if (!trim_copy(token).empty()) {
            throw std::invalid_argument("invalid HTTP sink header list");
        }
        if (separator == std::string_view::npos) {
            break;
        }
        value.remove_prefix(separator + 1);
    }
    return headers;
}

std::string render_json_batch(const std::vector<std::string>& records) {
    std::string body = "{\"records\":[";
    for (std::size_t i = 0; i < records.size(); ++i) {
        if (i != 0) {
            body += ',';
        }
        body += "{\"message\":\"";
        body += json_escape(records[i]);
        body += "\"}";
    }
    body += "]}";
    return body;
}

std::string render_delivery_batch_json(std::string_view agent_id, const DeliveryBatch& batch) {
    std::string body = "{";
    body += "\"agent_id\":\"";
    body += json_escape(agent_id);
    body += "\",\"shard\":";
    body += std::to_string(batch.shard);
    body += ",\"first_sequence\":";
    body += std::to_string(batch.first_sequence);
    body += ",\"next_sequence\":";
    body += std::to_string(batch.next_sequence);
    body += ",\"records\":[";
    for (std::size_t i = 0; i < batch.records.size(); ++i) {
        if (i != 0) {
            body += ',';
        }
        body += "{\"sequence\":";
        body += std::to_string(batch.first_sequence + i);
        body += ",\"message\":\"";
        body += json_escape(batch.records[i]);
        body += "\"}";
    }
    body += "]}";
    return body;
}

void post_http_batch(const HttpEndpoint& endpoint, std::string_view body) {
    addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* results = nullptr;
    const auto port = std::to_string(endpoint.port);
    const int gai = ::getaddrinfo(endpoint.host.c_str(), port.c_str(), &hints, &results);
    if (gai != 0) {
        throw std::runtime_error(std::string("failed to resolve HTTP sink: ") + ::gai_strerror(gai));
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> guard(results, ::freeaddrinfo);

    int connected_fd = -1;
    for (auto* ai = results; ai != nullptr; ai = ai->ai_next) {
        const int fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) {
            continue;
        }
        if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
            connected_fd = fd;
            break;
        }
        ::close(fd);
    }
    if (connected_fd < 0) {
        throw std::runtime_error("failed to connect HTTP sink: " + endpoint.host);
    }
    SocketFd socket(connected_fd);

    const auto request = render_http_request(endpoint, std::string(body), {});

    std::size_t sent = 0;
    while (sent < request.size()) {
        const auto rc = ::send(socket.get(), request.data() + sent, request.size() - sent, 0);
        if (rc <= 0) {
            throw std::runtime_error("failed to send HTTP sink request");
        }
        sent += static_cast<std::size_t>(rc);
    }

    std::string response;
    char buffer[1024];
    while (true) {
        const auto rc = ::recv(socket.get(), buffer, sizeof(buffer), 0);
        if (rc < 0) {
            throw std::runtime_error("failed to read HTTP sink response");
        }
        if (rc == 0) {
            break;
        }
        response.append(buffer, static_cast<std::size_t>(rc));
        if (response.size() > 4096) {
            break;
        }
    }

    const auto line_end = response.find("\r\n");
    const auto status_line = response.substr(0, line_end);
    if (status_line.rfind("HTTP/", 0) != 0 || status_line.size() < 12) {
        throw std::runtime_error("invalid HTTP sink response");
    }
    const auto status_text = std::string_view(status_line).substr(9, 3);
    int status = 0;
    const auto result = std::from_chars(status_text.data(), status_text.data() + status_text.size(), status);
    if (result.ec != std::errc{} || status < 200 || status >= 300) {
        throw std::runtime_error("HTTP sink returned non-2xx status: " + status_line);
    }
}

static seastar::future<> post_http_batch_async_impl(const HttpEndpoint& endpoint, std::string body, const HttpPostOptions& options) {
    auto retryable_status_codes = options.retryable_status_codes;
    std::sort(retryable_status_codes.begin(), retryable_status_codes.end());
    retryable_status_codes.erase(std::unique(retryable_status_codes.begin(), retryable_status_codes.end()), retryable_status_codes.end());

    const auto address = co_await seastar::net::dns::resolve_name(endpoint.host);
    auto socket = co_await seastar::engine().net().connect(seastar::socket_address(address, endpoint.port));
    auto out = socket.output();
    auto in = socket.input();

    const auto request = render_http_request(endpoint, std::move(body), options.headers);

    co_await out.write(request);
    co_await out.flush();

    auto response = co_await in.read();
    co_await out.close();
    co_await in.close();

    if (response.empty()) {
        throw std::runtime_error("empty HTTP sink response");
    }
    std::string_view response_view(response.get(), response.size());
    const auto line_end = response_view.find("\r\n");
    const auto status_line = response_view.substr(0, line_end);
    if (status_line.rfind("HTTP/", 0) != 0 || status_line.size() < 12) {
        throw std::runtime_error("invalid HTTP sink response");
    }
    const auto status_text = status_line.substr(9, 3);
    int status = 0;
    const auto result = std::from_chars(status_text.data(), status_text.data() + status_text.size(), status);
    if (result.ec != std::errc{} || status < 200 || status >= 300) {
        if (std::binary_search(retryable_status_codes.begin(), retryable_status_codes.end(), status)) {
            throw RetryableHttpStatusError(status, std::string(status_line));
        }
        throw std::runtime_error("HTTP sink returned non-2xx status: " + std::string(status_line));
    }
}

seastar::future<> post_http_batch_async(const HttpEndpoint& endpoint, std::string body, const HttpPostOptions& options) {
    if (options.timeout_ms == 0) {
        return post_http_batch_async_impl(endpoint, std::move(body), options);
    }
    return seastar::with_timeout(
        seastar::lowres_clock::now() + std::chrono::milliseconds(options.timeout_ms),
        post_http_batch_async_impl(endpoint, std::move(body), options));
}

seastar::future<> post_http_batch_async(const HttpEndpoint& endpoint, std::string body) {
    HttpPostOptions options;
    return post_http_batch_async(endpoint, std::move(body), options);
}

void write_stdout_batch(const std::vector<std::string>& records) {
    for (const auto& record : records) {
        std::cout << record << '\n';
    }
    std::cout.flush();
    if (!std::cout.good()) {
        throw std::runtime_error("failed to write stdout sink");
    }
}

}  // namespace log_engine::agent
