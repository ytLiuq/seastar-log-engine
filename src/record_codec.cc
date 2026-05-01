#include "log_engine/record_codec.hh"

#include <array>
#include <charconv>
#include <cstring>

namespace log_engine {

namespace {

constexpr std::array<std::uint32_t, 256> make_crc32_table() {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t i = 0; i < table.size(); ++i) {
        auto value = i;
        for (int bit = 0; bit < 8; ++bit) {
            value = (value & 1U) ? (0xedb88320U ^ (value >> 1U)) : (value >> 1U);
        }
        table[i] = value;
    }
    return table;
}

constexpr auto crc32_table = make_crc32_table();
constexpr char hex_digits[] = "0123456789abcdef";

std::uint32_t crc32_update(std::uint32_t value, std::string_view data) noexcept {
    for (const auto ch : data) {
        value = crc32_table[(value ^ static_cast<unsigned char>(ch)) & 0xffU] ^ (value >> 8U);
    }
    return value;
}

std::uint32_t crc32_update_byte(std::uint32_t value, char ch) noexcept {
    return crc32_table[(value ^ static_cast<unsigned char>(ch)) & 0xffU] ^ (value >> 8U);
}

std::optional<std::uint32_t> parse_crc_hex(std::string_view input) {
    std::uint32_t value = 0;
    const auto* begin = input.data();
    const auto* end = input.data() + input.size();
    const auto result = std::from_chars(begin, end, value, 16);
    if (result.ec != std::errc{} || result.ptr != end) {
        return std::nullopt;
    }
    return value;
}

std::optional<std::string_view> extract_field(std::string_view body, std::string_view key) {
    std::size_t start = 0;
    while (start < body.size()) {
        const auto stop = body.find('\t', start);
        const auto token = body.substr(start, stop == std::string_view::npos ? body.size() - start : stop - start);
        if (token.rfind(key, 0) == 0) {
            return token.substr(key.size());
        }
        if (stop == std::string_view::npos) {
            break;
        }
        start = stop + 1;
    }
    return std::nullopt;
}

std::size_t decimal_length(std::uint64_t value) noexcept {
    std::size_t digits = 1;
    while (value >= 10) {
        value /= 10;
        ++digits;
    }
    return digits;
}

char* append_literal(char* out, std::string_view value) noexcept {
    if (!value.empty()) {
        std::memcpy(out, value.data(), value.size());
        out += value.size();
    }
    return out;
}

char* append_literal(char* out, std::string_view value, std::uint32_t* crc) noexcept {
    if (!crc) {
        return append_literal(out, value);
    }
    for (const auto ch : value) {
        *out++ = ch;
        *crc = crc32_update_byte(*crc, ch);
    }
    return out;
}

char* append_sanitized_payload(char* out, std::string_view payload, std::uint32_t* crc) noexcept {
    const auto first_special = payload.find_first_of("\n\r\t");
    if (first_special == std::string_view::npos) {
        return append_literal(out, payload, crc);
    }

    if (first_special > 0) {
        out = append_literal(out, payload.substr(0, first_special), crc);
    }

    for (std::size_t i = first_special; i < payload.size(); ++i) {
        const char ch = payload[i];
        const char sanitized = (ch == '\n' || ch == '\r' || ch == '\t') ? ' ' : ch;
        *out++ = sanitized;
        if (crc) {
            *crc = crc32_update_byte(*crc, sanitized);
        }
    }
    return out;
}

char* append_sanitized_payload(char* out, std::string_view payload) noexcept {
    return append_sanitized_payload(out, payload, nullptr);
}

char* append_decimal(char* out, std::uint64_t value) {
    auto result = std::to_chars(out, out + 32, value, 10);
    return result.ptr;
}

char* append_decimal(char* out, std::uint64_t value, std::uint32_t* crc) {
    auto* begin = out;
    auto result = std::to_chars(out, out + 32, value, 10);
    if (crc) {
        *crc = crc32_update(*crc, std::string_view(begin, static_cast<std::size_t>(result.ptr - begin)));
    }
    return result.ptr;
}

char* append_field_prefix(char* out, bool& first, std::string_view key, std::uint32_t* crc = nullptr) noexcept {
    if (!first) {
        *out++ = '\t';
        if (crc) {
            *crc = crc32_update(*crc, "\t");
        }
    } else {
        first = false;
    }
    return append_literal(out, key, crc);
}

void write_crc_prefix(char* out, std::uint32_t crc) noexcept {
    out[0] = 'c';
    out[1] = 'r';
    out[2] = 'c';
    out[3] = '=';
    for (int i = 0; i < 8; ++i) {
        out[11 - i] = hex_digits[crc & 0xfU];
        crc >>= 4U;
    }
    out[12] = '\t';
}

}  // namespace

seastar::temporary_buffer<char> encode_record_buffer(
    const EngineConfig& config,
    unsigned shard_id,
    std::uint64_t sequence,
    LogLevel level,
    std::string_view timestamp,
    std::string_view payload) {
    if (!config.record_crc_enabled &&
        !config.record_timestamp_enabled &&
        !config.record_shard_id_enabled &&
        !config.record_sequence_enabled &&
        !config.record_level_enabled) {
        auto buffer = seastar::temporary_buffer<char>(payload.size() + 1);
        auto* out = buffer.get_write();
        out = append_sanitized_payload(out, payload);
        *out++ = '\n';
        return buffer;
    }

    const auto shard_len = config.record_shard_id_enabled ? decimal_length(shard_id) : 0;
    const auto seq_len = config.record_sequence_enabled ? decimal_length(sequence) : 0;
    const auto level_value = config.record_level_enabled ? std::string_view(level_to_string(level)) : std::string_view();

    std::size_t body_estimate = payload.size() + 32;
    if (config.record_timestamp_enabled) {
        body_estimate += 3 + timestamp.size();
    }
    if (config.record_shard_id_enabled) {
        body_estimate += 6 + shard_len;
    }
    if (config.record_sequence_enabled) {
        body_estimate += 4 + seq_len;
    }
    if (config.record_level_enabled) {
        body_estimate += 6 + level_value.size();
    }

    const std::size_t prefix_size = config.record_crc_enabled ? 13 : 0;
    auto buffer = seastar::temporary_buffer<char>(prefix_size + body_estimate + 1);
    auto* const base = buffer.get_write();
    char* body = base + prefix_size;
    char* out = body;
    bool first = true;
    std::uint32_t crc = 0xffffffffU;
    auto* crc_ptr = config.record_crc_enabled ? &crc : nullptr;

    if (config.record_timestamp_enabled) {
        out = append_field_prefix(out, first, "ts=", crc_ptr);
        out = append_literal(out, timestamp, crc_ptr);
    }
    if (config.record_shard_id_enabled) {
        out = append_field_prefix(out, first, "shard=", crc_ptr);
        out = append_decimal(out, shard_id, crc_ptr);
    }

    if (config.record_sequence_enabled) {
        out = append_field_prefix(out, first, "seq=", crc_ptr);
        out = append_decimal(out, sequence, crc_ptr);
    }

    if (config.record_level_enabled) {
        out = append_field_prefix(out, first, "level=", crc_ptr);
        out = append_literal(out, level_value, crc_ptr);
    }

    out = append_field_prefix(out, first, "payload=", crc_ptr);
    out = append_sanitized_payload(out, payload, crc_ptr);

    const auto body_size = static_cast<std::size_t>(out - body);
    if (config.record_crc_enabled) {
        write_crc_prefix(base, crc ^ 0xffffffffU);
    }
    *out++ = '\n';
    buffer.trim(static_cast<std::size_t>(out - base));
    return buffer;
}

seastar::sstring encode_record(
    const EngineConfig& config,
    unsigned shard_id,
    std::uint64_t sequence,
    LogLevel level,
    std::string_view timestamp,
    std::string_view payload) {
    auto buffer = encode_record_buffer(config, shard_id, sequence, level, timestamp, payload);
    return seastar::sstring(buffer.get(), buffer.size());
}

VerifiedLogState scan_log_content(std::string_view content) {
    VerifiedLogState state;
    std::size_t offset = 0;
    while (offset < content.size()) {
        const auto newline = content.find('\n', offset);
        if (newline == std::string_view::npos) {
            state.clean_end = false;
            break;
        }

        const auto line = content.substr(offset, newline - offset);
        if (!line.empty()) {
            if (!verify_record_line(line)) {
                state.clean_end = false;
                break;
            }

            const auto sequence = extract_sequence(line);
            state.next_sequence = sequence ? (*sequence + 1) : (state.next_sequence + 1);
            ++state.valid_records;
        }

        state.valid_size = newline + 1;
        offset = newline + 1;
    }
    return state;
}

bool verify_record_line(std::string_view line) {
    constexpr std::string_view prefix = "crc=";
    if (line.rfind(prefix, 0) != 0) {
        return parse_record_line(line).has_value();
    }

    const auto tab = line.find('\t');
    if (tab == std::string_view::npos || tab <= prefix.size()) {
        return false;
    }

    const auto encoded_crc = parse_crc_hex(line.substr(prefix.size(), tab - prefix.size()));
    if (!encoded_crc) {
        return false;
    }

    const auto body = line.substr(tab + 1);
    return *encoded_crc == crc32(body);
}

std::optional<std::uint64_t> extract_sequence(std::string_view line) {
    const auto body = line.rfind("crc=", 0) == 0
        ? ([&]() -> std::optional<std::string_view> {
              const auto tab = line.find('\t');
              if (tab == std::string_view::npos || tab + 1 >= line.size()) {
                  return std::nullopt;
              }
              return line.substr(tab + 1);
          })()
        : std::optional<std::string_view>(line);
    if (!body) {
        return std::nullopt;
    }
    const auto seq = extract_field(*body, "seq=");
    if (!seq) {
        return std::nullopt;
    }

    std::uint64_t value = 0;
    const auto result = std::from_chars(seq->data(), seq->data() + seq->size(), value, 10);
    if (result.ec != std::errc{} || result.ptr != seq->data() + seq->size()) {
        return std::nullopt;
    }
    return value;
}

std::optional<ParsedRecord> parse_record_line(std::string_view line) {
    constexpr std::string_view prefix = "crc=";
    std::optional<std::uint32_t> encoded_crc;
    std::string_view body = line;
    if (line.rfind(prefix, 0) == 0) {
        const auto tab = line.find('\t');
        if (tab == std::string_view::npos || tab <= prefix.size()) {
            return std::nullopt;
        }

        encoded_crc = parse_crc_hex(line.substr(prefix.size(), tab - prefix.size()));
        if (!encoded_crc) {
            return std::nullopt;
        }

        body = line.substr(tab + 1);
        if (*encoded_crc != crc32(body)) {
            return std::nullopt;
        }
    }

    ParsedRecord record;
    record.crc = encoded_crc.value_or(0);
    record.raw_line.assign(line);

    const auto ts = extract_field(body, "ts=");
    const auto shard = extract_field(body, "shard=");
    const auto seq = extract_field(body, "seq=");
    const auto level = extract_field(body, "level=");
    const auto payload = extract_field(body, "payload=");

    if (ts) {
        record.timestamp.assign(ts->data(), ts->size());
    }
    if (shard) {
        unsigned parsed = 0;
        const auto result = std::from_chars(shard->data(), shard->data() + shard->size(), parsed, 10);
        if (result.ec != std::errc{} || result.ptr != shard->data() + shard->size()) {
            return std::nullopt;
        }
        record.shard = parsed;
    }
    if (seq) {
        std::uint64_t parsed = 0;
        const auto result = std::from_chars(seq->data(), seq->data() + seq->size(), parsed, 10);
        if (result.ec != std::errc{} || result.ptr != seq->data() + seq->size()) {
            return std::nullopt;
        }
        record.has_sequence = true;
        record.sequence = parsed;
    }
    if (level) {
        if (*level == "INFO") {
            record.level = LogLevel::info;
        } else if (*level == "WARN") {
            record.level = LogLevel::warn;
        } else if (*level == "ERROR") {
            record.level = LogLevel::error;
        } else {
            return std::nullopt;
        }
    }
    if (payload) {
        record.payload.assign(payload->data(), payload->size());
    } else {
        record.payload.assign(body.data(), body.size());
    }
    return record;
}

const char* level_to_string(LogLevel level) noexcept {
    switch (level) {
    case LogLevel::info:
        return "INFO";
    case LogLevel::warn:
        return "WARN";
    case LogLevel::error:
        return "ERROR";
    }
    return "UNKNOWN";
}

std::uint32_t crc32(std::string_view data) noexcept {
    return crc32_update(0xffffffffU, data) ^ 0xffffffffU;
}

}  // namespace log_engine
