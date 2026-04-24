#include "log_engine/record_codec.hh"

#include <array>
#include <charconv>

#include <seastar/core/print.hh>

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

}  // namespace

seastar::sstring encode_record(
    unsigned shard_id,
    std::uint64_t sequence,
    LogLevel level,
    std::string timestamp,
    std::string payload) {
    for (char& ch : payload) {
        if (ch == '\n' || ch == '\r' || ch == '\t') {
            ch = ' ';
        }
    }

    const auto body = seastar::format(
        "ts={}\tshard={}\tseq={}\tlevel={}\tpayload={}",
        timestamp,
        shard_id,
        sequence,
        level_to_string(level),
        payload);

    return seastar::format("crc={:08x}\t{}\n", crc32(body), body);
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
            if (!sequence) {
                state.clean_end = false;
                break;
            }
            state.next_sequence = *sequence + 1;
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
        return false;
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
    const auto tab = line.find('\t');
    if (tab == std::string_view::npos || tab + 1 >= line.size()) {
        return std::nullopt;
    }

    const auto body = line.substr(tab + 1);
    const auto seq = extract_field(body, "seq=");
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
    if (line.rfind(prefix, 0) != 0) {
        return std::nullopt;
    }

    const auto tab = line.find('\t');
    if (tab == std::string_view::npos || tab <= prefix.size()) {
        return std::nullopt;
    }

    const auto encoded_crc = parse_crc_hex(line.substr(prefix.size(), tab - prefix.size()));
    if (!encoded_crc) {
        return std::nullopt;
    }

    const auto body = line.substr(tab + 1);
    if (*encoded_crc != crc32(body)) {
        return std::nullopt;
    }

    ParsedRecord record;
    record.crc = *encoded_crc;
    record.raw_line.assign(line);

    const auto ts = extract_field(body, "ts=");
    const auto shard = extract_field(body, "shard=");
    const auto seq = extract_field(body, "seq=");
    const auto level = extract_field(body, "level=");
    const auto payload = extract_field(body, "payload=");
    if (!ts || !shard || !seq || !level || !payload) {
        return std::nullopt;
    }

    record.timestamp.assign(ts->data(), ts->size());
    {
        unsigned parsed = 0;
        const auto result = std::from_chars(shard->data(), shard->data() + shard->size(), parsed, 10);
        if (result.ec != std::errc{} || result.ptr != shard->data() + shard->size()) {
            return std::nullopt;
        }
        record.shard = parsed;
    }
    {
        std::uint64_t parsed = 0;
        const auto result = std::from_chars(seq->data(), seq->data() + seq->size(), parsed, 10);
        if (result.ec != std::errc{} || result.ptr != seq->data() + seq->size()) {
            return std::nullopt;
        }
        record.sequence = parsed;
    }
    if (*level == "INFO") {
        record.level = LogLevel::info;
    } else if (*level == "WARN") {
        record.level = LogLevel::warn;
    } else if (*level == "ERROR") {
        record.level = LogLevel::error;
    } else {
        return std::nullopt;
    }
    record.payload.assign(payload->data(), payload->size());
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
    std::uint32_t value = 0xffffffffU;
    for (const auto ch : data) {
        value = crc32_table[(value ^ static_cast<unsigned char>(ch)) & 0xffU] ^ (value >> 8U);
    }
    return value ^ 0xffffffffU;
}

}  // namespace log_engine
