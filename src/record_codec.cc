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

constexpr std::array<std::array<std::uint32_t, 256>, 8> make_crc32_tables() {
    std::array<std::array<std::uint32_t, 256>, 8> tables{};
    tables[0] = make_crc32_table();
    for (std::size_t table_index = 1; table_index < tables.size(); ++table_index) {
        for (std::size_t byte = 0; byte < tables[0].size(); ++byte) {
            const auto prev = tables[table_index - 1][byte];
            tables[table_index][byte] = tables[0][prev & 0xffU] ^ (prev >> 8U);
        }
    }
    return tables;
}

constexpr auto crc32_tables = make_crc32_tables();
constexpr char hex_digits[] = "0123456789abcdef";
constexpr std::uint64_t xxh64_prime1 = 11400714785074694791ULL;
constexpr std::uint64_t xxh64_prime2 = 14029467366897019727ULL;
constexpr std::uint64_t xxh64_prime3 = 1609587929392839161ULL;
constexpr std::uint64_t xxh64_prime4 = 9650029242287828579ULL;
constexpr std::uint64_t xxh64_prime5 = 2870177450012600261ULL;

std::uint32_t crc32_update(std::uint32_t value, std::string_view data) noexcept {
    const auto* current = reinterpret_cast<const unsigned char*>(data.data());
    std::size_t remaining = data.size();

    while (remaining >= 8) {
        const std::uint32_t first =
            static_cast<std::uint32_t>(current[0]) |
            (static_cast<std::uint32_t>(current[1]) << 8U) |
            (static_cast<std::uint32_t>(current[2]) << 16U) |
            (static_cast<std::uint32_t>(current[3]) << 24U);
        const std::uint32_t second =
            static_cast<std::uint32_t>(current[4]) |
            (static_cast<std::uint32_t>(current[5]) << 8U) |
            (static_cast<std::uint32_t>(current[6]) << 16U) |
            (static_cast<std::uint32_t>(current[7]) << 24U);

        value ^= first;
        value =
            crc32_tables[7][value & 0xffU] ^
            crc32_tables[6][(value >> 8U) & 0xffU] ^
            crc32_tables[5][(value >> 16U) & 0xffU] ^
            crc32_tables[4][(value >> 24U) & 0xffU] ^
            crc32_tables[3][second & 0xffU] ^
            crc32_tables[2][(second >> 8U) & 0xffU] ^
            crc32_tables[1][(second >> 16U) & 0xffU] ^
            crc32_tables[0][(second >> 24U) & 0xffU];

        current += 8;
        remaining -= 8;
    }

    while (remaining > 0) {
        value = crc32_tables[0][(value ^ *current++) & 0xffU] ^ (value >> 8U);
        --remaining;
    }

    return value;
}

std::uint32_t crc32_update_byte(std::uint32_t value, char ch) noexcept {
    return crc32_tables[0][(value ^ static_cast<unsigned char>(ch)) & 0xffU] ^ (value >> 8U);
}

std::uint64_t rotl64(std::uint64_t value, unsigned count) noexcept {
    return (value << count) | (value >> (64U - count));
}

std::uint64_t read_u64_le(const void* ptr) noexcept {
    std::uint64_t value = 0;
    std::memcpy(&value, ptr, sizeof(value));
    return value;
}

std::uint32_t read_u32_le(const void* ptr) noexcept {
    std::uint32_t value = 0;
    std::memcpy(&value, ptr, sizeof(value));
    return value;
}

std::uint64_t xxh64_round(std::uint64_t acc, std::uint64_t input) noexcept {
    acc += input * xxh64_prime2;
    acc = rotl64(acc, 31);
    acc *= xxh64_prime1;
    return acc;
}

std::uint64_t xxh64_merge_round(std::uint64_t acc, std::uint64_t val) noexcept {
    acc ^= xxh64_round(0, val);
    acc = acc * xxh64_prime1 + xxh64_prime4;
    return acc;
}

std::uint64_t xxh64_avalanche(std::uint64_t hash) noexcept {
    hash ^= hash >> 33U;
    hash *= xxh64_prime2;
    hash ^= hash >> 29U;
    hash *= xxh64_prime3;
    hash ^= hash >> 32U;
    return hash;
}

std::uint64_t xxh64(std::string_view data, std::uint64_t seed = 0) noexcept {
    const auto* current = reinterpret_cast<const unsigned char*>(data.data());
    const auto* const end = current + data.size();
    std::uint64_t hash = 0;

    if (data.size() >= 32) {
        std::uint64_t v1 = seed + xxh64_prime1 + xxh64_prime2;
        std::uint64_t v2 = seed + xxh64_prime2;
        std::uint64_t v3 = seed;
        std::uint64_t v4 = seed - xxh64_prime1;

        const auto* const limit = end - 32;
        do {
            v1 = xxh64_round(v1, read_u64_le(current));
            current += 8;
            v2 = xxh64_round(v2, read_u64_le(current));
            current += 8;
            v3 = xxh64_round(v3, read_u64_le(current));
            current += 8;
            v4 = xxh64_round(v4, read_u64_le(current));
            current += 8;
        } while (current <= limit);

        hash = rotl64(v1, 1) + rotl64(v2, 7) + rotl64(v3, 12) + rotl64(v4, 18);
        hash = xxh64_merge_round(hash, v1);
        hash = xxh64_merge_round(hash, v2);
        hash = xxh64_merge_round(hash, v3);
        hash = xxh64_merge_round(hash, v4);
    } else {
        hash = seed + xxh64_prime5;
    }

    hash += data.size();

    while (current + 8 <= end) {
        const auto k1 = xxh64_round(0, read_u64_le(current));
        hash ^= k1;
        hash = rotl64(hash, 27) * xxh64_prime1 + xxh64_prime4;
        current += 8;
    }

    if (current + 4 <= end) {
        hash ^= static_cast<std::uint64_t>(read_u32_le(current)) * xxh64_prime1;
        hash = rotl64(hash, 23) * xxh64_prime2 + xxh64_prime3;
        current += 4;
    }

    while (current < end) {
        hash ^= static_cast<std::uint64_t>(*current) * xxh64_prime5;
        hash = rotl64(hash, 11) * xxh64_prime1;
        ++current;
    }

    return xxh64_avalanche(hash);
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
    if (!value.empty()) {
        std::memcpy(out, value.data(), value.size());
        *crc = crc32_update(*crc, value);
        out += value.size();
    }
    return out;
}

char* append_sanitized_payload(char* out, std::string_view payload, std::uint32_t* crc) noexcept {
    std::size_t start = 0;
    while (start < payload.size()) {
        const auto special = payload.find_first_of("\n\r\t", start);
        if (special == std::string_view::npos) {
            return append_literal(out, payload.substr(start), crc);
        }
        if (special > start) {
            out = append_literal(out, payload.substr(start, special - start), crc);
        }
        *out++ = ' ';
        if (crc) {
            *crc = crc32_update_byte(*crc, ' ');
        }
        start = special + 1;
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

char* append_hex32(char* out, std::uint32_t value) noexcept {
    for (int i = 7; i >= 0; --i) {
        out[i] = hex_digits[value & 0xfU];
        value >>= 4U;
    }
    return out + 8;
}

char* append_hex64(char* out, std::uint64_t value) noexcept {
    for (int i = 15; i >= 0; --i) {
        out[i] = hex_digits[value & 0xfU];
        value >>= 4U;
    }
    return out + 16;
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

void write_crc_prefix(char* out, std::uint32_t crc, CrcClass crc_class, std::uint64_t payload_hash = 0) noexcept {
    out[0] = 'c';
    out[1] = 'r';
    out[2] = 'c';
    out[3] = '=';
    switch (crc_class) {
    case CrcClass::header:
        out[4] = 'h';
        out[5] = ':';
        append_hex32(out + 6, crc);
        out[14] = '\t';
        return;
    case CrcClass::payload_hash:
        out[4] = 'x';
        out[5] = ':';
        append_hex32(out + 6, crc);
        out[14] = ':';
        append_hex64(out + 15, payload_hash);
        out[31] = '\t';
        return;
    case CrcClass::full:
        append_hex32(out + 4, crc);
        out[12] = '\t';
        return;
    case CrcClass::none:
        return;
    }
}

std::size_t crc_prefix_size(CrcClass crc_class) noexcept {
    switch (crc_class) {
    case CrcClass::header:
        return 15;
    case CrcClass::payload_hash:
        return 32;
    case CrcClass::full:
        return 13;
    case CrcClass::none:
        return 0;
    }
    return 0;
}

struct PayloadFieldView {
    std::string_view metadata;
    std::string_view payload;
};

std::optional<PayloadFieldView> split_payload_field(std::string_view body) noexcept {
    constexpr std::string_view payload_key = "payload=";
    if (body.rfind(payload_key, 0) == 0) {
        return PayloadFieldView{
            .metadata = std::string_view(),
            .payload = body.substr(payload_key.size()),
        };
    }
    const auto payload_pos = body.find("\tpayload=");
    if (payload_pos == std::string_view::npos) {
        return std::nullopt;
    }
    return PayloadFieldView{
        .metadata = body.substr(0, payload_pos),
        .payload = body.substr(payload_pos + 9),
    };
}

std::optional<std::pair<std::uint32_t, std::uint64_t>> parse_hash_crc_prefix(std::string_view line) {
    constexpr std::string_view hash_prefix = "crc=x:";
    if (line.rfind(hash_prefix, 0) != 0) {
        return std::nullopt;
    }
    const auto tab = line.find('\t');
    if (tab == std::string_view::npos || tab <= hash_prefix.size()) {
        return std::nullopt;
    }
    const auto encoded = line.substr(hash_prefix.size(), tab - hash_prefix.size());
    const auto sep = encoded.find(':');
    if (sep == std::string_view::npos || sep == 0 || sep + 1 >= encoded.size()) {
        return std::nullopt;
    }
    const auto crc_value = parse_crc_hex(encoded.substr(0, sep));
    if (!crc_value) {
        return std::nullopt;
    }
    std::uint64_t payload_hash = 0;
    const auto hash_text = encoded.substr(sep + 1);
    const auto result = std::from_chars(hash_text.data(), hash_text.data() + hash_text.size(), payload_hash, 16);
    if (result.ec != std::errc{} || result.ptr != hash_text.data() + hash_text.size()) {
        return std::nullopt;
    }
    return std::make_pair(*crc_value, payload_hash);
}

}  // namespace

seastar::temporary_buffer<char> encode_record_buffer(
    const EngineConfig& config,
    unsigned shard_id,
    std::uint64_t sequence,
    LogLevel level,
    std::string_view timestamp,
    std::string_view payload) {
    const bool crc_wanted = config.record_crc_enabled && config.record_crc_class != CrcClass::none;
    const bool header_only = crc_wanted && config.record_crc_class == CrcClass::header;
    const bool payload_hash_mode = crc_wanted && config.record_crc_class == CrcClass::payload_hash;
    const bool any_structured = crc_wanted || config.record_timestamp_enabled ||
        config.record_shard_id_enabled || config.record_sequence_enabled ||
        config.record_level_enabled;

    if (!any_structured) {
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

    const std::size_t prefix_size = crc_wanted ? crc_prefix_size(config.record_crc_class) : 0;
    auto buffer = seastar::temporary_buffer<char>(prefix_size + body_estimate + 1);
    auto* const base = buffer.get_write();
    char* body = base + prefix_size;
    char* out = body;
    bool first = true;
    std::uint32_t crc = 0xffffffffU;
    auto* crc_ptr = crc_wanted ? &crc : nullptr;
    // For header-only CRC, we use a separate pointer to track CRC through metadata
    // but skip the payload. The metadata_crc_ptr is the same as crc_ptr until we
    // reach the payload field, then we switch to nullptr.
    auto* metadata_crc_ptr = crc_ptr;

    if (config.record_timestamp_enabled) {
        out = append_field_prefix(out, first, "ts=", metadata_crc_ptr);
        out = append_literal(out, timestamp, metadata_crc_ptr);
    }
    if (config.record_shard_id_enabled) {
        out = append_field_prefix(out, first, "shard=", metadata_crc_ptr);
        out = append_decimal(out, shard_id, metadata_crc_ptr);
    }
    if (config.record_sequence_enabled) {
        out = append_field_prefix(out, first, "seq=", metadata_crc_ptr);
        out = append_decimal(out, sequence, metadata_crc_ptr);
    }
    if (config.record_level_enabled) {
        out = append_field_prefix(out, first, "level=", metadata_crc_ptr);
        out = append_literal(out, level_value, metadata_crc_ptr);
    }

    // Payload: for header-only CRC, exclude payload bytes from CRC computation
    out = append_field_prefix(out, first, "payload=", (header_only || payload_hash_mode) ? nullptr : metadata_crc_ptr);
    char* const payload_begin = out;
    out = append_sanitized_payload(out, payload, (header_only || payload_hash_mode) ? nullptr : metadata_crc_ptr);

    if (crc_wanted) {
        std::uint64_t payload_hash = 0;
        if (payload_hash_mode) {
            payload_hash = xxh64(std::string_view(payload_begin, static_cast<std::size_t>(out - payload_begin)));
            std::array<char, 16> hash_hex{};
            append_hex64(hash_hex.data(), payload_hash);
            crc = crc32_update(crc, std::string_view(hash_hex.data(), hash_hex.size()));
        }
        write_crc_prefix(base, crc ^ 0xffffffffU, config.record_crc_class, payload_hash);
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
    constexpr std::string_view full_prefix = "crc=";
    constexpr std::string_view header_prefix = "crc=h:";
    constexpr std::string_view hash_prefix = "crc=x:";
    if (line.rfind(hash_prefix, 0) == 0) {
        const auto parsed_prefix = parse_hash_crc_prefix(line);
        if (!parsed_prefix) {
            return false;
        }
        const auto tab = line.find('\t');
        const auto body = line.substr(tab + 1);
        const auto payload_view = split_payload_field(body);
        if (!payload_view) {
            return false;
        }
        const auto payload_hash = xxh64(payload_view->payload);
        if (payload_hash != parsed_prefix->second) {
            return false;
        }
        std::array<char, 16> hash_hex{};
        append_hex64(hash_hex.data(), payload_hash);
        auto crc = crc32(payload_view->metadata);
        crc = crc32_update(crc ^ 0xffffffffU, std::string_view(hash_hex.data(), hash_hex.size())) ^ 0xffffffffU;
        return parsed_prefix->first == crc;
    }
    if (line.rfind(header_prefix, 0) == 0) {
        const auto tab = line.find('\t');
        if (tab == std::string_view::npos || tab <= header_prefix.size()) {
            return false;
        }
        const auto encoded_crc = parse_crc_hex(line.substr(header_prefix.size(), tab - header_prefix.size()));
        if (!encoded_crc) {
            return false;
        }
        const auto body = line.substr(tab + 1);
        const auto payload_pos = body.find("\tpayload=");
        const auto metadata = payload_pos == std::string_view::npos ? body : body.substr(0, payload_pos);
        return *encoded_crc == crc32(metadata);
    }
    if (line.rfind(full_prefix, 0) == 0) {
        const auto tab = line.find('\t');
        if (tab == std::string_view::npos || tab <= full_prefix.size()) {
            return false;
        }
        const auto encoded_crc = parse_crc_hex(line.substr(full_prefix.size(), tab - full_prefix.size()));
        if (!encoded_crc) {
            return false;
        }
        const auto body = line.substr(tab + 1);
        return *encoded_crc == crc32(body);
    }
    return parse_record_line(line).has_value();
}

std::optional<std::uint64_t> extract_sequence(std::string_view line) {
    const auto starts_with_crc = line.rfind("crc=", 0) == 0 || line.rfind("crc=h:", 0) == 0;
    const auto body = starts_with_crc
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
    constexpr std::string_view full_prefix = "crc=";
    constexpr std::string_view header_prefix = "crc=h:";
    constexpr std::string_view hash_prefix = "crc=x:";
    std::optional<std::uint32_t> encoded_crc;
    std::string_view body = line;

    if (line.rfind(hash_prefix, 0) == 0) {
        const auto parsed_prefix = parse_hash_crc_prefix(line);
        if (!parsed_prefix) {
            return std::nullopt;
        }
        const auto tab = line.find('\t');
        body = line.substr(tab + 1);
        const auto payload_view = split_payload_field(body);
        if (!payload_view) {
            return std::nullopt;
        }
        const auto payload_hash = xxh64(payload_view->payload);
        if (payload_hash != parsed_prefix->second) {
            return std::nullopt;
        }
        std::array<char, 16> hash_hex{};
        append_hex64(hash_hex.data(), payload_hash);
        auto crc = crc32(payload_view->metadata);
        crc = crc32_update(crc ^ 0xffffffffU, std::string_view(hash_hex.data(), hash_hex.size())) ^ 0xffffffffU;
        if (crc != parsed_prefix->first) {
            return std::nullopt;
        }
        encoded_crc = parsed_prefix->first;
    } else if (line.rfind(header_prefix, 0) == 0) {
        const auto tab = line.find('\t');
        if (tab == std::string_view::npos || tab <= header_prefix.size()) {
            return std::nullopt;
        }
        encoded_crc = parse_crc_hex(line.substr(header_prefix.size(), tab - header_prefix.size()));
        if (!encoded_crc) {
            return std::nullopt;
        }
        body = line.substr(tab + 1);
        const auto payload_pos = body.find("\tpayload=");
        const auto metadata = payload_pos == std::string_view::npos ? body : body.substr(0, payload_pos);
        if (*encoded_crc != crc32(metadata)) {
            return std::nullopt;
        }
    } else if (line.rfind(full_prefix, 0) == 0) {
        const auto tab = line.find('\t');
        if (tab == std::string_view::npos || tab <= full_prefix.size()) {
            return std::nullopt;
        }
        encoded_crc = parse_crc_hex(line.substr(full_prefix.size(), tab - full_prefix.size()));
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
