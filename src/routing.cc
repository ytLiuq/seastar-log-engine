#include "log_engine/routing.hh"

#include <algorithm>
#include <array>
#include <string>

namespace log_engine {

namespace {

constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ull;
constexpr std::uint64_t kFnvPrime = 1099511628211ull;

std::uint64_t hash_parts(std::string_view first, std::string_view second) noexcept {
    auto hash = kFnvOffsetBasis;
    for (const auto part : std::array<std::string_view, 2>{first, second}) {
        for (const unsigned char ch : part) {
            hash ^= ch;
            hash *= kFnvPrime;
        }
        hash ^= 0xffu;
        hash *= kFnvPrime;
    }
    return hash;
}

}  // namespace

void ShardRouter::configure(RoutingStrategy strategy, std::size_t virtual_nodes, unsigned shard_count) {
    _strategy = strategy;
    _virtual_nodes = std::max<std::size_t>(virtual_nodes, 1);
    _shard_count = shard_count;
    _ring.clear();

    if (_strategy != RoutingStrategy::consistent_hashing || _shard_count == 0) {
        return;
    }

    _ring.reserve(static_cast<std::size_t>(_shard_count) * _virtual_nodes);
    for (unsigned shard = 0; shard < _shard_count; ++shard) {
        const auto shard_id = std::to_string(shard);
        for (std::size_t vnode = 0; vnode < _virtual_nodes; ++vnode) {
            _ring.emplace_back(hash_parts(shard_id, std::to_string(vnode)), shard);
        }
    }
    std::sort(_ring.begin(), _ring.end(), [] (const auto& lhs, const auto& rhs) {
        if (lhs.first != rhs.first) {
            return lhs.first < rhs.first;
        }
        return lhs.second < rhs.second;
    });
}

RouteDecision ShardRouter::route(
    std::string_view route_key,
    unsigned local_shard,
    EmptyRoutePolicy empty_route_policy,
    std::uint64_t empty_route_index) const noexcept {
    if (_shard_count == 0) {
        return RouteDecision{
            .shard = 0u,
            .hash = 0,
            .token = 0,
            .used_local_fallback = true,
        };
    }

    if (route_key.empty()) {
        const auto shard = empty_route_policy == EmptyRoutePolicy::round_robin
            ? static_cast<unsigned>(empty_route_index % _shard_count)
            : (local_shard % _shard_count);
        return RouteDecision{
            .shard = shard,
            .hash = 0,
            .token = 0,
            .used_local_fallback = empty_route_policy == EmptyRoutePolicy::local,
        };
    }

    const auto hash = stable_hash(route_key);
    if (_strategy == RoutingStrategy::consistent_hashing && !_ring.empty()) {
        const auto it = std::lower_bound(_ring.begin(), _ring.end(), hash, [] (const auto& entry, std::uint64_t value) {
            return entry.first < value;
        });
        const auto& match = it == _ring.end() ? _ring.front() : *it;
        return RouteDecision{
            .shard = match.second,
            .hash = hash,
            .token = match.first,
            .used_local_fallback = false,
        };
    }

    return RouteDecision{
        .shard = static_cast<unsigned>(hash % _shard_count),
        .hash = hash,
        .token = hash,
        .used_local_fallback = false,
    };
}

unsigned ShardRouter::shard_count() const noexcept {
    return _shard_count;
}

std::size_t ShardRouter::ring_size() const noexcept {
    return _ring.size();
}

RoutingStrategy ShardRouter::strategy() const noexcept {
    return _strategy;
}

std::size_t ShardRouter::virtual_nodes() const noexcept {
    return _virtual_nodes;
}

std::uint64_t ShardRouter::stable_hash(std::string_view value) noexcept {
    auto hash = kFnvOffsetBasis;
    for (const unsigned char ch : value) {
        hash ^= ch;
        hash *= kFnvPrime;
    }
    return hash;
}

}  // namespace log_engine
