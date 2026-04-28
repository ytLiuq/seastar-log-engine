#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include "log_engine/config.hh"

namespace log_engine {

struct RouteDecision {
    unsigned shard = 0;
    std::uint64_t hash = 0;
    std::uint64_t token = 0;
    bool used_local_fallback = true;
};

class ShardRouter {
public:
    void configure(RoutingStrategy strategy, std::size_t virtual_nodes, unsigned shard_count);
    [[nodiscard]] RouteDecision route(std::string_view route_key, unsigned local_shard) const noexcept;
    [[nodiscard]] unsigned shard_count() const noexcept;
    [[nodiscard]] std::size_t ring_size() const noexcept;
    [[nodiscard]] RoutingStrategy strategy() const noexcept;
    [[nodiscard]] std::size_t virtual_nodes() const noexcept;

    static std::uint64_t stable_hash(std::string_view value) noexcept;

private:
    RoutingStrategy _strategy = RoutingStrategy::hash_modulo;
    std::size_t _virtual_nodes = 128;
    unsigned _shard_count = 0;
    std::vector<std::pair<std::uint64_t, unsigned>> _ring;
};

}  // namespace log_engine
