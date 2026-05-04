#include "log_engine/log_engine.hh"

#include <functional>
#include <utility>

#include <seastar/core/future-util.hh>
#include <seastar/core/smp.hh>

namespace log_engine {

seastar::future<> LogEngine::start(EngineConfig config) {
    _config = std::move(config);
    _config.validate();
    if (_started) {
        co_return;
    }
    co_await _writers.start();
    co_await _writers.invoke_on_all([cfg = _config](AsyncWriter& writer) mutable {
        return writer.start(std::move(cfg));
    });
    _router.configure(_config.routing_strategy, _config.routing_virtual_nodes, seastar::smp::count);
    _started = true;
}

seastar::future<> LogEngine::stop() {
    if (!_started) {
        co_return;
    }
    co_await _writers.invoke_on_all(&AsyncWriter::stop);
    co_await _writers.stop();
    _started = false;
}

seastar::future<> LogEngine::append(LogMessage message) {
    const auto shard = route_to_shard(message.route_key);
    if (shard == seastar::this_shard_id()) {
        co_await _writers.local().submit(std::move(message));
        co_return;
    }
    co_await _writers.invoke_on(shard, [msg = std::move(message)](AsyncWriter& writer) mutable {
        return writer.submit(std::move(msg));
    });
}

seastar::future<> LogEngine::append_batch(std::vector<LogMessage> messages) {
    if (messages.empty()) {
        co_return;
    }

    if (_config.empty_route_policy == EmptyRoutePolicy::local) {
        bool all_empty_local = true;
        for (const auto& message : messages) {
            if (!message.route_key.empty()) {
                all_empty_local = false;
                break;
            }
        }
        if (all_empty_local) {
            co_await _writers.local().submit_many(std::move(messages));
            co_return;
        }
    }

    std::vector<std::size_t> per_shard_counts(seastar::smp::count, 0);
    std::uint64_t empty_route_base = 0;
    std::uint64_t empty_route_index = 0;
    unsigned first_shard = 0;
    bool first_shard_set = false;
    bool all_same_shard = true;
    bool all_empty_round_robin = _config.empty_route_policy == EmptyRoutePolicy::round_robin;
    std::size_t empty_count = 0;
    if (_config.empty_route_policy == EmptyRoutePolicy::round_robin) {
        for (const auto& message : messages) {
            empty_count += message.route_key.empty();
        }
        if (empty_count > 0) {
            empty_route_base = _rr_counter.fetch_add(empty_count, std::memory_order_relaxed);
        }
        all_empty_round_robin = empty_count == messages.size();
    }

    if (all_empty_round_robin) {
        std::vector<std::vector<LogMessage>> per_shard(seastar::smp::count);
        const auto shard_count = seastar::smp::count;
        const auto base_shard = static_cast<unsigned>(empty_route_base % shard_count);
        const auto full_cycles = messages.size() / shard_count;
        const auto remainder = messages.size() % shard_count;
        for (unsigned offset = 0; offset < shard_count; ++offset) {
            const auto shard = (base_shard + offset) % shard_count;
            per_shard[shard].reserve(full_cycles + (offset < remainder ? 1 : 0));
        }
        for (std::size_t index = 0; index < messages.size(); ++index) {
            const auto shard = static_cast<unsigned>((base_shard + index) % shard_count);
            per_shard[shard].push_back(std::move(messages[index]));
        }

        std::vector<seastar::future<>> shard_submits;
        shard_submits.reserve(shard_count);
        for (unsigned shard = 0; shard < per_shard.size(); ++shard) {
            if (per_shard[shard].empty()) {
                continue;
            }
            if (shard == seastar::this_shard_id()) {
                shard_submits.push_back(_writers.local().submit_many(std::move(per_shard[shard])));
                continue;
            }
            shard_submits.push_back(_writers.invoke_on(shard, [batch = std::move(per_shard[shard])](AsyncWriter& writer) mutable {
                return writer.submit_many(std::move(batch));
            }));
        }
        if (!shard_submits.empty()) {
            co_await seastar::when_all_succeed(shard_submits.begin(), shard_submits.end());
        }
        co_return;
    }

    std::vector<unsigned> shards;
    shards.reserve(messages.size());
    for (const auto& message : messages) {
        unsigned shard = 0;
        if (_config.empty_route_policy == EmptyRoutePolicy::round_robin && message.route_key.empty()) {
            shard = static_cast<unsigned>((empty_route_base + empty_route_index) % seastar::smp::count);
            ++empty_route_index;
        } else {
            shard = route_to_shard(message.route_key);
        }
        if (!first_shard_set) {
            first_shard = shard;
            first_shard_set = true;
        } else if (shard != first_shard) {
            all_same_shard = false;
        }
        shards.push_back(shard);
        ++per_shard_counts[shard];
    }

    if (all_same_shard) {
        if (first_shard == seastar::this_shard_id()) {
            co_await _writers.local().submit_many(std::move(messages));
            co_return;
        }
        co_await _writers.invoke_on(first_shard, [batch = std::move(messages)](AsyncWriter& writer) mutable {
            return writer.submit_many(std::move(batch));
        });
        co_return;
    }

    std::vector<std::vector<LogMessage>> per_shard(seastar::smp::count);
    for (unsigned shard = 0; shard < per_shard.size(); ++shard) {
        per_shard[shard].reserve(per_shard_counts[shard]);
    }
    for (std::size_t index = 0; index < messages.size(); ++index) {
        per_shard[shards[index]].push_back(std::move(messages[index]));
    }

    std::vector<seastar::future<>> shard_submits;
    shard_submits.reserve(seastar::smp::count);

    for (unsigned shard = 0; shard < per_shard.size(); ++shard) {
        if (per_shard[shard].empty()) {
            continue;
        }
        if (shard == seastar::this_shard_id()) {
            shard_submits.push_back(_writers.local().submit_many(std::move(per_shard[shard])));
            continue;
        }
        shard_submits.push_back(_writers.invoke_on(shard, [batch = std::move(per_shard[shard])](AsyncWriter& writer) mutable {
            return writer.submit_many(std::move(batch));
        }));
    }

    if (!shard_submits.empty()) {
        co_await seastar::when_all_succeed(shard_submits.begin(), shard_submits.end());
    }
}

seastar::future<> LogEngine::info(std::string payload, std::string route_key) {
    co_await append(LogMessage{.level = LogLevel::info, .payload = std::move(payload), .route_key = std::move(route_key)});
}

seastar::future<> LogEngine::warn(std::string payload, std::string route_key) {
    co_await append(LogMessage{.level = LogLevel::warn, .payload = std::move(payload), .route_key = std::move(route_key)});
}

seastar::future<> LogEngine::error(std::string payload, std::string route_key) {
    co_await append(LogMessage{.level = LogLevel::error, .payload = std::move(payload), .route_key = std::move(route_key)});
}

unsigned LogEngine::route_to_shard(std::string_view route_key) noexcept {
    return _router.route(route_key, seastar::this_shard_id(), _config.empty_route_policy, next_empty_route_index()).shard;
}

std::uint64_t LogEngine::next_empty_route_index() noexcept {
    if (_config.empty_route_policy != EmptyRoutePolicy::round_robin) {
        return 0;
    }
    return _rr_counter.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace log_engine
