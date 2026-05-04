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

    std::vector<std::vector<LogMessage>> per_shard(seastar::smp::count);
    std::vector<unsigned> shards;
    shards.reserve(messages.size());
    std::vector<std::size_t> per_shard_counts(seastar::smp::count, 0);
    for (const auto& message : messages) {
        const auto shard = route_to_shard(message.route_key);
        shards.push_back(shard);
        ++per_shard_counts[shard];
    }
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
