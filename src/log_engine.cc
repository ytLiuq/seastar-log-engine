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
    for (auto& message : messages) {
        const auto shard = route_to_shard(message.route_key);
        per_shard[shard].push_back(std::move(message));
    }

    std::vector<seastar::future<>> remote_submits;
    remote_submits.reserve(seastar::smp::count > 0 ? seastar::smp::count - 1 : 0);

    for (unsigned shard = 0; shard < per_shard.size(); ++shard) {
        if (per_shard[shard].empty()) {
            continue;
        }
        if (shard == seastar::this_shard_id()) {
            co_await _writers.local().submit_many(std::move(per_shard[shard]));
            continue;
        }
        remote_submits.push_back(_writers.invoke_on(shard, [batch = std::move(per_shard[shard])](AsyncWriter& writer) mutable {
            return writer.submit_many(std::move(batch));
        }));
    }

    if (!remote_submits.empty()) {
        co_await seastar::when_all_succeed(remote_submits.begin(), remote_submits.end());
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
    return _router.route(route_key, seastar::this_shard_id()).shard;
}

}  // namespace log_engine
