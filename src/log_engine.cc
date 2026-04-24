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
    co_await _writers.invoke_on(shard, [msg = std::move(message)](AsyncWriter& writer) mutable {
        return writer.submit(std::move(msg));
    });
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
    const auto shard_count = seastar::smp::count;
    if (shard_count == 0) {
        return 0;
    }
    if (!route_key.empty()) {
        return static_cast<unsigned>(std::hash<std::string_view>{}(route_key) % shard_count);
    }
    return static_cast<unsigned>(_rr_counter.fetch_add(1, std::memory_order_relaxed) % shard_count);
}

}  // namespace log_engine
