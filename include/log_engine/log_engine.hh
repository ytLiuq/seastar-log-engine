#pragma once

#include <atomic>
#include <string_view>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "log_engine/async_writer.hh"
#include "log_engine/config.hh"
#include "log_engine/routing.hh"

namespace log_engine {

class LogEngine {
public:
    seastar::future<> start(EngineConfig config);
    seastar::future<> stop();

    seastar::future<> append(LogMessage message);
    seastar::future<> append_batch(std::vector<LogMessage> messages);
    seastar::future<> info(std::string payload, std::string route_key = {});
    seastar::future<> warn(std::string payload, std::string route_key = {});
    seastar::future<> error(std::string payload, std::string route_key = {});

private:
    unsigned route_to_shard(std::string_view route_key) noexcept;

private:
    EngineConfig _config;
    seastar::sharded<AsyncWriter> _writers;
    ShardRouter _router;
    std::atomic<std::uint64_t> _rr_counter{0};
    bool _started = false;
};

}  // namespace log_engine
