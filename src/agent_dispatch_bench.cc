#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <stdexcept>
#include <numeric>
#include <string>
#include <vector>

#include <boost/program_options.hpp>
#include <fmt/format.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

namespace {

struct Work {
    std::uint64_t id = 0;
    std::size_t bytes = 0;
    seastar::promise<> done;
};

struct ShardResult {
    std::vector<std::uint64_t> latencies;
    std::size_t max_depth = 0;
};

class BenchDispatcher {
public:
    BenchDispatcher(std::size_t capacity, std::size_t workers, std::uint64_t sink_delay_us)
        : _capacity(capacity)
        , _workers(workers)
        , _sink_delay_us(sink_delay_us) {
    }

    void start() {
        _worker_futures.reserve(_workers);
        for (std::size_t i = 0; i < _workers; ++i) {
            _worker_futures.push_back(run_worker());
        }
    }

    seastar::future<> submit(std::uint64_t id, std::size_t bytes) {
        auto work = std::make_unique<Work>();
        work->id = id;
        work->bytes = bytes;
        auto done = work->done.get_future();
        co_await _not_full.wait([this] {
            return _stopping || _queue.size() < _capacity;
        });
        if (_stopping) {
            co_return;
        }
        _queue.push_back(std::move(work));
        _max_depth = std::max<std::size_t>(_max_depth, _queue.size());
        _not_empty.broadcast();
        co_await std::move(done);
    }

    seastar::future<> stop() {
        _stopping = true;
        _not_empty.broadcast();
        _not_full.broadcast();
        if (!_worker_futures.empty()) {
            co_await seastar::when_all_succeed(_worker_futures.begin(), _worker_futures.end());
        }
    }

    std::size_t max_depth() const {
        return _max_depth;
    }

private:
    seastar::future<> run_worker() {
        while (true) {
            co_await _not_empty.wait([this] {
                return _stopping || !_queue.empty();
            });
            if (_queue.empty()) {
                if (_stopping) {
                    co_return;
                }
                continue;
            }
            auto work = std::move(_queue.front());
            _queue.pop_front();
            _not_full.broadcast();
            if (_sink_delay_us > 0) {
                co_await seastar::sleep(std::chrono::microseconds(_sink_delay_us));
            }
            work->done.set_value();
        }
    }

private:
    std::size_t _capacity = 0;
    std::size_t _workers = 0;
    std::uint64_t _sink_delay_us = 0;
    bool _stopping = false;
    std::deque<std::unique_ptr<Work>> _queue;
    std::size_t _max_depth = 0;
    seastar::condition_variable _not_empty;
    seastar::condition_variable _not_full;
    std::vector<seastar::future<>> _worker_futures;
};

double percentile_us(std::vector<std::uint64_t> values, double ratio) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const auto index = static_cast<std::size_t>(ratio * static_cast<double>(values.size() - 1));
    return static_cast<double>(values[index]);
}

}  // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    namespace bpo = boost::program_options;

    app.add_options()
        ("messages-per-shard", bpo::value<std::uint64_t>()->default_value(50000), "Messages submitted by each shard")
        ("payload-size", bpo::value<std::size_t>()->default_value(256), "Synthetic payload size")
        ("queue-capacity", bpo::value<std::size_t>()->default_value(1024), "Dispatcher queue capacity")
        ("workers", bpo::value<std::size_t>()->default_value(4), "Dispatcher worker count")
        ("sink-delay-us", bpo::value<std::uint64_t>()->default_value(0), "Synthetic per-message sink delay")
        ("inflight-per-shard", bpo::value<std::size_t>()->default_value(1), "Concurrent submits issued by each shard")
        ("mode", bpo::value<std::string>()->default_value("centralized"), "Dispatcher mode: centralized or per_shard");

    return app.run(argc, argv, [&app] () -> seastar::future<> {
        const auto messages_per_shard = app.configuration()["messages-per-shard"].as<std::uint64_t>();
        const auto payload_size = app.configuration()["payload-size"].as<std::size_t>();
        const auto queue_capacity = app.configuration()["queue-capacity"].as<std::size_t>();
        const auto workers = app.configuration()["workers"].as<std::size_t>();
        const auto sink_delay_us = app.configuration()["sink-delay-us"].as<std::uint64_t>();
        const auto inflight_per_shard = app.configuration()["inflight-per-shard"].as<std::size_t>();
        const auto mode = app.configuration()["mode"].as<std::string>();
        const auto dispatcher_shard = seastar::this_shard_id();

        std::vector<ShardResult> shard_results;
        shard_results.reserve(seastar::smp::count);
        auto start = std::chrono::steady_clock::now();
        std::size_t central_max_depth = 0;

        if (mode == "centralized") {
            BenchDispatcher dispatcher(queue_capacity, workers, sink_delay_us);
            dispatcher.start();

            std::vector<seastar::future<ShardResult>> submitters;
            submitters.reserve(seastar::smp::count);
            for (unsigned shard = 0; shard < seastar::smp::count; ++shard) {
                submitters.push_back(seastar::smp::submit_to(
                    shard,
                    [&, shard] () -> seastar::future<ShardResult> {
                        ShardResult result;
                        auto& latencies = result.latencies;
                        latencies.resize(messages_per_shard);
                        std::vector<std::uint64_t> indices;
                        indices.reserve(messages_per_shard);
                        for (std::uint64_t i = 0; i < messages_per_shard; ++i) {
                            indices.push_back(i);
                        }
                        co_await seastar::max_concurrent_for_each(indices, inflight_per_shard, [&, shard] (std::uint64_t i) {
                            const auto submitted = std::chrono::steady_clock::now();
                            return seastar::smp::submit_to(
                                dispatcher_shard,
                                [&dispatcher, id = (static_cast<std::uint64_t>(shard) << 48) | i, payload_size] {
                                    return dispatcher.submit(id, payload_size);
                                }).then([&, i, submitted] {
                                latencies[i] = static_cast<std::uint64_t>(
                                    std::chrono::duration_cast<std::chrono::microseconds>(
                                        std::chrono::steady_clock::now() - submitted).count());
                            });
                        });
                        co_return result;
                    }));
            }
            for (auto& submitter : submitters) {
                shard_results.push_back(co_await std::move(submitter));
            }
            central_max_depth = dispatcher.max_depth();
            co_await dispatcher.stop();
        } else if (mode == "per_shard") {
            std::vector<seastar::future<ShardResult>> submitters;
            submitters.reserve(seastar::smp::count);
            for (unsigned shard = 0; shard < seastar::smp::count; ++shard) {
                submitters.push_back(seastar::smp::submit_to(
                    shard,
                    [=] () -> seastar::future<ShardResult> {
                        return seastar::do_with(
                            BenchDispatcher(queue_capacity, workers, sink_delay_us),
                            [=] (BenchDispatcher& dispatcher) -> seastar::future<ShardResult> {
                                dispatcher.start();
                                ShardResult result;
                                auto& latencies = result.latencies;
                                latencies.resize(messages_per_shard);
                                std::vector<std::uint64_t> indices;
                                indices.reserve(messages_per_shard);
                                for (std::uint64_t i = 0; i < messages_per_shard; ++i) {
                                    indices.push_back(i);
                                }
                                co_await seastar::max_concurrent_for_each(indices, inflight_per_shard, [&, shard] (std::uint64_t i) {
                                    const auto submitted = std::chrono::steady_clock::now();
                                    return dispatcher.submit((static_cast<std::uint64_t>(shard) << 48) | i, payload_size).then([&, i, submitted] {
                                        latencies[i] = static_cast<std::uint64_t>(
                                            std::chrono::duration_cast<std::chrono::microseconds>(
                                                std::chrono::steady_clock::now() - submitted).count());
                                    });
                                });
                                result.max_depth = dispatcher.max_depth();
                                co_await dispatcher.stop();
                                co_return result;
                            });
                    }));
            }
            for (auto& submitter : submitters) {
                shard_results.push_back(co_await std::move(submitter));
            }
        } else {
            throw std::invalid_argument("mode must be centralized or per_shard");
        }

        const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start).count();

        std::vector<std::uint64_t> latencies;
        std::size_t per_shard_max_depth = 0;
        for (auto& shard_result : shard_results) {
            per_shard_max_depth = std::max(per_shard_max_depth, shard_result.max_depth);
            latencies.insert(latencies.end(), shard_result.latencies.begin(), shard_result.latencies.end());
        }
        const auto total_messages = messages_per_shard * seastar::smp::count;
        const auto latency_sum = std::accumulate(latencies.begin(), latencies.end(), std::uint64_t{0});
        const auto avg_us = latencies.empty() ? 0.0 : static_cast<double>(latency_sum) / static_cast<double>(latencies.size());
        const auto throughput = elapsed_us == 0
            ? 0.0
            : static_cast<double>(total_messages) * 1000000.0 / static_cast<double>(elapsed_us);
        const auto max_queue_depth = mode == "centralized"
            ? central_max_depth
            : per_shard_max_depth;

        fmt::print(
            "mode={} shards={} messages_per_shard={} total_messages={} elapsed_us={} throughput_msg_per_sec={:.2f} queue_capacity={} workers={} inflight_per_shard={} sink_delay_us={} avg_submit_us={:.2f} p50_submit_us={:.2f} p95_submit_us={:.2f} p99_submit_us={:.2f} max_queue_depth={}\n",
            mode,
            seastar::smp::count,
            messages_per_shard,
            total_messages,
            elapsed_us,
            throughput,
            queue_capacity,
            workers,
            inflight_per_shard,
            sink_delay_us,
            avg_us,
            percentile_us(latencies, 0.50),
            percentile_us(latencies, 0.95),
            percentile_us(latencies, 0.99),
            max_queue_depth);
    });
}
