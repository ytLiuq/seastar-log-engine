#include <string>

#include <seastar/core/future.hh>

#include "log_engine/health_monitor.hh"

seastar::future<> test_health_counter_window_eviction() {
    log_engine::SlidingWindowCounter counter;
    counter.record_for_minute(100, 3);
    counter.record_for_minute(101, 2);
    if (counter.recent_sum_for_minute(101) != 5) {
        throw std::runtime_error("recent window should include fresh buckets");
    }
    if (counter.recent_sum_for_minute(105) != 2) {
        throw std::runtime_error("recent window should evict buckets older than 5 minutes");
    }
    if (counter.recent_sum_for_minute(106) != 0) {
        throw std::runtime_error("recent window should fully expire old buckets");
    }
    if (counter.lifetime_total() != 5) {
        throw std::runtime_error("lifetime total should not drop with window eviction");
    }
    return seastar::make_ready_future<>();
}
