#include <stdexcept>
#include <string>
#include <string_view>

#include <boost/program_options.hpp>

#include <seastar/core/app-template.hh>
#include "log_engine/compat_glog.hh"

namespace {

}

int main(int argc, char** argv) {
    seastar::app_template app;
    namespace bpo = boost::program_options;

    app.add_options()
        ("log-dir", bpo::value<std::string>()->default_value("logs"), "Directory for shard log files")
        ("archive-dir", bpo::value<std::string>()->default_value("archive"), "Directory for archived log files")
        ("messages", bpo::value<std::uint64_t>()->default_value(100), "Number of compatibility log lines")
        ("batch-size", bpo::value<std::size_t>()->default_value(64), "Flush batch size");

    return app.run(argc, argv, [&app] () -> seastar::future<> {
        log_engine::EngineConfig config;
        config.log_dir = app.configuration()["log-dir"].as<std::string>();
        config.archive_dir = app.configuration()["archive-dir"].as<std::string>();
        config.batch_size = app.configuration()["batch-size"].as<std::size_t>();

        co_await log_engine::compat::init(config);
        const auto total_messages = app.configuration()["messages"].as<std::uint64_t>();
        for (std::uint64_t index = 0; index < total_messages; ++index) {
            LOG_INFO << "compat-demo-" << index;
            LOG_WARNING_R("compat-route") << "warning-" << index;
        }
        co_await log_engine::compat::flush();
        co_await log_engine::compat::shutdown();
    });
}
