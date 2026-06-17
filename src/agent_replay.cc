#include <string>

#include <boost/program_options.hpp>
#include <fmt/format.h>

#include <seastar/core/app-template.hh>

#include "log_engine/agent_support.hh"
#include "log_engine/config_loader.hh"

int main(int argc, char** argv) {
    seastar::app_template app;
    namespace bpo = boost::program_options;

    app.add_options()
        ("config", bpo::value<std::string>()->default_value(""), "Path to key=value config file")
        ("log-dir", bpo::value<std::string>()->default_value("logs"), "Directory for shard log files")
        ("archive-dir", bpo::value<std::string>()->default_value("archive"), "Directory for archived log files")
        ("shard-file-prefix", bpo::value<std::string>()->default_value("shard"), "Shard log file prefix")
        ("agent-id", bpo::value<std::string>()->default_value("seastar-log-agent"), "Agent id to include in replay batches")
        ("delivery-offset-path", bpo::value<std::string>()->default_value("agent-delivery.offset"), "Delivery offset checkpoint")
        ("batch-size", bpo::value<std::size_t>()->default_value(100), "Max records per shard batch")
        ("include-archive", bpo::value<bool>()->default_value(true), "Whether to include archived segments")
        ("shard", bpo::value<unsigned>(), "Only replay one shard");

    return app.run(argc, argv, [&app] {
        auto& options = app.configuration();
        const auto file_values = log_engine::load_config_file(options["config"].as<std::string>());

        log_engine::EngineConfig base;
        base.log_dir = options["log-dir"].as<std::string>();
        base.archive_dir = options["archive-dir"].as<std::string>();
        base.shard_file_prefix = options["shard-file-prefix"].as<std::string>();
        const auto config = log_engine::apply_engine_config_overrides(base, options, file_values);

        log_engine::agent::ReplayOptions replay;
        replay.delivery_offset_path = log_engine::resolve_string_option(
            options,
            file_values,
            "delivery-offset-path",
            options["delivery-offset-path"].as<std::string>());
        replay.batch_size = log_engine::resolve_size_option(
            options,
            file_values,
            "batch-size",
            options["batch-size"].as<std::size_t>());
        replay.include_archive = log_engine::resolve_bool_option(
            options,
            file_values,
            "include-archive",
            options["include-archive"].as<bool>());
        if (options.count("shard")) {
            replay.shard = options["shard"].as<unsigned>();
        }

        const auto agent_id = log_engine::resolve_string_option(
            options,
            file_values,
            "agent-id",
            options["agent-id"].as<std::string>());
        for (const auto& batch : log_engine::agent::build_replay_batches(config, replay)) {
            fmt::print("{}\n", log_engine::agent::render_delivery_batch_json(agent_id, batch));
        }
        return seastar::make_ready_future<>();
    });
}
