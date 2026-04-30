#include <string>

#include <boost/program_options.hpp>
#include <fmt/format.h>

#include <seastar/core/app-template.hh>

#include "log_engine/config_loader.hh"
#include "log_engine/log_reader.hh"

int main(int argc, char** argv) {
    seastar::app_template app;
    namespace bpo = boost::program_options;

    app.add_options()
        ("config", bpo::value<std::string>()->default_value(""), "Path to key=value config file")
        ("log-dir", bpo::value<std::string>()->default_value("logs"), "Directory for shard log files")
        ("archive-dir", bpo::value<std::string>()->default_value("archive"), "Directory for archived log files")
        ("shard-file-prefix", bpo::value<std::string>()->default_value("shard"), "Shard log file prefix")
        ("shard", bpo::value<unsigned>(), "Only read records from the given shard")
        ("seq-from", bpo::value<std::uint64_t>(), "Minimum sequence number")
        ("seq-to", bpo::value<std::uint64_t>(), "Maximum sequence number")
        ("time-from", bpo::value<std::string>(), "Minimum timestamp string")
        ("time-to", bpo::value<std::string>(), "Maximum timestamp string")
        ("limit", bpo::value<std::size_t>()->default_value(100), "Maximum number of records to return")
        ("include-archive", bpo::value<bool>()->default_value(true), "Whether to search archive files too");

    return app.run(argc, argv, [&app] {
        log_engine::EngineConfig base;
        base.log_dir = app.configuration()["log-dir"].as<std::string>();
        base.archive_dir = app.configuration()["archive-dir"].as<std::string>();
        base.shard_file_prefix = app.configuration()["shard-file-prefix"].as<std::string>();

        const auto file_values = log_engine::load_config_file(app.configuration()["config"].as<std::string>());
        auto config = log_engine::apply_engine_config_overrides(base, app.configuration(), file_values);

        log_engine::ReadQuery query;
        if (app.configuration().count("shard")) {
            query.shard = app.configuration()["shard"].as<unsigned>();
        }
        if (app.configuration().count("seq-from")) {
            query.seq_from = app.configuration()["seq-from"].as<std::uint64_t>();
        }
        if (app.configuration().count("seq-to")) {
            query.seq_to = app.configuration()["seq-to"].as<std::uint64_t>();
        }
        if (app.configuration().count("time-from")) {
            query.time_from = app.configuration()["time-from"].as<std::string>();
        }
        if (app.configuration().count("time-to")) {
            query.time_to = app.configuration()["time-to"].as<std::string>();
        }
        query.limit = log_engine::resolve_size_option(app.configuration(), file_values, "limit", app.configuration()["limit"].as<std::size_t>());
        query.include_archive = log_engine::resolve_bool_option(app.configuration(), file_values, "include-archive", app.configuration()["include-archive"].as<bool>());

        const auto segments = log_engine::collect_segments(config, query);
        const auto records = log_engine::read_records(segments, query);
        for (const auto& record : records) {
            fmt::print("{}\n", record.raw_line);
        }
        return seastar::make_ready_future<>();
    });
}
