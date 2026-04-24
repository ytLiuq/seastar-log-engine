#include <fstream>
#include <sstream>
#include <string>

#include <boost/program_options.hpp>
#include <fmt/format.h>

#include <seastar/core/app-template.hh>

#include "log_engine/config_loader.hh"
#include "log_engine/record_codec.hh"

int main(int argc, char** argv) {
    seastar::app_template app;
    namespace bpo = boost::program_options;

    app.add_options()
        ("config", bpo::value<std::string>()->default_value(""), "Path to key=value config file")
        ("path", bpo::value<std::string>()->required(), "Log file path to verify");

    return app.run(argc, argv, [&app] () -> seastar::future<> {
        const auto file_values = log_engine::load_config_file(app.configuration()["config"].as<std::string>());
        const auto path = log_engine::resolve_string_option(app.configuration(), file_values, "path", app.configuration()["path"].as<std::string>());
        std::ifstream in(path, std::ios::binary);
        std::stringstream buffer;
        buffer << in.rdbuf();
        const auto content = buffer.str();
        const auto verified = log_engine::scan_log_content(content);

        fmt::print(
            "path={} valid_size={} valid_records={} next_sequence={} clean_end={}\n",
            path,
            verified.valid_size,
            verified.valid_records,
            verified.next_sequence,
            verified.clean_end);
        return seastar::make_ready_future<>();
    });
}
