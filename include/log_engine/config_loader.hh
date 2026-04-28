#pragma once

#include <optional>
#include <string>
#include <unordered_map>

#include <boost/program_options/variables_map.hpp>

#include "log_engine/config.hh"

namespace log_engine {

using ConfigMap = std::unordered_map<std::string, std::string>;

ConfigMap load_config_file(const std::string& path);
EngineConfig apply_engine_config_overrides(
    EngineConfig base,
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values);
std::string resolve_string_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    const std::string& current_value);
std::uint64_t resolve_u64_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    std::uint64_t current_value);
std::size_t resolve_size_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    std::size_t current_value);
bool resolve_bool_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    bool current_value);
WriteMode resolve_write_mode_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    WriteMode current_value);
RoutingStrategy resolve_routing_strategy_option(
    const boost::program_options::variables_map& cli,
    const ConfigMap& file_values,
    const std::string& name,
    RoutingStrategy current_value);

}  // namespace log_engine
