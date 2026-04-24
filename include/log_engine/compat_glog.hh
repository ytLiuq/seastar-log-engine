#pragma once

#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>

#include <seastar/core/future.hh>

#include "log_engine/config.hh"
#include "log_engine/log_engine.hh"

namespace log_engine::compat {

void bind(LogEngine& engine) noexcept;
void unbind() noexcept;
seastar::future<> flush();
bool is_initialized() noexcept;
seastar::future<> submit(LogLevel level, std::string message, std::string route_key = {});

class LogLine {
public:
    LogLine(LogLevel level, const char* file, int line, std::string route_key = {});
    ~LogLine();

    std::ostream& stream() noexcept;
    void send();

private:
    LogLevel _level;
    std::string _file;
    int _line;
    std::string _route_key;
    std::ostringstream _stream;
    bool _sent = false;
};

}  // namespace log_engine::compat

#define LOG_ENGINE_LOG(level) ::log_engine::compat::LogLine(::log_engine::LogLevel::level, __FILE__, __LINE__).stream()
#define LOG_ENGINE_LOG_R(level, route_key) ::log_engine::compat::LogLine(::log_engine::LogLevel::level, __FILE__, __LINE__, (route_key)).stream()

#define LOG_INFO LOG_ENGINE_LOG(info)
#define LOG_WARNING LOG_ENGINE_LOG(warn)
#define LOG_ERROR LOG_ENGINE_LOG(error)

#define LOG_INFO_R(route_key) LOG_ENGINE_LOG_R(info, (route_key))
#define LOG_WARNING_R(route_key) LOG_ENGINE_LOG_R(warn, (route_key))
#define LOG_ERROR_R(route_key) LOG_ENGINE_LOG_R(error, (route_key))
