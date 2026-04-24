#include "log_engine/compat_glog.hh"

#include <cstdio>
#include <exception>
#include <utility>
#include <vector>

namespace log_engine::compat {

namespace {

LogEngine* bound_engine = nullptr;
std::vector<LogMessage> pending_messages;

}  // namespace

void bind(LogEngine& engine) noexcept {
    bound_engine = &engine;
    pending_messages.clear();
}

void unbind() noexcept {
    bound_engine = nullptr;
    pending_messages.clear();
}

seastar::future<> flush() {
    if (!bound_engine || pending_messages.empty()) {
        co_return;
    }
    auto messages = std::move(pending_messages);
    pending_messages.clear();
    for (auto& message : messages) {
        co_await bound_engine->append(std::move(message));
    }
}

bool is_initialized() noexcept {
    return bound_engine != nullptr;
}

seastar::future<> submit(LogLevel level, std::string message, std::string route_key) {
    if (!bound_engine) {
        std::fprintf(stderr, "[log_engine::compat] logger not initialized: %s\n", message.c_str());
        co_return;
    }
    co_await bound_engine->append(LogMessage{
        .level = level,
        .payload = std::move(message),
        .route_key = std::move(route_key),
    });
}

LogLine::LogLine(LogLevel level, const char* file, int line, std::string route_key)
    : _level(level)
    , _file(file ? file : "")
    , _line(line)
    , _route_key(std::move(route_key)) {
}

LogLine::~LogLine() {
    send();
}

void LogLine::send() {
    if (_sent) {
        return;
    }
    _sent = true;
    auto payload = _stream.str();
    if (payload.empty()) {
        return;
    }
    auto full_message = _file.empty()
        ? payload
        : ("[" + _file + ":" + std::to_string(_line) + "] " + payload);

    pending_messages.push_back(LogMessage{
        .level = _level,
        .payload = std::move(full_message),
        .route_key = std::move(_route_key),
    });
}

std::ostream& LogLine::stream() noexcept {
    return _stream;
}

}  // namespace log_engine::compat
