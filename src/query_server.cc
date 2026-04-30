#include <atomic>
#include <charconv>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <boost/program_options.hpp>
#include <fmt/format.h>
#include <grpcpp/grpcpp.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/api.hh>
#include <seastar/util/defer.hh>

#include "../../seastar/apps/lib/stop_signal.hh"
#include "log_engine/config_loader.hh"
#include "log_engine/log_reader.hh"
#include "log_engine/routing.hh"
#include "log_engine_query.grpc.pb.h"

namespace {

using logengine::query::v1::Empty;
using logengine::query::v1::QueryRecord;
using logengine::query::v1::QueryRecordsReply;
using logengine::query::v1::QueryRecordsRequest;
using logengine::query::v1::QueryService;
using logengine::query::v1::RouteReply;
using logengine::query::v1::RouteRequest;
using logengine::query::v1::StatusReply;

std::string json_escape(std::string_view value) {
    std::string escaped;
    escaped.reserve(value.size() + 8);
    for (const char ch : value) {
        switch (ch) {
        case '\\':
            escaped += "\\\\";
            break;
        case '"':
            escaped += "\\\"";
            break;
        case '\n':
            escaped += "\\n";
            break;
        case '\r':
            escaped += "\\r";
            break;
        case '\t':
            escaped += "\\t";
            break;
        default:
            escaped.push_back(ch);
            break;
        }
    }
    return escaped;
}

template <typename T>
std::optional<T> parse_integer(std::string_view value) {
    if (value.empty()) {
        return std::nullopt;
    }
    T parsed{};
    const auto res = std::from_chars(value.data(), value.data() + value.size(), parsed, 10);
    if (res.ec != std::errc{} || res.ptr != value.data() + value.size()) {
        return std::nullopt;
    }
    return parsed;
}

bool parse_bool_or_default(std::string_view value, bool fallback) {
    if (value.empty()) {
        return fallback;
    }
    if (value == "1" || value == "true" || value == "yes" || value == "on") {
        return true;
    }
    if (value == "0" || value == "false" || value == "no" || value == "off") {
        return false;
    }
    throw std::invalid_argument("invalid bool query parameter");
}

struct QueryContext {
    log_engine::EngineConfig config;
    log_engine::ShardRouter router;
    unsigned routing_shards = 0;

    log_engine::RouteDecision route(std::string_view route_key) const noexcept {
        return router.route(route_key, 0);
    }

    std::vector<log_engine::ParsedRecord> query_records(const log_engine::ReadQuery& query) const {
        const auto segments = log_engine::collect_segments(config, query);
        return log_engine::read_records(segments, query);
    }

    std::string render_status_json() const {
        return fmt::format(
            "{{\"routing_strategy\":\"{}\",\"routing_shards\":{},\"routing_virtual_nodes\":{},\"ring_size\":{},\"log_dir\":\"{}\",\"archive_dir\":\"{}\",\"shard_file_prefix\":\"{}\"}}",
            log_engine::routing_strategy_to_string(router.strategy()),
            routing_shards,
            router.virtual_nodes(),
            router.ring_size(),
            json_escape(config.log_dir),
            json_escape(config.archive_dir),
            json_escape(config.shard_file_prefix));
    }

    std::string render_route_json(std::string_view key) const {
        const auto decision = route(key);
        return fmt::format(
            "{{\"route_key\":\"{}\",\"shard\":{},\"hash\":{},\"token\":{},\"used_local_fallback\":{}}}",
            json_escape(key),
            decision.shard,
            decision.hash,
            decision.token,
            decision.used_local_fallback ? "true" : "false");
    }

    std::string render_records_json(const log_engine::ReadQuery& query) const {
        const auto records = query_records(query);
        std::string out = "{\"records\":[";
        bool first = true;
        for (const auto& record : records) {
            if (!first) {
                out += ',';
            }
            first = false;
            out += fmt::format(
                "{{\"crc\":{},\"timestamp\":\"{}\",\"shard\":{},\"has_sequence\":{},\"sequence\":{},\"level\":\"{}\",\"payload\":\"{}\",\"raw_line\":\"{}\"}}",
                record.crc,
                json_escape(record.timestamp),
                record.shard,
                record.has_sequence ? "true" : "false",
                record.sequence,
                log_engine::level_to_string(record.level),
                json_escape(record.payload),
                json_escape(record.raw_line));
        }
        out += "]}";
        return out;
    }

    void fill_status(StatusReply* reply) const {
        reply->set_routing_strategy(log_engine::routing_strategy_to_string(router.strategy()));
        reply->set_routing_shards(routing_shards);
        reply->set_routing_virtual_nodes(router.virtual_nodes());
        reply->set_ring_size(router.ring_size());
        reply->set_log_dir(config.log_dir);
        reply->set_archive_dir(config.archive_dir);
        reply->set_shard_file_prefix(config.shard_file_prefix);
    }

    void fill_route(std::string_view key, RouteReply* reply) const {
        const auto decision = route(key);
        reply->set_shard(decision.shard);
        reply->set_hash(decision.hash);
        reply->set_token(decision.token);
        reply->set_used_local_fallback(decision.used_local_fallback);
    }

    void fill_records(const log_engine::ReadQuery& query, QueryRecordsReply* reply) const {
        const auto records = query_records(query);
        for (const auto& record : records) {
            auto* out = reply->add_records();
            out->set_crc(record.crc);
            out->set_timestamp(record.timestamp);
            out->set_shard(record.shard);
            out->set_has_sequence(record.has_sequence);
            out->set_sequence(record.sequence);
            out->set_level(log_engine::level_to_string(record.level));
            out->set_payload(record.payload);
            out->set_raw_line(record.raw_line);
        }
    }
};

log_engine::ReadQuery read_query_from_http(const seastar::httpd::const_req& req) {
    log_engine::ReadQuery query;
    if (req.has_query_param("shard")) {
        const auto value = parse_integer<unsigned>(req.get_query_param("shard"));
        if (!value) {
            throw std::invalid_argument("invalid shard");
        }
        query.shard = *value;
    }
    if (req.has_query_param("seq_from")) {
        const auto value = parse_integer<std::uint64_t>(req.get_query_param("seq_from"));
        if (!value) {
            throw std::invalid_argument("invalid seq_from");
        }
        query.seq_from = *value;
    }
    if (req.has_query_param("seq_to")) {
        const auto value = parse_integer<std::uint64_t>(req.get_query_param("seq_to"));
        if (!value) {
            throw std::invalid_argument("invalid seq_to");
        }
        query.seq_to = *value;
    }
    if (req.has_query_param("time_from")) {
        query.time_from = req.get_query_param("time_from");
    }
    if (req.has_query_param("time_to")) {
        query.time_to = req.get_query_param("time_to");
    }
    query.limit = parse_integer<std::size_t>(req.get_query_param("limit", "100")).value_or(100);
    query.include_archive = parse_bool_or_default(req.get_query_param("include_archive", "true"), true);
    return query;
}

log_engine::ReadQuery read_query_from_proto(const QueryRecordsRequest& req) {
    log_engine::ReadQuery query;
    if (req.has_shard()) {
        query.shard = req.shard();
    }
    if (req.has_seq_from()) {
        query.seq_from = req.seq_from();
    }
    if (req.has_seq_to()) {
        query.seq_to = req.seq_to();
    }
    if (req.has_time_from()) {
        query.time_from = req.time_from();
    }
    if (req.has_time_to()) {
        query.time_to = req.time_to();
    }
    query.limit = req.limit() == 0 ? 100 : static_cast<std::size_t>(req.limit());
    query.include_archive = req.include_archive();
    return query;
}

class GrpcQueryService final : public QueryService::Service {
public:
    explicit GrpcQueryService(const QueryContext& context)
        : _context(context) {
    }

    grpc::Status GetStatus(grpc::ServerContext*, const Empty*, StatusReply* reply) override {
        _context.fill_status(reply);
        return grpc::Status::OK;
    }

    grpc::Status Route(grpc::ServerContext*, const RouteRequest* req, RouteReply* reply) override {
        _context.fill_route(req->route_key(), reply);
        return grpc::Status::OK;
    }

    grpc::Status QueryRecords(grpc::ServerContext*, const QueryRecordsRequest* req, QueryRecordsReply* reply) override {
        _context.fill_records(read_query_from_proto(*req), reply);
        return grpc::Status::OK;
    }

private:
    const QueryContext& _context;
};

class GrpcServerRunner {
public:
    explicit GrpcServerRunner(const QueryContext& context)
        : _service(context) {
    }

    void start(const std::string& address) {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        builder.RegisterService(&_service);
        _server = builder.BuildAndStart();
        if (!_server) {
            throw std::runtime_error("failed to start gRPC server");
        }
        _thread = std::thread([this] {
            _server->Wait();
        });
    }

    void stop() {
        if (_server) {
            _server->Shutdown();
        }
        if (_thread.joinable()) {
            _thread.join();
        }
        _server.reset();
    }

    ~GrpcServerRunner() {
        stop();
    }

private:
    GrpcQueryService _service;
    std::unique_ptr<grpc::Server> _server;
    std::thread _thread;
};

void set_query_routes(seastar::httpd::routes& routes, const QueryContext& context) {
    using namespace seastar::httpd;

    routes.add(operation_type::GET, url("/healthz"), new function_handler([](const_req) {
        return seastar::sstring("{\"ok\":true}");
    }, "json"));

    routes.add(operation_type::GET, url("/v1/status"), new function_handler([&context](const_req, seastar::http::reply&) {
        return seastar::sstring(context.render_status_json());
    }, "json"));

    routes.add(operation_type::GET, url("/v1/route"), new function_handler([&context](const_req req, seastar::http::reply& rep) {
        if (!req.has_query_param("key")) {
            rep.set_status(seastar::http::reply::status_type::bad_request);
            return seastar::sstring("{\"error\":\"missing key query parameter\"}");
        }
        return seastar::sstring(context.render_route_json(req.get_query_param("key")));
    }, "json"));

    routes.add(operation_type::GET, url("/v1/records"), new function_handler([&context](const_req req, seastar::http::reply& rep) {
        try {
            return seastar::sstring(context.render_records_json(read_query_from_http(req)));
        } catch (const std::exception& ex) {
            rep.set_status(seastar::http::reply::status_type::bad_request);
            return seastar::sstring(fmt::format("{{\"error\":\"{}\"}}", json_escape(ex.what())));
        }
    }, "json"));
}

}  // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    namespace bpo = boost::program_options;

    app.add_options()
        ("config", bpo::value<std::string>()->default_value(""), "Path to key=value config file")
        ("log-dir", bpo::value<std::string>()->default_value("logs"), "Directory for shard log files")
        ("archive-dir", bpo::value<std::string>()->default_value("archive"), "Directory for archived log files")
        ("shard-file-prefix", bpo::value<std::string>()->default_value("shard"), "Shard log file prefix")
        ("routing-strategy", bpo::value<std::string>()->default_value("hash_modulo"), "Routing strategy: hash_modulo or consistent_hashing")
        ("routing-virtual-nodes", bpo::value<std::size_t>()->default_value(128), "Virtual nodes per shard for consistent hashing")
        ("routing-shards", bpo::value<unsigned>()->default_value(0), "Shard count used for route queries, 0 uses current Seastar shard count")
        ("http-address", bpo::value<std::string>()->default_value("0.0.0.0"), "HTTP query listen address")
        ("http-port", bpo::value<uint16_t>()->default_value(18080), "HTTP query port")
        ("grpc-address", bpo::value<std::string>()->default_value("0.0.0.0"), "gRPC query listen address")
        ("grpc-port", bpo::value<uint16_t>()->default_value(19090), "gRPC query port")
        ("metrics-address", bpo::value<std::string>()->default_value("0.0.0.0"), "Prometheus metrics listen address")
        ("metrics-port", bpo::value<uint16_t>()->default_value(19181), "Prometheus metrics port, 0 disables");

    return app.run(argc, argv, [&app] {
        return seastar::async([&app] {
            seastar_apps_lib::stop_signal stop_signal;
            auto& options = app.configuration();

            log_engine::EngineConfig base;
            base.log_dir = options["log-dir"].as<std::string>();
            base.archive_dir = options["archive-dir"].as<std::string>();
            base.shard_file_prefix = options["shard-file-prefix"].as<std::string>();
            base.routing_strategy = log_engine::parse_routing_strategy(options["routing-strategy"].as<std::string>());
            base.routing_virtual_nodes = options["routing-virtual-nodes"].as<std::size_t>();

            const auto file_values = log_engine::load_config_file(options["config"].as<std::string>());
            auto config = log_engine::apply_engine_config_overrides(base, options, file_values);

            QueryContext context;
            context.config = config;
            context.routing_shards = options["routing-shards"].as<unsigned>();
            if (context.routing_shards == 0) {
                context.routing_shards = seastar::smp::count;
            }
            context.router.configure(config.routing_strategy, config.routing_virtual_nodes, context.routing_shards);

            seastar::httpd::http_server_control http_server;
            http_server.start("log-engine-query").get();
            auto stop_http = seastar::defer([&http_server] () noexcept {
                http_server.stop().get();
            });
            http_server.set_routes([&context](seastar::httpd::routes& routes) {
                set_query_routes(routes, context);
            }).get();
            http_server.listen(
                seastar::socket_address{
                    seastar::net::inet_address(options["http-address"].as<std::string>()),
                    options["http-port"].as<uint16_t>()}).get();

            seastar::httpd::http_server_control prometheus_server;
            const auto metrics_port = options["metrics-port"].as<uint16_t>();
            bool prometheus_started = false;
            auto stop_prometheus = seastar::defer([&prometheus_server, &prometheus_started] () noexcept {
                if (prometheus_started) {
                    prometheus_server.stop().get();
                }
            });
            if (metrics_port != 0) {
                prometheus_server.start("prometheus").get();
                seastar::prometheus::config metrics_config;
                metrics_config.prefix = "log_engine";
                seastar::prometheus::start(prometheus_server, metrics_config).get();
                prometheus_server.listen(
                    seastar::socket_address{
                        seastar::net::inet_address(options["metrics-address"].as<std::string>()),
                        metrics_port}).get();
                prometheus_started = true;
            }

            GrpcServerRunner grpc_server(context);
            grpc_server.start(fmt::format(
                "{}:{}",
                options["grpc-address"].as<std::string>(),
                options["grpc-port"].as<uint16_t>()));

            stop_signal.wait().get();
            grpc_server.stop();
        });
    });
}
