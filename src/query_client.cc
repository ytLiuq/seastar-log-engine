#include <iostream>
#include <memory>
#include <optional>
#include <string>

#include <boost/program_options.hpp>
#include <fmt/format.h>
#include <grpcpp/grpcpp.h>

#include "log_engine_query.grpc.pb.h"

namespace {

using logengine::query::v1::Empty;
using logengine::query::v1::QueryRecordsReply;
using logengine::query::v1::QueryRecordsRequest;
using logengine::query::v1::QueryService;
using logengine::query::v1::RouteReply;
using logengine::query::v1::RouteRequest;
using logengine::query::v1::StatusReply;

void print_status(const StatusReply& reply) {
    fmt::print(
        "{{\"health\":\"{}\",\"health_reason\":\"{}\",\"health_reason_basis\":\"{}\",\"recovery_fallback_reason\":\"{}\",\"routing_strategy\":\"{}\",\"routing_shards\":{},\"routing_virtual_nodes\":{},\"ring_size\":{},\"log_dir\":\"{}\",\"archive_dir\":\"{}\",\"shard_file_prefix\":\"{}\",\"reader_stats\":{{\"segments_read\":{},\"archive_segments_read\":{},\"active_segments_read\":{},\"records_returned\":{},\"corrupted_segments\":{},\"corrupted_lines\":{},\"gzip_read_errors\":{}}},\"log_manager_stats\":{{\"rotate_operations\":{},\"checkpoint_write_successes\":{},\"checkpoint_write_failures\":{},\"recovery_fallbacks\":{},\"recovery_fallback_incomplete_checkpoint\":{},\"recovery_fallback_stale_checkpoint\":{},\"gzip_archive_successes\":{},\"gzip_archive_failures\":{}}},\"health_recent_errors\":{{\"reader_corrupted_segments\":{},\"reader_corrupted_lines\":{},\"reader_gzip_read_errors\":{},\"log_manager_checkpoint_failures\":{},\"log_manager_gzip_failures\":{},\"log_manager_recovery_fallbacks\":{}}}}}\n",
        reply.health(),
        reply.health_reason(),
        reply.health_reason_basis(),
        reply.recovery_fallback_reason(),
        reply.routing_strategy(),
        reply.routing_shards(),
        reply.routing_virtual_nodes(),
        reply.ring_size(),
        reply.log_dir(),
        reply.archive_dir(),
        reply.shard_file_prefix(),
        reply.reader_segments_read(),
        reply.reader_archive_segments_read(),
        reply.reader_active_segments_read(),
        reply.reader_records_returned(),
        reply.reader_corrupted_segments(),
        reply.reader_corrupted_lines(),
        reply.reader_gzip_read_errors(),
        reply.log_manager_rotate_operations(),
        reply.log_manager_checkpoint_write_successes(),
        reply.log_manager_checkpoint_write_failures(),
        reply.log_manager_recovery_fallbacks(),
        reply.log_manager_recovery_fallback_incomplete_checkpoint(),
        reply.log_manager_recovery_fallback_stale_checkpoint(),
        reply.log_manager_gzip_archive_successes(),
        reply.log_manager_gzip_archive_failures(),
        reply.health_reader_corrupted_segments_recent(),
        reply.health_reader_corrupted_lines_recent(),
        reply.health_reader_gzip_read_errors_recent(),
        reply.health_checkpoint_failures_recent(),
        reply.health_gzip_failures_recent(),
        reply.health_recovery_fallbacks_recent());
}

void print_route(const RouteReply& reply) {
    fmt::print(
        "{{\"route_key\":\"{}\",\"shard\":{},\"hash\":{},\"token\":{},\"used_local_fallback\":{}}}\n",
        reply.route_key(),
        reply.shard(),
        reply.hash(),
        reply.token(),
        reply.used_local_fallback() ? "true" : "false");
}

void print_records(const QueryRecordsReply& reply) {
    fmt::print("{{\"records\":[");
    bool first = true;
    for (const auto& record : reply.records()) {
        if (!first) {
            fmt::print(",");
        }
        first = false;
        fmt::print(
            "{{\"crc\":{},\"timestamp\":\"{}\",\"shard\":{},\"has_sequence\":{},\"sequence\":{},\"level\":\"{}\",\"payload\":\"{}\",\"raw_line\":\"{}\"}}",
            record.crc(),
            record.timestamp(),
            record.shard(),
            record.has_sequence() ? "true" : "false",
            record.sequence(),
            record.level(),
            record.payload(),
            record.raw_line());
    }
    fmt::print("]}}\n");
}

}  // namespace

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;

    bpo::options_description options("log_engine_query_client options");
    options.add_options()
        ("target", bpo::value<std::string>()->default_value("127.0.0.1:19090"), "gRPC target host:port")
        ("method", bpo::value<std::string>()->default_value("status"), "Method: status, route, records")
        ("route-key", bpo::value<std::string>()->default_value(""), "Route key for route method")
        ("shard", bpo::value<unsigned>(), "Optional shard filter for records method")
        ("seq-from", bpo::value<std::uint64_t>(), "Optional minimum sequence filter")
        ("seq-to", bpo::value<std::uint64_t>(), "Optional maximum sequence filter")
        ("time-from", bpo::value<std::string>(), "Optional minimum timestamp filter")
        ("time-to", bpo::value<std::string>(), "Optional maximum timestamp filter")
        ("limit", bpo::value<std::size_t>()->default_value(100), "Records query limit")
        ("include-archive", bpo::value<bool>()->default_value(true), "Include archive files in records query")
        ("help", "Show help");

    bpo::variables_map vm;
    bpo::store(bpo::parse_command_line(argc, argv, options), vm);
    bpo::notify(vm);

    if (vm.count("help")) {
        std::cout << options << '\n';
        return 0;
    }

    auto channel = grpc::CreateChannel(vm["target"].as<std::string>(), grpc::InsecureChannelCredentials());
    auto stub = QueryService::NewStub(channel);
    grpc::ClientContext context;

    const auto method = vm["method"].as<std::string>();
    if (method == "status") {
        Empty req;
        StatusReply reply;
        const auto status = stub->GetStatus(&context, req, &reply);
        if (!status.ok()) {
            throw std::runtime_error(status.error_message());
        }
        print_status(reply);
        return 0;
    }

    if (method == "route") {
        RouteRequest req;
        req.set_route_key(vm["route-key"].as<std::string>());
        RouteReply reply;
        const auto status = stub->Route(&context, req, &reply);
        if (!status.ok()) {
            throw std::runtime_error(status.error_message());
        }
        print_route(reply);
        return 0;
    }

    if (method == "records") {
        QueryRecordsRequest req;
        if (vm.count("shard")) {
            req.set_shard(vm["shard"].as<unsigned>());
        }
        if (vm.count("seq-from")) {
            req.set_seq_from(vm["seq-from"].as<std::uint64_t>());
        }
        if (vm.count("seq-to")) {
            req.set_seq_to(vm["seq-to"].as<std::uint64_t>());
        }
        if (vm.count("time-from")) {
            req.set_time_from(vm["time-from"].as<std::string>());
        }
        if (vm.count("time-to")) {
            req.set_time_to(vm["time-to"].as<std::string>());
        }
        req.set_limit(vm["limit"].as<std::size_t>());
        req.set_include_archive(vm["include-archive"].as<bool>());

        QueryRecordsReply reply;
        const auto status = stub->QueryRecords(&context, req, &reply);
        if (!status.ok()) {
            throw std::runtime_error(status.error_message());
        }
        print_records(reply);
        return 0;
    }

    throw std::invalid_argument("method must be status, route, or records");
}
