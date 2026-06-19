# Seastar Log Agent Usage Guide

This guide explains how to run `log_engine_agent` as a lightweight durable log agent. For container deployment examples, see `doc/agent-deployment.md`.

## Role

The agent sits between local log producers and downstream collectors. It accepts logs from HTTP, files, stdin, Unix sockets, TCP, or UDP, writes them first into the local Seastar log engine, and then delivers acknowledged batches to a sink.

The intended use case is an edge or gateway node that needs a local durable buffer before forwarding logs to another system.

## Minimal Local Run

Build the project first:

```bash
./script/build.sh
```

Start the agent with the sample config:

```bash
./build/log_engine_agent --config config/agent.conf -c 2
```

Send one HTTP record:

```bash
curl -fsS -X POST http://127.0.0.1:18081/v1/logs \
  -H 'Content-Type: application/json' \
  -d '{"service":"order","level":"info","message":"created order"}'
```

Check status:

```bash
curl -fsS http://127.0.0.1:18081/v1/status
```

## Input Configuration

Only enable the inputs needed by a deployment. All inputs write into the same local engine and therefore share the same durability and delivery semantics.

### HTTP Ingest

HTTP ingest is enabled by `http-ingest-address` and `http-ingest-port`.

```text
http-ingest-address=0.0.0.0
http-ingest-port=18081
max-request-bytes=1048576
```

Supported shapes:

```json
{"message":"single line","service":"api","level":"info"}
```

```json
{"records":[{"message":"a"},{"message":"b"}]}
```

Common structured fields are preserved in the agent envelope: `timestamp`, `level`, `service`, `host`, `trace_id`, and `attributes`.

### File Tail

Use `file-source-path` for one file or `file-source-glob` for multiple files:

```text
file-source-path=/var/log/app/app.log
file-source-glob=/var/log/app/*.log
source-offset-path=/var/lib/seastar-log-agent/source.offset
source-poll-ms=1000
source-max-lines=1024
```

For glob mode, offsets are tracked per source file. Rename-based rotation and truncation are detected and counted in `/v1/status` as `source_rotations`.

### Multiline Logs

Enable multiline mode when one logical event spans multiple lines, such as Java stack traces:

```text
multiline-enabled=true
multiline-start-pattern=^[0-9]{4}-[0-9]{2}-[0-9]{2}
multiline-max-lines=128
```

A line matching `multiline-start-pattern` starts a new event. Following non-matching lines are appended until the next start line or `multiline-max-lines`.

### Stdin

Stdin is useful for simple wrappers and tests:

```text
stdin-source-enabled=true
```

Run example:

```bash
tail -F /var/log/app.log | ./build/log_engine_agent --config config/agent.conf --stdin-source-enabled true -c 1
```

### Unix Socket, TCP, And UDP

Socket inputs are useful when local applications can write newline-delimited events directly to the agent:

```text
unix-socket-source-path=/run/seastar-log-agent.sock
tcp-source-port=19001
udp-source-port=19002
max-socket-connections=128
source-max-message-bytes=262144
source-max-buffer-bytes=1048576
```

TCP and Unix socket sources enforce message and connection limits. UDP is best-effort; oversized or backpressured packets are dropped and counted.

## Sink Configuration

### No Remote Sink

Use `sink-kind=none` when the agent is only acting as a local durable log engine:

```text
sink-kind=none
```

### Stdout Sink

The stdout sink is useful for local demos and debugging:

```text
sink-kind=stdout
sink-batch-size=100
```

### HTTP Sink

The HTTP sink sends idempotent JSON delivery batches:

```text
sink-kind=http
sink-http-url=http://collector.internal:9000/ingest
sink-http-headers=Authorization: Bearer token,X-Agent: edge-1
sink-http-timeout-ms=5000
sink-http-retryable-statuses=408,425,429,500,502,503,504
delivery-offset-path=/var/lib/seastar-log-agent/delivery.offset
pending-delivery-path=/var/lib/seastar-log-agent/delivery.pending
sink-batch-size=100
sink-dispatcher-concurrency=4
sink-dispatch-queue-capacity=64
delivery-scan-idle-ms=100
sink-retry-backoff-ms=1000
sink-retry-max-backoff-ms=30000
```

Each batch includes `agent_id`, `shard`, `first_sequence`, `next_sequence`, and per-record sequence metadata. Downstream receivers should use these fields for deduplication.
Each shard scans and retries independently. Batches are handed to a bounded dispatcher, whose concurrency controls the maximum number of simultaneous sink requests.

### Kafka And Object Store Sidecars

`kafka` and `object_store` are represented as sidecar-oriented strategies in this build. The agent prepares deterministic metadata and keeps the delivery batch boundary stable, but it does not link Kafka or object storage SDKs directly.

See `doc/agent-sink-strategy.md` for the sidecar contract.

## Reliability Semantics

The agent provides local durability before remote delivery:

1. Input records are appended to the local Seastar log engine.
2. The active log is protected by record CRC, sequence metadata, checkpoint files, and tail scan recovery.
3. Before remote sink delivery, the agent persists a pending delivery batch.
4. Delivery offsets advance only after the sink ACKs the batch.
5. On restart, the agent reloads pending batches and delivery offsets before scanning more records.

This gives at-least-once remote delivery. A crash can replay a bounded ACK window, so downstream sinks must deduplicate by `agent_id + shard + sequence` or by the batch range fields.

## Backpressure

Backpressure can pause sources or drop UDP input when local or remote pressure becomes high:

```text
max-buffer-bytes=1073741824
resume-buffer-bytes=536870912
max-sink-backlog-records=10000
max-recent-sink-failures=10
max-sink-latency-ms=5000
max-pending-agent-bytes=67108864
max-sink-latency-average-ms=2000
```

`/v1/status` exposes `current_backpressure_reason`, pause duration, sink failure counters, sink latency, and UDP drop counters.

## Recovery Operations

After a crash, restart the agent with the same data directory:

```bash
./build/log_engine_agent --config config/agent.conf -c 2
```

If a remote sink needs manual replay, export unacknowledged records:

```bash
./build/log_engine_agent_replay \
  --config config/agent.conf \
  --delivery-offset-path agent-delivery.offset \
  --include-archive true \
  --limit 1000
```

## Known Limitations

- Remote HTTP TLS is expected to be handled by a local TLS proxy in this build.
- Kafka and object storage are sidecar strategies, not native linked sinks.
- UDP input is best-effort and can drop records under pressure.
- Rich parsing, transforms, sampling, redaction, and schema routing are intentionally minimal compared with mature agents.
- The current low-level record layout remains text based with a JSON metadata envelope; binary record layout is a future optimization.
- At-least-once delivery requires downstream idempotency for exactly-once effects.
