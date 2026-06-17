# Seastar Log Agent Roadmap TODO

This document tracks follow-up work after the current agent/checkpoint iteration.

## Completed

- 2026-06-17: Added durable pending delivery batches. The agent persists a batch to `pending-delivery-path` before sink delivery, reloads it on restart, advances per-shard delivery offsets only after sink ACK, and removes the pending file after ACK.
- 2026-06-17: Added sink idempotency metadata to HTTP delivery batches: `agent_id`, `shard`, `first_sequence`, `next_sequence`, and per-record `sequence`.
- 2026-06-17: Exposed `last_recovery_fallback_reason` through `/v1/status`.
- 2026-06-17: Added unit coverage for pending delivery batch roundtrip, async HTTP sink metadata, and file-to-HTTP delivery offset flow.
- 2026-06-17: Added `log_engine_agent_replay`, an offline replay helper that exports unacknowledged local-engine records from per-shard delivery offsets as idempotent JSON batches.

## Current Baseline

- Checkpoint recovery uses a checksummed v2 checkpoint file plus tail scan after the checkpoint offset.
- Startup can recover valid records after the checkpoint and stop at a corrupted tail.
- Agent MVP supports HTTP ingest, file tailing, glob file inputs, stdin, Unix socket, TCP, UDP, stdout sink, HTTP sink, disk quota checks, dynamic backpressure inputs, Docker runtime image, and Docker Compose deployment.
- Delivery state can be tracked per shard, pending sink batches are persisted before delivery, and unit tests cover source offsets, delivery offsets, pending delivery batches, file tailing, glob expansion, backpressure decisions, async HTTP sink behavior, and a file-to-HTTP delivery flow.

## P0 Reliability

- Make sink delivery fully at-least-once:
  - [x] Persist retry batches before sending to remote sinks.
  - [x] Advance per-shard delivery offsets only after sink ACK.
  - [x] On restart, reload pending delivery batch and delivery offsets before scanning new records.
  - [x] Add idempotency fields to sink batches, such as `agent_id`, `shard`, `first_sequence`, and `next_sequence`.
  - [x] Add a replay helper that can explicitly export unsent local-engine records from delivery offsets for offline recovery.

- Harden crash/restart behavior:
  - Add an integration test that kills the agent between local append and sink ACK.
  - Add an integration test that kills the agent after sink ACK but before offset persistence.
  - Verify duplicate window is bounded and explain sink-side dedup expectations.

- Improve checkpoint durability:
  - Keep current checkpoint CRC and fsync behavior.
  - Add fault-injection tests for partial checkpoint write, stale checkpoint, corrupted checkpoint CRC, and corrupted active log tail.
  - [x] Expose the last recovery fallback reason through `/v1/status`.
  - [ ] Track detailed recovery mode counters in metrics.

## P1 Sink Architecture

- Introduce a pluggable sink interface:
  - `HttpSink`
  - `StdoutSink`
  - future `KafkaSink`
  - future `ObjectStoreSink`

- HTTP sink improvements:
  - Support configurable headers.
  - Support request timeout.
  - Support retryable status codes.
  - Support TLS later, either through Seastar TLS support or a clearly documented proxy mode.

- Kafka sink:
  - Decide whether to link `librdkafka` or keep Kafka as an external sidecar target.
  - Use shard/sequence metadata as message keys or headers.
  - Add delivery ACK handling and retry policy.

- Object storage sink:
  - Batch records into compressed objects.
  - Use deterministic object names based on shard and sequence range.
  - Write a manifest or commit marker only after object upload succeeds.

## P1 Input Sources

- File source:
  - Track one source offset per file when using glob.
  - Detect rename-based rotation and truncation more explicitly.
  - Add multiline log support with a configurable start-line pattern.

- Socket sources:
  - Add connection limits and request-size limits for TCP and Unix socket sources.
  - Add UDP drop counters.
  - Add tests for malformed input and oversized messages.

- HTTP ingest:
  - Add batch ingest endpoint.
  - Add structured JSON parsing for common fields like timestamp, level, service, host, trace id, and attributes.

## P1 Backpressure

- Replace threshold-only backpressure with a small controller:
  - Disk usage high watermark and low watermark.
  - Pending bytes in local engine.
  - Sink backlog size.
  - Recent sink failure rate.
  - Sink latency moving average.

- Make source pause/resume observable:
  - Expose current backpressure reason in `/v1/status`.
  - Add counters for pause duration and dropped UDP records.

## P2 Record Layout And Query

- Add an agent-oriented record schema:
  - `source_id`
  - `source_offset`
  - `agent_id`
  - `ingest_timestamp`
  - `attributes`

- Add query support for agent metadata:
  - Filter by source.
  - Filter by shard and sequence range.
  - Export records in sink batch format for replay.

- Evaluate binary record layout:
  - Reduce parsing overhead versus text records.
  - Preserve CRC and sequence verification.
  - Keep a migration path from the current text layout.

## P2 Deployment

- Expand Docker Compose examples:
  - stdout-only local demo.
  - HTTP sink demo with a fake receiver.
  - mounted host log directory.

- Kubernetes packaging:
  - Provide DaemonSet example.
  - Mount host log directories read-only.
  - Mount agent data directory as a writable volume.
  - Add readiness and liveness probes.

- Non-root operation:
  - Keep runtime container non-root.
  - Document required permissions for reading host log files and binding Unix sockets.

## P2 Testing

- Add integration tests:
  - crash/restart replay test.
  - fake HTTP sink retry and recovery test.
  - rotate plus tail plus delivery offset test.
  - disk-full or permission-error test.
  - Docker Compose smoke test.

- Add performance tests:
  - file tail throughput.
  - HTTP sink batch size sweep.
  - backpressure behavior under slow sink.
  - memory usage under large payload records.

## P3 Documentation

- Add an agent usage guide:
  - input configuration examples.
  - sink configuration examples.
  - reliability semantics.
  - known limitations.

- Add an architecture diagram:
  - input source.
  - local Seastar log engine.
  - checkpoint and tail scan recovery.
  - delivery offset.
  - remote sink.

- Add a Vector/Filebeat comparison note:
  - clarify that this project is a lightweight durable edge buffer plus local log engine.
  - avoid claiming it replaces mature production agents without Kafka/object-store/TLS/rich parsing support.
