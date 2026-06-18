# Agent Comparison With Vector And Filebeat

This note clarifies the positioning of this project if it is packaged as a log agent.

## Project Positioning

`seastar-log-agent` is best described as a lightweight durable edge buffer plus local log engine. It is useful when a node needs to accept local logs, persist them in a high-throughput append-only engine, and forward them later with at-least-once semantics.

It should not be described as a full replacement for mature production agents such as Vector or Filebeat. Those projects have much broader ecosystems, more parsers, more sinks, richer transforms, and longer operational histories.

## Summary Table

| Area | seastar-log-agent | Vector | Filebeat |
| --- | --- | --- | --- |
| Core focus | Durable local buffering and Seastar-based append/query path | General-purpose observability pipeline | Lightweight log shipper for the Elastic ecosystem |
| Local durability | First-class local engine with checkpoint, CRC, tail scan, archive, delivery offsets | Supports buffering, including disk buffers depending on config | Registry and spool/queue behavior focused on shipping |
| Runtime model | Seastar reactor/future, per-shard writers | Rust async pipeline | Go-based Beat runtime |
| Inputs | HTTP, file/glob, stdin, Unix socket, TCP, UDP | Very broad source catalog | Strong file/container/journald style sources |
| Sinks | stdout, HTTP, sidecar contracts for Kafka/object store | Broad sink catalog | Strong Elasticsearch/Logstash output path plus supported outputs |
| Transforms | Minimal structured parsing and metadata envelope | Rich transform language and routing | Processors for common enrichment/filtering |
| Delivery semantics | At-least-once remote delivery with sink-side dedup metadata | Depends on source, buffer, sink, and ACK configuration | Depends on output ACK and registry behavior |
| Best fit | Edge gateways that value local append durability and replay | Production observability pipelines needing many integrations | Elastic-oriented log collection |

## Where This Agent Is Strong

- Local durability is explicit. Accepted records are stored in the local Seastar log engine before remote delivery.
- Recovery logic is inspectable: checkpoint CRC, stale checkpoint fallback, tail scan, and corrupted-tail boundaries are visible in status and tests.
- Remote replay has clear idempotency metadata: `agent_id`, `shard`, `first_sequence`, `next_sequence`, and per-record sequence.
- The same local data can support query, replay, and sink delivery.
- Seastar's shard-oriented model fits high-throughput append workloads and predictable local buffering.

## Where Vector Or Filebeat Are Stronger

- Rich parsing, transforms, remapping, redaction, sampling, and routing.
- Large sink/source ecosystems.
- Production-grade TLS, authentication, and cloud integrations.
- Kubernetes discovery, container metadata enrichment, and operational polish.
- Wider community validation and existing runbooks.

## Recommended Messaging

Good positioning:

```text
A Seastar-based durable edge log buffer and lightweight agent that stores accepted logs locally, recovers through checkpoint plus tail scan, and forwards idempotent batches with at-least-once semantics.
```

Avoid this claim:

```text
A complete replacement for Vector or Filebeat.
```

More accurate claim:

```text
It complements mature log agents in scenarios where local durable buffering, replay, and high-throughput append behavior are the main requirements.
```

## Practical Deployment Choices

Use this agent when:

- the node is an edge or IoT gateway with intermittent uplink.
- local replay after remote collector downtime matters.
- the application can tolerate at-least-once delivery and downstream deduplication.
- the pipeline is simple enough for HTTP/stdout/sidecar delivery.
- local query and recovery inspection are useful.

Use Vector or Filebeat when:

- the pipeline needs many built-in integrations.
- logs require complex transforms or redaction.
- the deployment needs first-class Kubernetes/service discovery features.
- native TLS, authentication, and mature sink clients are mandatory.
- operational teams already standardize on Elastic or Vector.

## Integration Pattern

This project can also sit in front of a mature agent:

```text
local apps -> seastar-log-agent durable buffer -> HTTP sink -> Vector/Filebeat/custom collector -> central backend
```

In that layout, `seastar-log-agent` handles local durability and replay, while Vector or Filebeat handles rich processing and broad backend integration.
