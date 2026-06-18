# Agent Sink Strategy

## Pluggable Sink Boundary

The agent runtime now routes sink delivery through a small `AgentSink` interface. The production-ready built-in sinks are:

- `http`: sends idempotent delivery batches to an HTTP endpoint.
- `stdout`: writes normalized records locally.
- `none`: disables remote delivery.

`kafka` and `object_store` are intentionally represented as sidecar-oriented strategies in this build. They preserve the same idempotency metadata (`agent_id`, `shard`, `first_sequence`, `next_sequence`) without forcing this lightweight edge agent to link large client SDKs.

## HTTP TLS Mode

The current built-in HTTP sink accepts `http://` endpoints. For TLS deployments, run a local TLS proxy such as Envoy, Nginx, HAProxy, or stunnel next to the agent:

```text
agent --sink-kind=http --sink-http-url=http://127.0.0.1:19090/ingest
local TLS proxy -> https://remote-collector.example.com/ingest
```

This keeps Seastar file I/O and delivery retry semantics inside the agent, while TLS certificates, rotation, mTLS, and remote policy are handled by the proxy layer. A native Seastar TLS sink can be added later behind the same `AgentSink` interface.

## Kafka Sidecar Strategy

When `sink-kind=kafka` is configured, the agent creates Kafka-sidecar metadata with:

- topic
- bootstrap server metadata
- message key: `shard:first_sequence`
- headers: `shard`, `first_sequence`, `next_sequence`
- payload: the normal delivery batch JSON

The current binary does not link `librdkafka`; it fails fast with a payload preview so operators can wire the same batch contract to a sidecar or future native sink.

## Object Store Strategy

When `sink-kind=object_store` is configured, the agent creates a deterministic manifest:

- object name: `<prefix>/shard-<shard>/<first_sequence>-<next_sequence>.json`
- commit marker: `<object_name>.commit`
- record count and sequence range
- optional compression metadata

The object data should be uploaded first, and the commit marker should be written only after upload success. This gives replay-safe at-least-once semantics and lets downstream jobs treat commit markers as durable visibility boundaries.
