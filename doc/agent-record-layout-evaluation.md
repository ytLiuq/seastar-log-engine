# Agent Record Layout Evaluation

## Current Choice

The agent now stores source metadata as a backward-compatible JSON envelope inside the existing record payload:

```json
{
  "message": "raw log line",
  "agent_id": "edge-agent-1",
  "source_id": "/var/log/app.log",
  "source_offset": 12345,
  "ingest_timestamp": "2026-06-17T14:30:00Z",
  "attributes": {
    "host": "edge-1",
    "trace_id": "trace-1"
  }
}
```

The reader recognizes this envelope and exposes the normalized message as `ParsedRecord::payload`, while preserving `agent_id`, `source_id`, `source_offset`, `ingest_timestamp`, and `attributes` as queryable metadata. Existing records that do not use the envelope continue to parse as plain payload records.

## Why Not Rewrite To Binary Yet

The current text record layout already participates in CRC verification, sequence recovery, checkpoint tail scan, archive reading, and query output. Replacing it with a binary layout in one step would require a versioned parser, recovery migration logic, mixed archive support, and new corruption-boundary tests.

For the agent milestone, the JSON envelope gives the required semantics with much lower compatibility risk:

- Existing CRC and sequence verification continue to protect the full serialized record.
- Checkpoint and tail-scan recovery do not need a new framing format.
- Query clients can read both old records and agent records during migration.
- Replay export can use the normalized payload and sequence metadata without rewriting old logs.

## Future Binary Layout Sketch

A future format can reduce parsing overhead once the agent path is stable:

```text
magic/version
header_length
payload_length
record_crc
shard
sequence
timestamp
metadata_count
metadata key/value entries
payload bytes
```

The migration path should be additive:

1. Add a version marker so the reader can detect text records versus binary records.
2. Keep CRC and sequence semantics identical at the `ParsedRecord` layer.
3. Write new active logs in binary only after mixed-format reads are covered by tests.
4. Keep archive and recovery tests for old text logs until a documented retention boundary expires.

This keeps the external query and replay behavior stable while allowing a later performance-focused layout change.
