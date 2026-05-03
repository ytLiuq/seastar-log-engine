# Troubleshooting Manual

## Health status meanings

| Status | Condition |
|--------|-----------|
| `ok` | No recent errors in the last 5-minute window |
| `degraded` | Recent reader corruption / gzip read errors / recovery fallbacks exist, but are still below unhealthy thresholds |
| `unhealthy` | Recent checkpoint write failure or gzip archive failure exists, or recent reader / recovery errors exceeded their thresholds |

Check current health: `curl http://<host>:18080/v1/status | jq .health`

Additional status fields:

- `health_reason`: primary category currently driving health
- `health_reason_basis`: currently `recent_window` when health is degraded/unhealthy
- `recovery_fallback_reason`: latest recovery fallback classification (`none`, `incomplete_checkpoint`, `stale_checkpoint`)

## Common issues

### 1. "degraded" or "unhealthy" health status

Check the `health_recent_errors` section in `/v1/status` to identify which error
category is elevated.

**reader_corrupted_segments > 0 / reader_corrupted_lines > 0:**
- Active log or archive segment has unparseable lines
- Check: `verify` tool on the segment files
- Recovery: if active log is corrupted, restart with `truncate-on-start=true` to
  truncate the damaged tail, then disable truncation
- Prevention: ensure clean shutdown, enable checkpoint

**reader_gzip_read_errors > 0:**
- Compressed archive file is truncated or corrupted
- Check: `gunzip -t <archive_path>` to verify integrity
- Recovery: remove the broken `.log.gz` file; data in that archive is lost
- Prevention: the engine already writes `.gz.tmp` then atomically renames; check
  for unexpected kills during compression

**checkpoint_write_failures > 0:**
- Disk full or permission denied on the log directory
- Check: `df -h <log_dir>`, `ls -la <log_dir>/shard-*.checkpoint`
- Recovery: free disk space, fix permissions, restart

**gzip_archive_failures > 0:**
- Disk full on archive directory or zlib error
- Check: `df -h <archive_dir>`
- Recovery: free disk space, the `.log` file is preserved (gzip failed, original kept)

**recovery_fallbacks > 0:**
- Checkpoint file is incomplete or stale (size mismatch with active log)
- Check the `log_manager_stats` section for breakdown:
  - `recovery_fallback_incomplete_checkpoint`: checkpoint file missing required fields
  - `recovery_fallback_stale_checkpoint`: checkpoint `logical_size` doesn't match verified scan
- Impact: fallback means sequence number and rotation index come from log content
  scan rather than checkpoint, which is safe but may warn
- Prevention: ensure clean shutdown or use `sync_ack` mode for stricter durability

### 2. High flush errors

- Indicates `write()` / `flush()` syscall failures on the active log file
- Check: `dmesg` for I/O errors, disk health with `smartctl`
- Check: filesystem is not read-only (`mount | grep <log_dir>`)
- The writer retries failed batches (configured via `write-retry-count`)

### 3. Sustained backpressure

- `waiting_submitters > 0` for extended periods
- Writers are blocked because `pending_bytes >= max_pending_bytes`
- Options:
  - Increase `max_pending_bytes` (more memory for queuing)
  - Decrease `batch_size` (more frequent, smaller flushes)
  - Add more shards (`-c N`) to parallelize writes
  - Check disk I/O throughput — if disk is saturated, add faster storage

### 4. Log files not rotating

- Check `rotate_size_bytes` and `rotate_interval_seconds` in config
- Rotation only triggers after a flush completes and the threshold is crossed
- If `flush-ms` is high, rotation may be delayed
- Verify: `log_engine_log_manager_rotate_operations` metric should increment

### 5. Checkpoint file missing or stale

- Check `checkpoint-enabled=true` in config
- Checkpoint is written on clean shutdown and after each rotation
- If process is killed (SIGKILL), the last checkpoint may not reflect recent writes
- The engine falls back to log content scan on recovery — data is safe, but
  recovery takes longer

### 6. Query returns fewer records than expected

- Records may be in archive files — pass `include_archive=true`
- Records before `seq_from` or after `seq_to` are filtered
- Corrupted segments stop reading at the first bad line; records after the
  corruption in that segment are skipped. Subsequent segments continue normally.
- Check `gzip_read_errors` — corrupted gzip archives are skipped entirely

### 7. Process won't start (port already in use)

- Default ports: HTTP 18080, gRPC 19090, Metrics 19181
- Change ports via CLI flags: `--http-port`, `--grpc-port`, `--metrics-port`
- Check: `ss -tlnp | grep -E '18080|19090|19181'`

### 8. Query server exits with "socket: Operation not permitted"

- Common in restricted containers or sandboxed CI environments
- Symptom:
  `std::system_error (error system:1, socket: Operation not permitted)`
- Meaning:
  the process is not allowed to bind listening sockets in the current environment
- Options:
  - run the query server on a less restricted host / container
  - change ports and retry in case the environment only blocks some bindings
  - for `script/test_soak_and_fault.sh`, use `--skip-query-checks` to keep validating recovery / rotate / fault-injection paths without HTTP/gRPC startup

## Diagnostic commands

```bash
# Full status dump
curl -s http://localhost:18080/v1/status | python3 -m json.tool

# Check specific health counters
curl -s http://localhost:18080/v1/status | jq '.health_recent_errors'

# Verify active log integrity
./build/log_engine_verify --path /data/log-engine/logs/shard-0.log

# Read recent records
curl -s "http://localhost:18080/v1/records?limit=5" | python3 -m json.tool

# Route a key to its shard
curl -s "http://localhost:18080/v1/route?key=test-key"

# Prometheus metrics snapshot
curl -s http://localhost:19181/metrics | grep '^log_engine_'
```
