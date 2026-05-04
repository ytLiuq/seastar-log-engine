# Performance Optimization Notes

Date: 2026-05-04

## Scope

This note records the multi-shard write-path optimization pass completed on 2026-05-04.

It separates:

- accepted optimizations that were committed and pushed
- rejected experiments that showed mixed or negative feedback

## Accepted Optimization

Commit:

- `daf70d4` `Improve multi-shard empty-key routing`

Changes in this commit:

- `LogEngine::append_batch()` no longer blocks on local shard submission before dispatching remote shard work
- shard fanout now reserves per-shard vectors after a counting pass, reducing avoidable reallocations
- added `empty-route-policy=local|round_robin`
- empty `route_key` can now be distributed across shards with round-robin instead of always falling back to the caller shard
- wired the new policy into config loading, `demo`, `bench`, README, and unit tests

### Why this was accepted

The previous behavior guaranteed that `route_keys=0` would collapse to a single effective shard when the caller stayed on one shard.

That made the multi-shard configuration look unhealthy even when the actual write path could scale if traffic were distributed.

### Benchmark signal

Command shape:

```bash
./build/log_engine_bench \
  --messages 40000 \
  --payload-size 512 \
  --batch-size 512 \
  --flush-ms 1 \
  --inflight 16 \
  --route-keys 0 \
  --submit-group-size 1 \
  -c 4
```

Results:

| Empty Route Policy | Throughput (msg/s) | P99 Submit (us) |
| --- | ---: | ---: |
| `local` | `499818.82` | `1` |
| `round_robin` | `641241.44` | `63` |

Interpretation:

- throughput improved by about `28.3%`
- tail latency increased from near-zero to tens of microseconds, which is expected because traffic is now actually crossing shards
- this is still a net positive because the old `local` behavior was effectively suppressing multi-shard parallelism for empty-key traffic

Additional reference point under distributed keys:

| Route Keys | Submit Group | Payload | Throughput (msg/s) |
| ---: | ---: | ---: | ---: |
| `16` | `1` | `2048` | `383002.36` |
| `16` | `16` | `2048` | `399058.22` |

This confirms that the earlier batched cross-shard submit path is still directionally helpful when routing already spreads traffic.

## Rejected Experiment

Experiment:

- remove duplicate `O(n)` batch-byte scans during flush
- specifically, stop recomputing batch byte size in both `AsyncWriter::flush_once()` and `AppendWriter::append_batch()`

### Why it looked promising

The flush path was scanning the same batch twice just to sum bytes:

- once before calling into `AppendWriter`
- once again inside `AppendWriter`

For larger batch sizes this is pure CPU overhead.

### Measured outcome

Positive or neutral cases:

| Scenario | Before (msg/s) | After (msg/s) | Delta |
| --- | ---: | ---: | ---: |
| `route_keys=0`, `empty-route-policy=round_robin`, `payload=512`, `submit_group_size=1`, `-c 4` | `641241.44` | `744227.58` | `+16.1%` |
| `route_keys=16`, `payload=2048`, `submit_group_size=1`, `-c 4` | `383002.36` | `389947.16` | `+1.8%` |

Negative cases:

| Scenario | Before (msg/s) | After (msg/s) | Delta |
| --- | ---: | ---: | ---: |
| `route_keys=16`, `payload=2048`, `submit_group_size=16`, `-c 4` | `399058.22` | `387886.31` | `-2.8%` |
| `route_keys=16`, `payload=2048`, `submit_group_size=16`, `-c 4` rerun | `399058.22` | `369313.72` | `-7.5%` |

Decision:

- not committed
- not pushed

Reason:

- the gain was not stable across the grouped-submit case
- the optimization is too small to justify carrying it with mixed benchmark feedback

## Current Takeaways

- The most material bottleneck fixed on 2026-05-04 was distribution of empty-key traffic, not raw record encoding cost.
- If a workload emits many records without route keys, `empty-route-policy=round_robin` is now the right benchmark setting when the goal is to evaluate multi-shard ingest capacity rather than single-shard locality.
- The next likely high-value optimization is still deeper batching inside the target shard:
  - batch record encoding
  - fewer small allocations
  - a more contiguous staging/write path

## Recommended Next Steps

1. Add a true batch-encode path on the target shard instead of formatting one record at a time into many small buffers.
2. Measure per-shard skew and remote-submit ratios so routing quality can be separated from writer overhead.
3. Re-test grouped submit under `payload=512` and `payload=2048` after batch-encode work, since that is where remaining overhead is most visible.
