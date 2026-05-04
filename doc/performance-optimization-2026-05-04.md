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
- pending new optimization from the same date is recorded below after validation

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

## Accepted Optimization

Commit:

- to be recorded after push: batch round-robin reservation for empty-key grouped submit

Changes:

- for `append_batch()` under `empty-route-policy=round_robin`, empty route keys no longer call `_rr_counter.fetch_add(1)` per message
- the batch first counts empty-key records, reserves one contiguous round-robin index range, and then assigns shard targets from that reserved range

### Why this was accepted

After enabling round-robin for empty keys, grouped submit still paid one atomic increment per empty-key record.

That overhead is pure control-plane cost in the exact scenario where the caller is trying to batch many empty-key writes together.

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
  --empty-route-policy round_robin \
  --submit-group-size 16 \
  -c 4
```

Results:

| Version | Throughput (msg/s) | P95 Group Submit (us) | P99 Group Submit (us) |
| --- | ---: | ---: | ---: |
| before batch reservation | `865220.31` | `475` | `2661` |
| after batch reservation | `959140.61` | `383` | `2200` |
| after rerun | `1084334.08` | `276` | `1905` |

Interpretation:

- first confirmed run improved throughput by about `10.9%`
- rerun stayed directionally positive and crossed `1.08M msg/s`
- tail latency also improved rather than regressed, which makes this a clean acceptance

## Accepted Optimization

Commit:

- to be recorded after push: same-shard batch fast path

Changes:

- `append_batch()` now detects when every record in the batch routes to the same shard
- in that case it skips building per-shard buckets and skips the generic fanout path
- it directly calls one `submit_many()` on the local shard or one `invoke_on()` for the remote shard

### Why this was accepted

The grouped-submit path was still paying the full partition/fanout control cost even when the whole batch obviously belonged to one shard.

This showed up in workloads such as:

- `route_keys=1`
- repeated hot-key traffic
- grouped submit with larger payloads

### Benchmark signal

Command shape:

```bash
./build/log_engine_bench \
  --messages 40000 \
  --payload-size 2048 \
  --batch-size 512 \
  --flush-ms 1 \
  --inflight 16 \
  --route-keys 1 \
  --submit-group-size 16 \
  -c 4
```

Results:

| Version | Throughput (msg/s) | P95 Group Submit (us) | P99 Group Submit (us) |
| --- | ---: | ---: | ---: |
| before same-shard fast path | `130604.57` | `960` | `2936` |
| after same-shard fast path | `138089.60` | `800` | `1025` |

Additional reference point:

| Payload | Throughput (msg/s) |
| ---: | ---: |
| `512` | `501573.69` |

Interpretation:

- the primary validation case improved by about `5.7%`
- tail latency also improved, which suggests this is genuinely removing control overhead instead of just shifting work around

## Accepted Optimization

Commit:

- to be recorded after push: all-empty round-robin batch partition fast path

Changes:

- when `append_batch()` sees that every record has an empty route key and `empty-route-policy=round_robin`
- it now partitions the batch directly by arithmetic shard sequence
- it skips building the intermediate `shards` vector used by the generic routing path

### Why this was accepted

After batching the round-robin counter reservation, the grouped empty-key case still paid one extra per-message bookkeeping pass to remember every target shard before bucketing.

For a batch whose routing pattern is already fully determined by:

- batch start index
- shard count
- record position in the batch

that indirection is unnecessary.

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
  --empty-route-policy round_robin \
  --submit-group-size 16 \
  -c 4
```

Results:

| Version | Throughput (msg/s) | P95 Group Submit (us) | P99 Group Submit (us) |
| --- | ---: | ---: | ---: |
| before direct all-empty partition | `986144.67` | `347` | `2044` |
| after direct all-empty partition | `1123658.63` | `270` | `1905` |

Interpretation:

- throughput improved by about `13.9%`
- tail latency improved together with throughput
- the effect is large enough to justify keeping a dedicated fast path for this workload shape

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
- Once empty-key traffic is allowed to spread, reducing round-robin control overhead inside grouped submit produces another measurable gain.
- If a workload emits many records without route keys, `empty-route-policy=round_robin` is now the right benchmark setting when the goal is to evaluate multi-shard ingest capacity rather than single-shard locality.
- The next likely high-value optimization is still deeper batching inside the target shard:
  - batch record encoding
  - fewer small allocations
  - a more contiguous staging/write path

## Recommended Next Steps

1. Add a true batch-encode path on the target shard instead of formatting one record at a time into many small buffers.
2. Measure per-shard skew and remote-submit ratios so routing quality can be separated from writer overhead.
3. Re-test grouped submit under `payload=512` and `payload=2048` after batch-encode work, since that is where remaining overhead is most visible.
