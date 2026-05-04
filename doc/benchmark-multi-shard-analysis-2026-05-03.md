# Multi-Shard Performance Analysis

Date: 2026-05-03

Source data:
- `doc/bench-multi-profile-2026-05-03-report.tsv`
- `doc/bench-multi-profile-2026-05-03-report.md`

## Conclusion

Current multi-shard performance is only partially in line with expectations.

- For `route_keys=0`, multi-shard does not scale. This is expected because traffic is not distributed across shards.
- For `route_keys>0`, multi-shard improves throughput materially, especially at `payload=512` and `payload=2048`.
- `4` shards do not show healthy scaling relative to `2` shards in most distributed cases. Improvement exists, but it is far from linear.
- Small backpressure windows such as `131072` bytes are clearly too aggressive and cause large throughput collapse plus bad tail latency.

## Key Results

### 1. No distribution means no scaling

`write_ack`, `payload=512`, `route_keys=0`, `max_pending=0`:

| Shards | Throughput (msg/s) | P99 (us) |
| ---: | ---: | ---: |
| 1 | 464922 | 0 |
| 2 | 482241 | 0 |
| 4 | 448783 | 1 |

Interpretation:
- `2` shards vs `1` shard is only `+3.7%`
- `4` shards vs `1` shard is actually `-3.5%`
- This is not a multi-shard regression by itself; it shows the workload is not exercising shard fan-out

### 2. Route-key distribution unlocks real gains

`write_ack`, `payload=512`, `max_pending=0`:

| Shards | Route Keys | Throughput (msg/s) | P99 (us) |
| ---: | ---: | ---: | ---: |
| 2 | 0 | 482241 | 0 |
| 2 | 4 | 626782 | 63 |
| 2 | 16 | 670916 | 59 |
| 4 | 0 | 448783 | 1 |
| 4 | 4 | 574894 | 72 |
| 4 | 16 | 622103 | 55 |

Interpretation:
- `2` shards gains `+30.0%` to `+39.1%` once route-key distribution is enabled
- `4` shards gains `+28.1%` to `+38.6%` relative to its own `route_keys=0` baseline
- Tail latency increases from near-zero to tens of microseconds, but remains controlled

### 3. Larger payloads benefit more from distribution

`write_ack`, `payload=2048`, `max_pending=0`:

| Shards | Route Keys | Throughput (msg/s) | P99 (us) |
| ---: | ---: | ---: | ---: |
| 2 | 0 | 159422 | 2 |
| 2 | 4 | 295495 | 304 |
| 2 | 16 | 297663 | 308 |
| 4 | 0 | 152844 | 5 |
| 4 | 4 | 360926 | 273 |
| 4 | 16 | 348074 | 213 |

Interpretation:
- `2` shards improves by about `85%`
- `4` shards improves by about `128%` to `136%`
- This is the strongest evidence that multi-shard routing helps once payload cost is high enough

### 4. Four-shard scaling is still weak

Representative distributed cases:

| Payload | Route Keys | 2 Shards | 4 Shards | Gain |
| ---: | ---: | ---: | ---: | ---: |
| 128 | 4 | 812744 | 764351 | -5.9% |
| 128 | 16 | 864902 | 717051 | -17.1% |
| 512 | 4 | 626782 | 574894 | -8.3% |
| 512 | 16 | 670916 | 622103 | -7.3% |
| 2048 | 4 | 295495 | 360926 | +22.1% |
| 2048 | 16 | 297663 | 348074 | +16.9% |

Interpretation:
- `4` shards is often worse than `2` shards for small and medium payloads
- `4` shards only becomes clearly better for large payloads
- Current overheads likely dominate once per-record work is small

### 5. Small backpressure windows are unsafe

`2` shards, `write_ack`, `payload=512`, `route_keys=4`:

| Max Pending (B) | Throughput (msg/s) | P99 (us) |
| ---: | ---: | ---: |
| 0 | 626782 | 63 |
| 131072 | 241514 | 2295 |
| 524288 | 588616 | 47 |

Interpretation:
- `131072` cuts throughput by about `61%`
- P99 rises from `63us` to `2295us`
- `524288` is much closer to the no-limit baseline and is operationally safer

## Ack Mode Notes

`sync_ack` is not uniformly much worse than `write_ack` in this environment.

Examples:
- `2` shards, `payload=512`, `route_keys=4`: `write_ack=626782`, `sync_ack=577818`
- `4` shards, `payload=128`, `route_keys=0`: `write_ack=822199`, `sync_ack=866438`

Interpretation:
- The gap between ack modes is smaller than expected in several profiles
- Current bottlenecks are not dominated purely by fsync cost
- Cross-shard routing and queueing behavior appear more important than ack mode alone

## Whether It Meets Expectation

Short answer: not globally.

Meets expectation:
- No-scaling result under `route_keys=0`
- Clear throughput gain once route-key distribution is enabled
- Stronger multi-shard benefit for larger payloads

Does not meet expectation:
- `4` shards does not outperform `2` shards for many `128B` and `512B` distributed cases
- Small backpressure windows create severe tail-latency spikes
- Scaling efficiency is still low for light and medium records

## Operational Recommendations

- If the workload does not use route-key distribution, do not expect extra shards to improve ingest throughput.
- Prefer `2` shards as the current default performance point for small and medium payload workloads.
- Use `4` shards mainly when payloads are larger and routing actually spreads writes.
- Avoid `max_pending_bytes=131072` in production-like profiles.
- If backpressure must be enabled, start from `524288` and validate with real traffic.
- Treat `route_keys>=4` as the minimum useful profile when evaluating multi-shard write throughput.

## Optimization Priorities

1. Measure where `4`-shard overhead is coming from in distributed small-payload cases.
2. Profile cross-shard submit path, queue contention, and completion path under `payload=128/512`.
3. Turn backpressure from a fixed threshold into a profile-driven setting and document safe ranges.
4. Add a benchmark view that reports per-shard skew so route distribution quality can be separated from engine overhead.

## Scope Notes

- These numbers come from the benchmark harness in the current sandbox/workspace environment.
- Query-server-based end-to-end checks are constrained here by sandbox socket restrictions, so this report focuses on the write benchmark matrix.
