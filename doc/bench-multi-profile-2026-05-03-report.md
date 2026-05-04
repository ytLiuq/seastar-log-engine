# Multi-Shard Benchmark Profile

Generated: Sun May  3 13:38:51 CST 2026

- Messages per run: 20000
- Batch size: 512
- Inflight: 16
- Total runs: 84

## Throughput by Shard Count (write_ack, payload=512, no backpressure)

| Shards | Throughput (msg/s) | P50 (us) | P99 (us) |
| ---: | ---: | ---: | ---: |
| 1 | 464922 | 0.0000 | 0.0000 |
| 2 | 482241 | 0.0000 | 0.0000 |
| 4 | 448783 | 0.0000 | 1.0000 |

## Ack Mode Comparison (shards=2, payload=512, route_keys=0, no backpressure)

| Ack Mode | Throughput (msg/s) | P50 (us) | P99 (us) |
| ---: | ---: | ---: | ---: |
| write_ack | 482241 | 0.0000 | 0.0000 |
| sync_ack | 458526 | 0.0000 | 0.0000 |

## Payload Size Impact (shards=2, write_ack, route_keys=0, no backpressure)

| Payload (B) | Throughput (msg/s) | P50 (us) | P99 (us) |
| ---: | ---: | ---: | ---: |
| 128 | 980440 | 0.0000 | 0.0000 |
| 512 | 482241 | 0.0000 | 0.0000 |
| 2048 | 159422 | 1.0000 | 2.0000 |

## Route Key Distribution (shards=2, write_ack, payload=512, no backpressure)

| Route Keys | Throughput (msg/s) | P50 (us) | P99 (us) |
| ---: | ---: | ---: | ---: |
| 0 | 482241 | 0.0000 | 0.0000 |
| 4 | 626782 | 9.0000 | 63.0000 |
| 16 | 670916 | 8.0000 | 59.0000 |

## Backpressure Impact (shards=2, write_ack, payload=512, route_keys=4)

| Max Pending (B) | Throughput (msg/s) | P50 (us) | P95 (us) | P99 (us) |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 626782 | 9.0000 | 26.0000 | 63.0000 |
| 131072 | 241514 | 2.0000 | 26.0000 | 2295.0000 |
| 524288 | 588616 | 7.0000 | 24.0000 | 47.0000 |

## Scaling Efficiency

| Shards | write_ack thr (msg/s) | Efficiency |
| ---: | ---: | ---: |
| 1 | 464922 | 100.0% |
| 2 | 482241 | 51.9% |
| 4 | 448783 | 24.1% |
