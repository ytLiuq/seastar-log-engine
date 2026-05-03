# Benchmark Profiles 2026-05-02

- messages: `50000`
- payload_size: `256`
- batch_size: `4096`
- inflight: `16`
- shards: `1`
- flush_ms: `5`
- route_keys: `0`
- repeats: `3`

| Scenario | Group | Runs | Avg Throughput (msg/s) | Avg Submit (us) | Avg P95 (us) | Avg P99 (us) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `checkpoint-off` | `checkpoint` | 3 | 953552.81 | 2.1593 | 0.0000 | 0.0000 |
| `checkpoint-on` | `checkpoint` | 3 | 923765.87 | 2.1899 | 0.0000 | 0.0000 |
| `record-baseline` | `record_format` | 3 | 898457.17 | 2.1977 | 0.0000 | 0.0000 |
| `record-crc-on` | `record_format` | 3 | 600027.67 | 4.1542 | 1.0000 | 1.0000 |
| `record-structured-on` | `record_format` | 3 | 521372.13 | 5.0950 | 1.0000 | 1.0000 |
| `record-timestamp-on` | `record_format` | 3 | 815878.78 | 2.4524 | 0.0000 | 0.0000 |
| `rotate-off` | `rotate` | 3 | 907333.92 | 2.1636 | 0.0000 | 0.0000 |
| `rotate-on` | `rotate` | 3 | 877760.62 | 1.6748 | 0.0000 | 0.0000 |
