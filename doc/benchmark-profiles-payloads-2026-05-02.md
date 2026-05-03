# Benchmark Profiles 2026-05-02

- messages: `30000`
- payload_sizes: `256,1024,4096`
- batch_size: `4096`
- inflight: `16`
- shards: `1`
- flush_ms: `5`
- route_keys: `0`
- repeats: `3`

| Scenario | Group | Payload | Runs | Avg Throughput (msg/s) | Avg Submit (us) | Avg P95 (us) | Avg P99 (us) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `checkpoint-off` | `checkpoint` | 1024 | 3 | 300147.77 | 4.4163 | 0.3333 | 1.0000 |
| `checkpoint-off` | `checkpoint` | 256 | 3 | 853922.61 | 1.6030 | 0.0000 | 0.0000 |
| `checkpoint-off` | `checkpoint` | 4096 | 3 | 33535.19 | 30.9475 | 4.0000 | 15.3333 |
| `checkpoint-on` | `checkpoint` | 1024 | 3 | 287322.65 | 4.0994 | 1.0000 | 1.0000 |
| `checkpoint-on` | `checkpoint` | 256 | 3 | 829994.42 | 1.5960 | 0.0000 | 0.0000 |
| `checkpoint-on` | `checkpoint` | 4096 | 3 | 32907.82 | 31.0805 | 3.3333 | 14.6667 |
| `record-baseline` | `record_format` | 1024 | 3 | 286908.38 | 4.3149 | 0.6667 | 1.3333 |
| `record-baseline` | `record_format` | 256 | 3 | 838439.34 | 1.5834 | 0.0000 | 0.0000 |
| `record-baseline` | `record_format` | 4096 | 3 | 49484.35 | 28.6598 | 3.6667 | 14.6667 |
| `record-crc-on` | `record_format` | 1024 | 3 | 191167.54 | 14.6206 | 3.0000 | 8.0000 |
| `record-crc-on` | `record_format` | 256 | 3 | 555263.76 | 4.3948 | 0.6667 | 1.0000 |
| `record-crc-on` | `record_format` | 4096 | 3 | 32892.89 | 59.3811 | 14.6667 | 1524.0000 |
| `record-structured-on` | `record_format` | 1024 | 3 | 181728.60 | 16.3311 | 3.0000 | 9.0000 |
| `record-structured-on` | `record_format` | 256 | 3 | 491705.49 | 5.2615 | 1.0000 | 1.0000 |
| `record-structured-on` | `record_format` | 4096 | 3 | 32917.54 | 59.7942 | 15.0000 | 1524.0000 |
| `record-timestamp-on` | `record_format` | 1024 | 3 | 286821.07 | 5.2393 | 1.0000 | 1.0000 |
| `record-timestamp-on` | `record_format` | 256 | 3 | 742525.86 | 1.7626 | 0.0000 | 0.0000 |
| `record-timestamp-on` | `record_format` | 4096 | 3 | 33229.90 | 30.0352 | 4.0000 | 16.3333 |
| `rotate-off` | `rotate` | 1024 | 3 | 313670.11 | 4.6427 | 0.6667 | 1.3333 |
| `rotate-off` | `rotate` | 256 | 3 | 772010.07 | 1.5268 | 0.0000 | 0.0000 |
| `rotate-off` | `rotate` | 4096 | 3 | 33727.04 | 29.9509 | 3.6667 | 18.0000 |
| `rotate-on` | `rotate` | 1024 | 3 | 294168.35 | 4.6734 | 1.0000 | 1.0000 |
| `rotate-on` | `rotate` | 256 | 3 | 795252.31 | 1.7630 | 0.0000 | 0.0000 |
| `rotate-on` | `rotate` | 4096 | 3 | 33403.00 | 36.8842 | 4.3333 | 18.3333 |
