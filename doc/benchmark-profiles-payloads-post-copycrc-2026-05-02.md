# Benchmark Profiles 2026-05-02

- messages: `30000`
- payload_sizes: `1024,4096`
- batch_size: `4096`
- inflight: `16`
- shards: `1`
- flush_ms: `5`
- route_keys: `0`
- repeats: `3`

| Scenario | Group | Payload | Runs | Avg Throughput (msg/s) | Avg Submit (us) | Avg P95 (us) | Avg P99 (us) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `checkpoint-off` | `checkpoint` | 1024 | 3 | 296375.02 | 4.6652 | 1.0000 | 1.0000 |
| `checkpoint-off` | `checkpoint` | 4096 | 3 | 33098.02 | 29.6839 | 4.0000 | 17.6667 |
| `checkpoint-on` | `checkpoint` | 1024 | 3 | 299156.78 | 4.7667 | 1.0000 | 1.0000 |
| `checkpoint-on` | `checkpoint` | 4096 | 3 | 33684.14 | 30.2406 | 4.0000 | 17.3333 |
| `record-baseline` | `record_format` | 1024 | 3 | 278517.15 | 4.5326 | 1.0000 | 1.0000 |
| `record-baseline` | `record_format` | 4096 | 3 | 47204.71 | 28.0506 | 4.0000 | 18.3333 |
| `record-crc-on` | `record_format` | 1024 | 3 | 185266.21 | 15.0189 | 3.0000 | 8.0000 |
| `record-crc-on` | `record_format` | 4096 | 3 | 33671.78 | 60.4507 | 14.6667 | 1527.6667 |
| `record-structured-on` | `record_format` | 1024 | 3 | 178339.30 | 16.6146 | 3.0000 | 10.3333 |
| `record-structured-on` | `record_format` | 4096 | 3 | 32890.87 | 60.8567 | 15.0000 | 1526.6667 |
| `record-timestamp-on` | `record_format` | 1024 | 3 | 253007.17 | 4.3616 | 1.0000 | 1.3333 |
| `record-timestamp-on` | `record_format` | 4096 | 3 | 33002.36 | 29.9641 | 4.0000 | 20.0000 |
| `rotate-off` | `rotate` | 1024 | 3 | 290548.50 | 4.6552 | 1.0000 | 1.0000 |
| `rotate-off` | `rotate` | 4096 | 3 | 33153.75 | 30.3584 | 4.0000 | 17.3333 |
| `rotate-on` | `rotate` | 1024 | 3 | 291839.66 | 4.7265 | 1.0000 | 1.0000 |
| `rotate-on` | `rotate` | 4096 | 3 | 33566.01 | 37.3630 | 4.0000 | 18.0000 |
