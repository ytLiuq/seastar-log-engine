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
| `checkpoint-off` | `checkpoint` | 1024 | 3 | 304000.73 | 4.5534 | 1.0000 | 1.0000 |
| `checkpoint-off` | `checkpoint` | 4096 | 3 | 32993.15 | 31.5447 | 4.0000 | 45.6667 |
| `checkpoint-on` | `checkpoint` | 1024 | 3 | 295412.48 | 4.4638 | 0.6667 | 1.0000 |
| `checkpoint-on` | `checkpoint` | 4096 | 3 | 33465.47 | 31.0155 | 3.6667 | 16.3333 |
| `record-baseline` | `record_format` | 1024 | 3 | 303067.15 | 4.5206 | 0.6667 | 1.0000 |
| `record-baseline` | `record_format` | 4096 | 3 | 50733.58 | 27.9125 | 3.6667 | 15.3333 |
| `record-crc-on` | `record_format` | 1024 | 3 | 187650.90 | 15.3462 | 3.0000 | 9.3333 |
| `record-crc-on` | `record_format` | 4096 | 3 | 33495.81 | 59.7673 | 15.0000 | 1524.3333 |
| `record-structured-on` | `record_format` | 1024 | 3 | 178995.51 | 17.2005 | 3.0000 | 10.3333 |
| `record-structured-on` | `record_format` | 4096 | 3 | 33078.10 | 60.4886 | 15.0000 | 1525.0000 |
| `record-timestamp-on` | `record_format` | 1024 | 3 | 284917.09 | 5.0783 | 1.0000 | 1.3333 |
| `record-timestamp-on` | `record_format` | 4096 | 3 | 32741.69 | 29.8009 | 3.6667 | 18.3333 |
| `rotate-off` | `rotate` | 1024 | 3 | 277445.85 | 4.4497 | 0.6667 | 1.3333 |
| `rotate-off` | `rotate` | 4096 | 3 | 33564.55 | 30.4980 | 4.0000 | 14.6667 |
| `rotate-on` | `rotate` | 1024 | 3 | 269371.40 | 4.3815 | 1.0000 | 1.3333 |
| `rotate-on` | `rotate` | 4096 | 3 | 33291.14 | 37.8722 | 4.0000 | 18.3333 |
