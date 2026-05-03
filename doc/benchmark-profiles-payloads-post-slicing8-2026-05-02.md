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
| `checkpoint-off` | `checkpoint` | 1024 | 3 | 289623.57 | 4.9037 | 0.6667 | 1.0000 |
| `checkpoint-off` | `checkpoint` | 4096 | 3 | 33177.11 | 28.9601 | 4.3333 | 19.3333 |
| `checkpoint-on` | `checkpoint` | 1024 | 3 | 258115.78 | 4.3650 | 1.0000 | 1.6667 |
| `checkpoint-on` | `checkpoint` | 4096 | 3 | 33495.50 | 30.0063 | 4.0000 | 18.6667 |
| `record-baseline` | `record_format` | 1024 | 3 | 305497.19 | 4.7083 | 1.0000 | 1.3333 |
| `record-baseline` | `record_format` | 4096 | 3 | 49520.22 | 26.1593 | 4.3333 | 44.6667 |
| `record-crc-on` | `record_format` | 1024 | 3 | 193779.32 | 15.1274 | 3.0000 | 8.6667 |
| `record-crc-on` | `record_format` | 4096 | 3 | 33501.16 | 59.8006 | 15.0000 | 1524.3333 |
| `record-structured-on` | `record_format` | 1024 | 3 | 183349.32 | 16.2821 | 3.0000 | 8.6667 |
| `record-structured-on` | `record_format` | 4096 | 3 | 32825.53 | 61.0021 | 15.0000 | 1527.3333 |
| `record-timestamp-on` | `record_format` | 1024 | 3 | 257042.59 | 4.3044 | 1.0000 | 1.3333 |
| `record-timestamp-on` | `record_format` | 4096 | 3 | 33098.46 | 31.1711 | 4.0000 | 15.6667 |
| `rotate-off` | `rotate` | 1024 | 3 | 306340.93 | 4.3783 | 0.6667 | 1.0000 |
| `rotate-off` | `rotate` | 4096 | 3 | 33790.30 | 30.9966 | 3.6667 | 16.3333 |
| `rotate-on` | `rotate` | 1024 | 3 | 270370.99 | 4.6868 | 1.0000 | 1.3333 |
| `rotate-on` | `rotate` | 4096 | 3 | 33165.13 | 37.9259 | 4.0000 | 17.3333 |
