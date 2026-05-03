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
| `checkpoint-off` | `checkpoint` | 1024 | 3 | 283608.35 | 4.5031 | 0.3333 | 1.0000 |
| `checkpoint-off` | `checkpoint` | 4096 | 3 | 33297.14 | 30.5298 | 3.6667 | 17.0000 |
| `checkpoint-on` | `checkpoint` | 1024 | 3 | 283339.50 | 4.2868 | 1.0000 | 1.0000 |
| `checkpoint-on` | `checkpoint` | 4096 | 3 | 32756.31 | 31.2735 | 4.0000 | 19.0000 |
| `record-baseline` | `record_format` | 1024 | 3 | 278294.91 | 4.3287 | 1.0000 | 1.0000 |
| `record-baseline` | `record_format` | 4096 | 3 | 50326.84 | 27.7687 | 4.0000 | 16.3333 |
| `record-crc-on` | `record_format` | 1024 | 3 | 187955.57 | 15.0087 | 3.0000 | 8.3333 |
| `record-crc-on` | `record_format` | 4096 | 3 | 33381.98 | 60.2667 | 14.6667 | 1526.3333 |
| `record-structured-on` | `record_format` | 1024 | 3 | 170540.44 | 16.5526 | 3.0000 | 10.0000 |
| `record-structured-on` | `record_format` | 4096 | 3 | 31463.23 | 60.8007 | 16.0000 | 1525.3333 |
| `record-timestamp-on` | `record_format` | 1024 | 3 | 260924.20 | 4.4426 | 1.0000 | 1.3333 |
| `record-timestamp-on` | `record_format` | 4096 | 3 | 32447.42 | 30.5089 | 3.6667 | 21.3333 |
| `rotate-off` | `rotate` | 1024 | 3 | 300816.89 | 4.4676 | 0.6667 | 1.0000 |
| `rotate-off` | `rotate` | 4096 | 3 | 33266.54 | 31.1003 | 4.0000 | 19.0000 |
| `rotate-on` | `rotate` | 1024 | 3 | 299248.72 | 4.6019 | 0.6667 | 1.0000 |
| `rotate-on` | `rotate` | 4096 | 3 | 33171.38 | 37.1648 | 4.0000 | 17.3333 |
