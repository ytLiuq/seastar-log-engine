# Benchmark Results 2026-04-27

本轮结果基于 `2026-04-27` 的最新 `HEAD`，目标是让 `log_engine` 的默认路径尽量接近 `spdlog` 的异步文件 logger：

- 单 shard：`-c 1`
- 不分片路由：`--route-keys 0`
- 不开 checkpoint：`--checkpoint-enabled 0`
- 不开 rotate：`--rotate-size-bytes 0`
- 不开定时 flush：`--flush-ms 0`
- 默认 payload-only record，不追加 CRC / timestamp / level / shard / sequence
- `log_engine` 使用 `batch-size=8192`、`inflight=1`
- `spdlog_bench` 使用单 async worker，默认 queue size `8192`

相关文件：

- 扫描结果：[benchmark-scan-2026-04-27.tsv](/root/workspace/seastar-log-engine/doc/benchmark-scan-2026-04-27.tsv)
- 脚本：[compare_bench.sh](/root/workspace/seastar-log-engine/script/compare_bench.sh)

## Direct Comparison

下面这组是更干净的直接对比：每组 `messages=100000`，分别跑 3 次。

| Payload | Target | Run 1 (msg/s) | Run 2 (msg/s) | Run 3 (msg/s) | Avg (msg/s) |
| --- | --- | ---: | ---: | ---: | ---: |
| `128` | `log_engine` | 1548035.54 | 1614100.78 | 1581878.01 | 1581338.11 |
| `128` | `spdlog` | 876002 | 722491 | 741532 | 780008.33 |
| `512` | `log_engine` | 487462.47 | 487807.26 | 487232.08 | 487500.60 |
| `512` | `spdlog` | 620956 | 685171 | 653432 | 653186.33 |

## Scan Snapshot

`./script/compare_bench.sh --scan` 的单次结果里，payload 维度是：

| Scenario | Target | Throughput (msg/s) |
| --- | --- | ---: |
| `payload=128` | `log_engine` | 709844.12 |
| `payload=128` | `spdlog` | 751078 |
| `payload=512` | `log_engine` | 440218.35 |
| `payload=512` | `spdlog` | 622874 |

扫描同时给出 `log_engine` 自身的调参趋势：

- `batch-size=1024` 和 `8192` 都明显优于 `256`
- `inflight=1` 是当前默认轻量模式下的最好点
- `checkpoint=1` 会把 `payload=128` 场景从约 `1.40M msg/s` 拉低到约 `678k msg/s`

## Current Read

- 在 `payload=128` 这类较小消息上，当前轻量默认路径已经可以和 `spdlog` 对齐，直接重复跑里甚至平均约为 `2.03x`
- 在 `payload=512` 这类较大消息上，当前实现仍然落后于 `spdlog`，大约是它的 `74.6%`
- 当前主要剩余差距更像是大 payload 下的内存拼接 / 复制成本，而不再是 checkpoint、record metadata 或默认分片路径带来的固定负担
