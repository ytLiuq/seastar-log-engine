# Performance Analysis 2026-04-26

本报告基于一轮本机参数扫描结果生成。

相关文件：

- 原始扫描结果：[benchmark-scan-2026-04-26.tsv](/root/workspace/seastar-log-engine/doc/benchmark-scan-2026-04-26.tsv)
- 单次对比结果：[benchmark-results-2026-04-26.md](/root/workspace/seastar-log-engine/doc/benchmark-results-2026-04-26.md)
- 扫描脚本：[compare_bench.sh](/root/workspace/seastar-log-engine/script/compare_bench.sh)

## Method

执行命令：

```bash
./script/compare_bench.sh --scan
```

本轮扫描包含四组场景：

- `payload` 扫描：`128` / `512`
- `batch-size` 扫描：`64` / `256` / `1024`
- `inflight` 扫描：`16` / `64` / `256`
- `checkpoint` 扫描：`0` / `1`

约束：

- `messages=50000`
- `shards=2`
- `rotate-size-bytes=1048576`
- `payload` 扫描会同时跑 `log_engine` / `glog` / `spdlog`
- 其余扫描只针对 `log_engine`

## Summary

这轮结果里，`log_engine` 的吞吐明显低于 `glog` 和 `spdlog`，但变化趋势相当稳定：

- 在 `payload=128` 时，`log_engine` 约为 `373k msg/s`
- 同场景下，`glog` 约为 `640k msg/s`
- 同场景下，`spdlog` 约为 `968k msg/s`
- `log_engine` 在该点位大约是 `glog` 的 `58.3%`，`spdlog` 的 `38.6%`

这说明当前实现的主要成本不只是“写文件”，而是日志引擎额外承担的编码、分片、批量、DMA 对齐、checkpoint 和恢复语义。

## Baseline

| Scenario | Target | Throughput (msg/s) | Relative to `log_engine` |
| --- | --- | ---: | ---: |
| `payload=128` | `log_engine` | 373170.53 | 100.0% |
| `payload=128` | `glog` | 639607 | 171.4% |
| `payload=128` | `spdlog` | 967717 | 259.3% |
| `payload=512` | `log_engine` | 183944.58 | 100.0% |
| `payload=512` | `glog` | 538138 | 292.6% |
| `payload=512` | `spdlog` | 665442 | 361.8% |

## Findings

### 1. `payload` 放大对 `log_engine` 打击最明显

| Target | `payload=128` | `payload=512` | Change |
| --- | ---: | ---: | ---: |
| `log_engine` | 373170.53 | 183944.58 | `-50.7%` |
| `glog` | 639607 | 538138 | `-15.9%` |
| `spdlog` | 967717 | 665442 | `-31.2%` |

`log_engine` 的吞吐在 payload 放大后几乎减半，明显比 `glog` 和 `spdlog` 更敏感。最可能的原因有：

- 记录格式更重，包含时间戳、level、shard、sequence、CRC
- [record_codec.cc](/root/workspace/seastar-log-engine/src/record_codec.cc) 中的字符串拼接和 CRC32 成本会随 payload 增长
- [async_writer.cc](/root/workspace/seastar-log-engine/src/async_writer.cc) 中批量拼接、对齐缓冲复制、DMA 写入路径对大记录更敏感

### 2. `batch-size=256` 已接近当前最优点

| Batch Size | Throughput (msg/s) | Change |
| --- | ---: | ---: |
| `64` | 377233.22 | baseline |
| `256` | 433538.54 | `+14.9%` vs `64` |
| `1024` | 426766.81 | `-1.6%` vs `256` |

这说明：

- `64` 太小，flush 频率偏高
- `256` 明显更合适
- 继续加到 `1024` 收益已经消失，甚至略有回退

所以当前实现在这组负载下更像是“中等 batch 最优”，而不是“batch 越大越快”。

### 3. `inflight=64` 优于过低和过高并发

| Inflight | Throughput (msg/s) | Change |
| --- | ---: | ---: |
| `16` | 348305.84 | baseline |
| `64` | 383567.95 | `+10.1%` vs `16` |
| `256` | 371620.12 | `-3.1%` vs `64` |

这说明当前瓶颈不是简单的“并发不够”，而是已经进入了调度和内部队列的平衡区间：

- 太低：不能喂饱 shard writer
- 太高：调度、排队、聚合和 flush 竞争开始抵消收益

在这台机器和这组参数下，`64` 左右是更合理的提交并发。

### 4. `checkpoint` 开销存在，但不是主瓶颈

| Checkpoint | Throughput (msg/s) | Change |
| --- | ---: | ---: |
| `0` | 406649.53 | baseline |
| `1` | 390097.76 | `-4.1%` |

`checkpoint` 打开后有明确成本，但量级不算夸张。这意味着：

- checkpoint 不是当前最主要的吞吐杀手
- 更重的成本仍然在主写入链路本身
- 但在更小消息、更高 flush 频率场景下，这个成本可能会被进一步放大

## Interpretation

从代码路径看，这些结果是合理的。

### `log_engine` 为什么慢于 `glog` / `spdlog`

`log_engine` 的一次写入并不是简单的日志追加，而是经过：

- route key 到 shard 的分发：[log_engine.cc](/root/workspace/seastar-log-engine/src/log_engine.cc)
- per-shard pending queue 聚合：[async_writer.cc](/root/workspace/seastar-log-engine/src/async_writer.cc)
- 记录编码和 CRC：[record_codec.cc](/root/workspace/seastar-log-engine/src/record_codec.cc)
- DMA 对齐写入、tail buffer 管理：[async_writer.cc](/root/workspace/seastar-log-engine/src/async_writer.cc)
- 可选 checkpoint 持久化：[log_manager.cc](/root/workspace/seastar-log-engine/src/log_manager.cc)

而 `glog_bench` 和 `spdlog_bench` 更接近“直接 benchmark 单库写日志吞吐”，功能负担明显更轻。

### 为什么 `payload` 是最敏感维度

对当前实现来说，payload 变大同时放大了三种成本：

- 用户字符串自身拷贝
- 编码后的整条日志长度
- flush 阶段的 joined buffer 和 tail buffer 处理

也就是说，这里不仅有 IO 量增长，还有用户态 CPU 和内存搬运增长，所以曲线比 `glog/spdlog` 陡。

## Practical Tuning

如果你现在是以吞吐优先来调这个项目，优先建议：

1. 把 `batch-size` 先放在 `256` 左右，不要盲目增大
2. 把 `inflight` 放在中档，例如 `64`
3. 先确认实际业务 payload 分布，因为 payload 对结果影响最大
4. 如果业务允许，先关 `checkpoint` 看收益，再决定是否按场景配置

## Next Steps

如果要继续定位热点，下一轮最值得补的扫描是：

1. `shard` 数量扫描：`-c 1/2/4`
2. `flush-ms` 扫描
3. `write_behind` / `stream_buffer_size` 扫描
4. 一个“关闭 CRC”或“简化 record format”的实验分支
5. 一个“只测 active log，不走 checkpoint”的更纯净路径

这些实验会更直接地区分瓶颈到底是在：

- 编码
- 调度
- DMA 对齐与内存复制
- checkpoint / 文件元数据操作
