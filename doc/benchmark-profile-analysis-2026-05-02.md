# Benchmark Profile Analysis 2026-05-02

数据来源：

- 原始结果：[benchmark-profiles-2026-05-02.tsv](/root/workspace/seastar-log-engine/doc/benchmark-profiles-2026-05-02.tsv)
- 汇总结果：[benchmark-profiles-2026-05-02.md](/root/workspace/seastar-log-engine/doc/benchmark-profiles-2026-05-02.md)

测试参数：

- `messages=50000`
- `payload_size=256`
- `batch_size=4096`
- `inflight=16`
- `shards=1`
- `flush_ms=5`
- `repeats=3`

## 结论

### 1. `timestamp` 开销已经压到可接受范围

以 `record-baseline` 为基准，`record-timestamp-on` 的平均吞吐从约 `898k msg/s` 降到约 `816k msg/s`，跌幅约 `9.2%`；平均 submit latency 从 `2.20us` 增到 `2.45us`。

这说明前面的 timestamp 前缀缓存优化是有效的。当前 `timestamp` 不再是 record-format 路径里最重的单项成本。

### 2. `CRC` 仍然是 record-format 的主要成本来源

`record-crc-on` 的平均吞吐约 `600k msg/s`，相对 baseline 跌幅约 `33.2%`；平均 submit latency 从 `2.20us` 提升到 `4.15us`。

也就是说，虽然已经去掉了“整条 body 二次扫描”和多处逐字节慢路径，但 CRC 更新本身在 `payload_size=256` 的场景下仍然是主要热点。

### 3. 全量 structured fields 的额外成本主要还是叠加在 CRC 上

`record-structured-on` 的平均吞吐约 `521k msg/s`，相对 baseline 跌幅约 `42.0%`；相对 `record-crc-on` 再降约 `13.1%`。

这说明：

- `structured fields` 的额外字段拼装成本还在
- 但最大头的成本依然不是 `timestamp`，而是“CRC + 更多字段写入”叠加后的整体编码路径

### 4. `checkpoint` 和 `rotate` 在这组参数下不是主瓶颈

`checkpoint-on` 相对 `checkpoint-off` 的平均吞吐跌幅约 `3.1%`；`rotate-on` 相对 `rotate-off` 的平均吞吐跌幅约 `3.3%`。

这两个开销量级明显小于 `CRC` 和完整 structured record 开销，所以 record-format 优化下一阶段不应该把注意力重新拉回 `checkpoint` 或 `rotate`。

## 建议的下一步

1. 继续盯 `record_crc_enabled=true` 的编码热点，优先看是否还能减少 payload 上的逐字节 CRC 更新成本。
2. 单独拆一次更大的 payload（例如 `1024` 或 `4096`）跑同样 profile，确认 CRC 成本是否随 payload 放大得更明显。
3. 如果后续要继续优化 structured record，优先看 `append_decimal()` 和字段分隔符写入路径，而不是再花时间在 timestamp 缓存上。
