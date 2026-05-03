# Benchmark Profile Payload Analysis 2026-05-02

数据来源：

- 原始结果：[benchmark-profiles-payloads-2026-05-02.tsv](/root/workspace/seastar-log-engine/doc/benchmark-profiles-payloads-2026-05-02.tsv)
- 汇总结果：[benchmark-profiles-payloads-2026-05-02.md](/root/workspace/seastar-log-engine/doc/benchmark-profiles-payloads-2026-05-02.md)

测试参数：

- `messages=30000`
- `payload_sizes=256,1024,4096`
- `batch_size=4096`
- `inflight=16`
- `shards=1`
- `flush_ms=5`
- `repeats=3`

## 重点结论

### 1. `CRC` 成本会随 payload 放大明显增长

平均 submit latency：

- `payload=256`
  - baseline: `1.58us`
  - crc: `4.39us`
- `payload=1024`
  - baseline: `4.31us`
  - crc: `14.62us`
- `payload=4096`
  - baseline: `28.66us`
  - crc: `59.38us`

也就是说，随着 payload 从 `256` 放大到 `1024` 和 `4096`，CRC 路径的绝对成本增长非常快，已经明显超过 timestamp 和普通字段拼装。

### 2. `CRC` 相对 baseline 的吞吐跌幅在大多数 payload 下都稳定在三分之一左右

按平均吞吐看：

- `payload=256`：`crc` 相对 baseline 跌幅约 `33.8%`
- `payload=1024`：`crc` 相对 baseline 跌幅约 `33.4%`
- `payload=4096`：`crc` 相对 baseline 跌幅约 `33.5%`

这说明当前 CRC 热点不是某个“小 payload 特殊分支”的问题，而是编码主路径上的稳定成本。

### 3. `timestamp` 本身不是主要问题，但在超大 payload 场景下整体路径噪声会变大

- `payload=256`：`timestamp` 跌幅约 `11.4%`
- `payload=1024`：`timestamp` 跌幅约 `0%`
- `payload=4096`：`timestamp` 跌幅约 `32.9%`

其中 `payload=4096` 的 baseline 吞吐波动明显更大，单次 run 之间差异比前两组大得多，所以这一档不适合只盯吞吐百分比做精细判断。更可信的信号还是：

- `timestamp` 平均 submit latency 从 `28.66us` 到 `30.04us`
- 增幅远小于 `crc` 从 `28.66us` 到 `59.38us`

### 4. 全量 structured fields 在 `payload=4096` 时几乎已经和 `crc` 成本重合

- `payload=4096`
  - crc: `59.38us`
  - structured: `59.79us`

这说明在大 payload 场景下，额外字段拼装已经不是主要矛盾，主要矛盾还是 payload 本身的 CRC 更新成本。

## 对后续优化的含义

1. 下一阶段如果还要继续做 `record format` 优化，最值得投时间的就是 `record_crc_enabled=true` 路径。
2. 大 payload 场景下，不需要再优先优化 `timestamp` 或普通字段拼装，它们已经不是主热点。
3. 如果要继续验证优化收益，下一轮 benchmark 应该固定在 `payload=1024` 或 `4096`，因为这两档更容易放大 CRC 优化前后的差异。
