# Seastar Log Engine 当前功能与 TODO

> 历史快照：本文档保留 2026-05-01 时点状态。
> 当前待办请以仓库根目录 [TODO.md](../TODO.md) 和 [doc/README.md](README.md) 为准。

更新时间：`2026-05-01`

## 项目定位

`seastar-log-engine` 是一个基于 Seastar 的单机高性能异步顺序写日志引擎，当前重点已经从“多模式试验型原型”收敛到“一条统一写路径 + 可恢复日志语义”的可运行实现。

最近几轮提交已经完成：

- 引入 `log_layout` 做路径和 segment 元数据统一管理
- 删除历史上的快/慢双写路径，统一到单一 DMA 对齐写入链路
- `LogManager` / `log_reader` / `AsyncWriter` 全部迁到 `layout::SegmentDescriptor`
- 保留 rotate / archive / checkpoint / recovery / query 等完整语义

## 已实现功能

### 1. 核心写入引擎

- 基于 `seastar::sharded<AsyncWriter>` 的 per-shard writer 架构
- 每个 shard 独立顺序追加自己的 active log 文件
- 批量提交、批量刷盘、定时 flush
- 统一 DMA 对齐写入路径
- 写失败重试与退避
- `write_ack` / `sync_ack` 两档确认语义

### 2. 路由与分片

- `route_key -> shard` 路由
- `hash_modulo` 路由策略
- `consistent_hashing + virtual nodes` 路由策略
- 空 `route_key` 回退到当前 shard

### 3. Record Format

- payload-only 记录格式
- 可选结构化字段：
  - `ts=`
  - `shard=`
  - `seq=`
  - `level=`
  - `crc=`
- payload 中换行、回车、制表符清洗
- CRC32 校验与恢复扫描
- 当前已做一轮 timestamp 格式化缓存优化，减少每条日志重复的时间格式化成本

### 4. 日志布局与恢复

- active log 路径统一由 `log_layout` 生成
- archive 文件命名、收集、排序统一由 `log_layout` 管理
- checkpoint sidecar 持久化
- 启动时恢复 active log
- 按 checkpoint + 有效记录扫描修复损坏尾部
- 基于 `SegmentDescriptor` 的 segment 收集与读取

### 5. Rotate / Archive / Compression

- 按大小滚动
- 按时间滚动
- 归档目录管理
- gzip 压缩归档
- 按保留时长清理旧归档
- 按每 shard 最大归档数清理旧归档

### 6. 查询与读取

- 本地日志读取 CLI：`log_engine_read`
- 日志校验 CLI：`log_engine_verify`
- HTTP 查询服务：`log_engine_query_server`
- gRPC 查询服务：`log_engine_query_server`
- 查询内容覆盖：
  - 服务状态
  - 路由结果
  - 日志记录读取
  - metrics 暴露

### 7. 配置与兼容层

- 配置文件加载
- 命令行参数覆盖配置
- `compat_glog` 绑定式兼容层
- 支持在外部 `LogEngine` 实例上绑定并复用 `LOG_INFO/LOG_WARNING/LOG_ERROR`

### 8. Metrics / Bench / Test

- `seastar::metrics` 暴露 writer 指标：
  - `submitted_messages`
  - `submitted_bytes`
  - `flushed_batches`
  - `flushed_bytes`
  - `flush_errors`
  - `pending_entries`
  - `pending_bytes`
  - `logical_size_bytes`
- benchmark 工具：
  - `log_engine_bench`
  - `glog_bench`
  - `spdlog_bench`
- benchmark 脚本：
  - `script/compare_bench.sh`
  - `script/bench_regression.sh`
  - `script/bench_soak.sh`
- 测试脚本：
  - `script/test_unit.sh`
  - `script/test_rotation.sh`
  - `script/test_recovery.sh`
  - `script/test_fault_injection.sh`
  - `script/test_read.sh`

## 当前代码状态补充

### 已确认修复的问题

- 单元测试里对 `route_key` 是否写入日志正文的过时假设已经修正
- `force_flush()` 导致 time rotation 场景 active record 丢失的问题已经修复
- `LogManager` 恢复路径已去掉 `stringstream` 整文件二次拷贝
- `AppendWriter` 的 chunked flush 已去掉一层额外 aligned buffer 拷贝

### 已确认存在但未完成收尾的问题

- 全量 `cmake --build build` 目前仍可能卡在 `log_engine_query_client` 的链接问题
  - 现象：`fmt::v11::vprint` 未定义
  - 原因倾向：`query_client` 没有像 `log_engine_core` / `spdlog_bench` 那样显式链接 `fmt`
- 根目录 `README.md` 和 `doc/README.md` 存在明显过时内容
  - 仍然提到历史上的 `fast/full` 模式
  - 仍然提到已删除的 `memory_ack`
  - 示例命令和当前实现不一致

## TODO

### P0

- 修复 `log_engine_query_client` 的 `fmt` 链接问题，确保全量构建通过
- 同步更新根目录 `README.md` 和 `doc/README.md`
  - 删除旧的 `fast/full` 描述
  - 删除旧的 `memory_ack` 示例
  - 改成当前统一写路径和 `write_ack/sync_ack`

### P1

- 继续优化 `record format`
  - 降低 `encode_record_buffer()` 的字段拼装和 payload 扫描成本
  - 评估 CRC 路径在不同 payload 下的真实开销
  - 补充“timestamp on/off、crc on/off、structured fields on/off”的固定 benchmark profile
- 补更多恢复与 rotate 组合测试
  - checkpoint on/off
  - gzip archive on/off
  - 大 payload + rotate
  - 损坏尾部 + checkpoint 回放

### P2

- 扩展 `compat_glog` 能力，提升 drop-in 替换程度
- 完善 benchmark 文档，把“默认场景、record-format 场景、checkpoint 场景、rotate 场景”固定成可复现 profile
- 补充更系统的 query 接口测试，覆盖 HTTP / gRPC 读取一致性
- 整理 `doc/` 下 benchmark 结果与性能分析文档，减少重复和过时结论

## 推荐后续工作顺序

1. 先修 `query_client` 全量构建问题
2. 再清理 README / doc 中的过时模式说明
3. 然后继续做 `record format` 的定向优化和压测
4. 最后再扩展恢复、query、compat 层测试覆盖

## 当前结论

这个项目已经不是“概念原型”，而是一个具备以下完整链路的单机日志引擎实现：

- 分片路由
- 异步批量写入
- 统一 DMA 写路径
- rotate / archive / gzip
- checkpoint / recovery
- query / metrics
- benchmark / regression / soak / unit test

当前最需要做的不是继续加大功能面，而是：

- 修收尾问题
- 清过时文档
- 针对 `record format` 和恢复链路做更稳的性能与稳定性收敛
