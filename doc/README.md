# Seastar Log Engine

这是基于 Seastar 的异步批量顺序写日志引擎当前实现说明。

## 已落地能力

- 基于 `seastar::sharded` 的 per-shard writer
- 按批量阈值或定时器触发刷盘
- 每个 shard 独立顺序写自己的 active log
- 统一 DMA 对齐写入路径
- 按 route key 分片路由，支持 `hash_modulo` 和 `consistent_hashing + virtual nodes`
- checkpoint sidecar
- 启动时按 checkpoint + 记录扫描恢复 active log
- 按大小和按时间的滚动触发
- archive 目录维护
- gzip 归档与按数量 / 时间清理
- 写失败重试
- 配置文件加载
- 基于 `seastar::metrics` 的 writer 指标
- 背压 / 水位控制：
  - `max_pending_bytes`
  - `pending_bytes_low_watermark`
- 日志读取 / 校验 CLI
- HTTP / gRPC 查询接口
- 绑定式 `compat_glog` 兼容层
- `demo` / `bench` / `verify` / `read` / `query_server` / `query_client` / `unit_tests` 可执行程序
- `glog` 与 `spdlog` 对比 benchmark

## 当前写入模型

当前版本不再区分历史上的 `fast/full` 双路径，已经统一为一条写入链路：

`submit -> per-shard pending queue -> batch/flush -> DMA aligned append -> optional checkpoint/rotate/archive`

当前确认语义只有两档：

- `write_ack`
- `sync_ack`

其中：

- `write_ack`：当前待写 batch/tail 已提交到底层写入路径后返回，但不额外显式 flush
- `sync_ack`：当前待写 batch/tail 写入后额外执行 flush，再向调用方确认

## 目录结构

- `include/log_engine`: 头文件
- `src`: 核心实现和各可执行程序入口
- `proto`: gRPC proto 定义
- `script/build.sh`: 本地构建脚本
- `doc`: benchmark 和分析文档

## 当前已知未收尾项

- `log_engine_query_client` 仍可能因为 `fmt` 链接缺失导致全量构建失败
- 部分 benchmark / test 脚本还保留历史 `fast/full`、`memory_ack` 参数，需要后续继续清理

## 兼容层说明

- 头文件：`include/log_engine/compat_glog.hh`
- 当前只建议在 Seastar reactor 线程内使用
- 采用“绑定到外部 `LogEngine` 实例”的模式，而不是内部自管全局引擎
- 推荐流程：
  - 启动 `LogEngine`
  - `compat::bind(engine)`
  - 使用 `LOG_INFO/LOG_WARNING/LOG_ERROR`
  - `compat::flush()`
  - `compat::unbind()`

## 构建

```bash
./script/build.sh
```

## 配置文件启动

```bash
./build/log_engine_demo --config ./config/engine.conf -c 2
```

## 运行 demo

```bash
./build/log_engine_demo \
  --routing-strategy consistent_hashing \
  --routing-virtual-nodes 256 \
  --log-dir ./logs \
  --archive-dir ./archive \
  --messages 100000 \
  --batch-size 1024 \
  --flush-ms 1 \
  --rotate-size-bytes 1048576 \
  --compress-archives true \
  --checkpoint-enabled true \
  --truncate-on-start true
```

## 运行 benchmark

```bash
./build/log_engine_bench \
  --ack-mode write_ack \
  --log-dir ./logs \
  --archive-dir ./archive \
  --messages 200000 \
  --payload-size 256 \
  --batch-size 8192 \
  --flush-ms 0 \
  --inflight 1 \
  --checkpoint-enabled false
```

## 校验日志文件

```bash
./build/log_engine_verify --path ./logs/shard-0.log
```

## 读取日志

```bash
./build/log_engine_read --log-dir ./logs --archive-dir ./archive --include-archive 1 --seq-from 100 --seq-to 200 --limit 50
```

## 启动查询服务

```bash
./build/log_engine_query_server \
  --config ./config/engine.conf \
  --routing-strategy consistent_hashing \
  --routing-virtual-nodes 256 \
  --http-address 0.0.0.0 \
  --http-port 18080 \
  --grpc-address 0.0.0.0 \
  --grpc-port 19090 \
  --metrics-address 0.0.0.0 \
  --metrics-port 19181
```

## HTTP 查询接口

```bash
curl 'http://127.0.0.1:18080/v1/status'
curl 'http://127.0.0.1:18080/v1/route?key=route-a'
curl 'http://127.0.0.1:18080/v1/records?shard=0&limit=10&include_archive=true'
curl 'http://127.0.0.1:19181/metrics'
```

## gRPC 查询接口

```bash
./build/log_engine_query_client --target 127.0.0.1:19090 --method status
./build/log_engine_query_client --target 127.0.0.1:19090 --method route --route-key route-a
./build/log_engine_query_client --target 127.0.0.1:19090 --method records --limit 10 --include-archive true
```

## 路由参数

- `--routing-strategy hash_modulo|consistent_hashing`
- `--routing-virtual-nodes <n>`
- 空 `route_key` 会回退到当前 shard

## Writer Metrics

分组名：`log_engine_writer`

当前核心指标：

- `submitted_messages`
- `submitted_bytes`
- `flushed_batches`
- `flushed_bytes`
- `flush_errors`
- `backpressure_waits`
- `pending_entries`
- `pending_bytes`
- `waiting_submitters`
- `logical_size_bytes`

## Benchmark 输出

`log_engine_bench` 当前会输出：

- `avg_submit_us`
- `p50_submit_us`
- `p95_submit_us`
- `p99_submit_us`

可配合：

- `script/parse_results.py`
- `script/compare_bench.sh --scan`

生成结构化摘要。

## 恢复模式启动

```bash
./build/log_engine_demo --log-dir ./logs --archive-dir ./archive --messages 1000 --truncate-on-start false --checkpoint-enabled true
```

## 测试脚本

```bash
./script/test_rotation.sh
./script/test_recovery.sh
./script/test_fault_injection.sh
./script/test_read.sh
./script/test_unit.sh
```

## 当前最小单元测试覆盖

- 记录编解码与 CRC 校验
- 配置文件加载与参数覆盖
- 配置校验
- consistent hashing 路由与 virtual nodes 基本行为
- 绑定式 `compat_glog` 写日志与 flush
- 按时间滚动后的 gzip 归档读取
- active log 追加损坏尾部后的恢复扫描

## 对比 benchmark

```bash
./script/compare_bench.sh 50000 128
./script/compare_bench.sh --scan
./script/bench_regression.sh --messages 100000 --repeats 3 --shards 1
./script/bench_soak.sh --target log_engine --duration-seconds 300 --messages 50000 --payload-size 2048 --batch-size 8192 --inflight 1 --shards 1 --ack-mode write_ack
```

## 长稳 / 回归 benchmark

- `script/bench_regression.sh`
  固定回归矩阵，当前脚本本身仍有历史参数待清理
- `script/bench_soak.sh`
  连续循环压测，按时长收集多轮结果，用来观察吞吐波动和 `P95/P99` 漂移
- 两个脚本都会把原始结果写到 `doc/*.tsv`，把摘要写到 `doc/*.md`
