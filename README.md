# Seastar Log Engine

基于 Seastar 的高性能异步批量顺序写日志引擎。

当前版本已经收敛到一条统一写路径，定位是：

- 面向 Seastar 服务的本地持久化日志 / 轻量 WAL 引擎
- 支持按 shard 顺序写入、rotate、archive、checkpoint、recovery
- 支持读取、校验、HTTP / gRPC 查询和 benchmark

## 当前能力

- `seastar::sharded<>` per-shard writer
- 异步批量顺序写
- 统一 DMA 对齐写入路径
- `write_ack / sync_ack` 两档确认语义
- `hash_modulo` 和 `consistent_hashing + virtual nodes` 路由
- rotate / archive / gzip / checkpoint / recovery
- `seastar::metrics` 指标
- HTTP / gRPC 查询接口
- `glog` / `spdlog` benchmark 对比
- 长稳 / 回归 benchmark 脚本

## Build

```bash
./script/build.sh
```

By default the build script expects a Seastar checkout at `.deps/seastar`.
Bootstrap it locally with:

```bash
bash ./script/bootstrap_seastar.sh
./script/build.sh
./script/test_unit.sh
```

For a fully containerized build and unit regression run:

```bash
docker build --target test .
```

## Quick Start

运行 demo：

```bash
./build/log_engine_demo \
  --routing-strategy consistent_hashing \
  --routing-virtual-nodes 256 \
  --log-dir ./logs \
  --archive-dir ./archive \
  --messages 100000 \
  --batch-size 1024 \
  --flush-ms 1 \
  --checkpoint-enabled true \
  --truncate-on-start true
```

运行 benchmark：

```bash
./build/log_engine_bench \
  --ack-mode write_ack \
  --log-dir ./logs \
  --archive-dir ./archive \
  --messages 200000 \
  --payload-size 2048 \
  --batch-size 8192 \
  --flush-ms 0 \
  --inflight 1 \
  --checkpoint-enabled false
```

运行长稳 / 回归 benchmark：

```bash
./script/bench_regression.sh --messages 100000 --repeats 3 --shards 1
./script/bench_soak.sh --target log_engine --duration-seconds 300 --messages 50000 --payload-size 2048 --batch-size 8192 --inflight 1 --shards 1 --ack-mode write_ack
```

## Layout

- `include/log_engine`: 头文件
- `src`: 核心实现和可执行入口
- `proto`: gRPC proto
- `script`: 构建、测试、benchmark 脚本
- `doc`: 详细说明和 benchmark 结果

## Docs

- 详细使用说明：[doc/README.md](doc/README.md)
- 历史状态快照：[doc/seastar-log-engine-current-status-and-todo-2026-05-01.md](doc/seastar-log-engine-current-status-and-todo-2026-05-01.md)
- 实施方案：`../docs/基于Seastar的高性能异步批量日志顺序写入引擎实施方案.md`
