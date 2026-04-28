# Seastar Log Engine

基于 Seastar 的高性能异步批量顺序写日志引擎。

这个仓库当前已经实现了一个可运行的单机日志写入引擎，重点覆盖：

- `seastar::sharded<>` per-shard writer
- 异步批量顺序写
- `fast` / `full` 两条写路径
- `memory_ack / write_ack / sync_ack` 三档确认语义
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

## Quick Start

运行 demo：

```bash
./build/log_engine_demo \
  --mode full \
  --routing-strategy consistent_hashing \
  --routing-virtual-nodes 256 \
  --log-dir ./logs \
  --archive-dir ./archive \
  --messages 100000 \
  --batch-size 1024 \
  --flush-ms 1 \
  --checkpoint-enabled 1 \
  --truncate-on-start 1
```

运行 benchmark：

```bash
./build/log_engine_bench \
  --mode fast \
  --ack-mode memory_ack \
  --log-dir ./logs \
  --archive-dir ./archive \
  --messages 200000 \
  --payload-size 2048 \
  --batch-size 8192 \
  --flush-ms 0 \
  --inflight 1 \
  --checkpoint-enabled 0
```

运行长稳 / 回归 benchmark：

```bash
./script/bench_regression.sh --messages 100000 --repeats 3 --shards 1
./script/bench_soak.sh --target log_engine --duration-seconds 300 --messages 50000 --payload-size 2048 --batch-size 8192 --inflight 1 --shards 1 --ack-mode memory_ack
```

## Layout

- `include/log_engine`: 头文件
- `src`: 核心实现和可执行入口
- `proto`: gRPC proto
- `script`: 构建、测试、benchmark 脚本
- `doc`: 详细说明和 benchmark 结果

## Docs

- 详细使用说明：[doc/README.md](doc/README.md)
- 实施方案：`../docs/基于Seastar的高性能异步批量日志顺序写入引擎实施方案.md`

