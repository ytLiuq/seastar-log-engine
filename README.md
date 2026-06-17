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

运行日志 agent MVP：

```bash
./build/log_engine_agent --config config/agent.conf -c 2
```

写入一条日志：

```bash
curl -X POST http://127.0.0.1:18081/v1/logs \
  -H 'Content-Type: application/json' \
  -d '{"service":"order","level":"info","message":"created order"}'
```

查看 agent 状态：

```bash
curl http://127.0.0.1:18081/v1/status
```

作为文件采集 agent 使用：

```bash
./build/log_engine_agent \
  --config config/agent.conf \
  --file-source-glob '/var/log/app/*.log' \
  --sink-kind http \
  --sink-http-url http://127.0.0.1:9000/v1/logs \
  --sink-http-headers 'Authorization: Bearer TOKEN; X-Agent: edge-a' \
  --sink-http-timeout-ms 5000 \
  --sink-http-retryable-statuses 408,425,429,500,502,503,504 \
  --max-buffer-bytes 1073741824 \
  --resume-buffer-bytes 805306368
```

这个模式会 tail 本地文件，只提交已经换行结束的完整日志行；glob 模式下每个文件维护独立的 `source-offset-path.*`，检测到 rename rotation 或 truncate 后会从新文件头继续读取，并在 `/v1/status` 的 `source_rotations` 里累计。sink 投递前会把当前批次写到 `pending-delivery-path`，批次 JSON 带 `agent_id`、`shard`、`first_sequence`、`next_sequence`，成功收到 sink ACK 后再按 shard 推进 `delivery-offset-path` 并删除 pending 文件；如果进程在 ACK 前崩溃，重启后会优先重放 pending 批次。如果本地缓冲目录达到 `max-buffer-bytes`，或者 sink backlog / 失败数 / 延迟超过动态阈值，输入源会暂停，避免继续放大积压。

可靠性语义是 at-least-once：ACK 前崩溃会重放 pending 批次；如果远端 sink 已经 ACK、但本地 delivery offset 还没持久化就崩溃，重启后可能重复投递该批次。重复窗口被 `agent_id + shard + first_sequence + next_sequence` 限定，远端 sink 应按这些幂等字段去重。

离线导出未 ACK 批次：

```bash
./build/log_engine_agent_replay \
  --config config/agent.conf \
  --agent-id edge-a \
  --delivery-offset-path agent-delivery.offset \
  --batch-size 100
```

该命令会从 `delivery-offset-path` 记录的 per-shard sequence 开始读取本地 log engine，按 JSON lines 输出带幂等字段的补投批次，可用于人工排查或接入外部补投脚本。

Agent 输入源：

- `--file-source-path /var/log/app.log`
- `--file-source-glob '/var/log/app/*.log'`
- `--stdin-source-enabled true`
- `--unix-socket-source-path /tmp/seastar-log-agent.sock`
- `--tcp-source-port 19000`
- `--udp-source-port 19001`

Agent sink：

- `--sink-kind stdout`：直接输出到 stdout，适合调试和容器日志管道。
- `--sink-kind http --sink-http-url http://host:port/path`：批量 POST JSON；`--sink-http-headers 'Name: value; Other: value'` 追加自定义请求头，`--sink-http-timeout-ms` 控制请求超时，`--sink-http-retryable-statuses` 控制哪些非 2xx 状态进入重试退避。
- `--sink-kind kafka/object_store`：配置边界已预留，当前构建不会链接外部客户端，启动后会明确报错。

容器运行：

```bash
docker build --target agent-runtime -t seastar-log-engine:agent .
docker compose up --build
```

`docker-compose.yml` 默认以非 root 用户运行，挂载 `agent-data` 保存本地日志和 checkpoint，并通过 `/healthz` 做健康检查。

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
