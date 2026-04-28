# Seastar Log Engine

这是基于 Seastar 的异步批量顺序写日志引擎后续增强版实现，当前已经落地的能力包括：

- 基于 `seastar::sharded` 的 per-shard writer
- 按批量阈值或定时器触发刷盘
- 每个 shard 独立顺序写入自己的日志文件
- 按 route key 分片路由，支持 `hash_modulo` 和 consistent hashing + virtual nodes
- 按文件大小滚动 active log
- 归档目录维护与旧归档自动清理
- 记录级 CRC32 校验
- 每个 shard 的 checkpoint sidecar
- 启动时按 checkpoint + CRC 扫描恢复 active log
- 写失败重试
- 按大小和按时间的滚动触发
- gzip 归档与按数量/时间清理
- 配置文件加载
- 基于 `seastar::metrics` 的 writer 指标
- 日志读取 / 回溯 CLI
- HTTP / gRPC 查询接口
- 稳定的绑定式 glog 风格兼容头
- `demo` / `bench` / `verify` / `read` / `query_server` / `query_client` 六个可执行程序
- 独立的 `verify` 校验工具
- `glog` 对比 benchmark
- `spdlog` 对比 benchmark（当本机安装 `spdlog` 并成功被 CMake 发现时）
- 最小单元测试可执行程序

当前写入路径分成两种模式：

- `fast`：默认模式，尽量贴近 `spdlog` 的轻量异步文件追加；只支持 payload-only record，不支持 checkpoint / recovery / rotate / archive
- `full`：开启完整日志引擎语义，支持 structured record、checkpoint、恢复、rotate、archive 和压缩

当前目录结构：

- `include/log_engine`: 头文件
- `src`: 核心实现和 demo 入口
- `proto`: gRPC proto 定义
- `script/build.sh`: 本地构建脚本
- `logs`: 默认日志目录

当前版本还没有完成实施方案中的以下增强项：

- 更接近 drop-in 的 glog/spdlog 兼容 API 封装层
- 更系统化的单元测试与长稳压测

兼容层说明：

- 头文件在 `include/log_engine/compat_glog.hh`
- 当前只建议在 Seastar reactor 线程内使用
- 采用“绑定到外部 `LogEngine` 实例”的稳定模式，而不是内部自管全局引擎
- 推荐流程是：启动 `LogEngine`，`compat::bind(engine)`，使用 `LOG_INFO/LOG_WARNING/LOG_ERROR`，然后 `compat::flush()`，最后 `compat::unbind()`

构建：

```bash
./script/build.sh
```

配置文件：

```bash
./build/log_engine_demo --config ./config/engine.conf -c 2
```

运行 demo：

```bash
./build/log_engine_demo --mode full --routing-strategy consistent_hashing --routing-virtual-nodes 256 --log-dir ./logs --archive-dir ./archive --messages 100000 --batch-size 1024 --flush-ms 1 --rotate-size-bytes 1048576 --rotate-interval-seconds 0 --compress-archives 1 --checkpoint-enabled 1 --truncate-on-start 1
```

运行 benchmark：

```bash
./build/log_engine_bench --mode fast --log-dir ./logs --archive-dir ./archive --messages 200000 --payload-size 256 --batch-size 8192 --flush-ms 0 --inflight 1 --checkpoint-enabled 0
```

校验日志文件：

```bash
./build/log_engine_verify --path ./logs/shard-0.log
```

读取日志：

```bash
./build/log_engine_read --log-dir ./logs --archive-dir ./archive --include-archive 1 --seq-from 100 --seq-to 200 --limit 50
```

启动查询服务：

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

HTTP 查询接口：

```bash
curl 'http://127.0.0.1:18080/v1/status'
curl 'http://127.0.0.1:18080/v1/route?key=route-a'
curl 'http://127.0.0.1:18080/v1/records?shard=0&limit=10&include_archive=true'
curl 'http://127.0.0.1:19181/metrics'
```

gRPC 查询接口：

```bash
./build/log_engine_query_client --target 127.0.0.1:19090 --method status
./build/log_engine_query_client --target 127.0.0.1:19090 --method route --route-key route-a
./build/log_engine_query_client --target 127.0.0.1:19090 --method records --limit 10 --include-archive true
```

路由参数：

- `--routing-strategy hash_modulo|consistent_hashing`
- `--routing-virtual-nodes <n>`
- 空 `route_key` 仍然回退到当前 shard，本地无 key 写入不会被强制重路由

Writer metrics：

- 分组名：`log_engine_writer`
- 当前暴露的核心指标：`submitted_messages`、`submitted_bytes`、`flushed_batches`、`flushed_bytes`、`flush_errors`、`pending_entries`、`pending_bytes`、`logical_size_bytes`

恢复模式启动：

```bash
./build/log_engine_demo --mode full --log-dir ./logs --archive-dir ./archive --messages 1000 --truncate-on-start 0 --checkpoint-enabled 1
```

测试脚本：

```bash
./script/test_rotation.sh
./script/test_recovery.sh
./script/test_read.sh
./script/test_unit.sh
```

当前最小单元测试覆盖：

- 记录编解码与 CRC 校验
- 配置文件加载与参数覆盖
- consistent hashing 路由与 virtual nodes 基本行为
- 绑定式 `compat_glog` 写日志与 flush
- 按时间滚动后的 gzip 归档读取
- active log 追加损坏尾部后的恢复扫描

对比 benchmark：

```bash
./script/compare_bench.sh 50000 128
./script/compare_bench.sh --scan
```
