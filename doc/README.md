# Seastar Log Engine

这是基于 Seastar 的异步批量顺序写日志引擎后续增强版实现，当前已经落地的能力包括：

- 基于 `seastar::sharded` 的 per-shard writer
- 按批量阈值或定时器触发刷盘
- 每个 shard 独立顺序写入自己的日志文件
- 简单的按 route key 分片路由
- 按文件大小滚动 active log
- 归档目录维护与旧归档自动清理
- 记录级 CRC32 校验
- 每个 shard 的 checkpoint sidecar
- 启动时按 checkpoint + CRC 扫描恢复 active log
- 写失败重试
- 按大小和按时间的滚动触发
- gzip 归档与按数量/时间清理
- 配置文件加载
- 日志读取 / 回溯 CLI
- 稳定的绑定式 glog 风格兼容头
- `demo` / `bench` / `verify` / `read` 四个可执行程序
- 独立的 `verify` 校验工具
- `glog` 对比 benchmark
- 最小单元测试可执行程序

当前目录结构：

- `include/log_engine`: 头文件
- `src`: 核心实现和 demo 入口
- `script/build.sh`: 本地构建脚本
- `logs`: 默认日志目录

当前版本还没有完成实施方案中的以下增强项：

- spdlog 对比 benchmark
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
./build/log_engine_demo --log-dir ./logs --archive-dir ./archive --messages 100000 --batch-size 1024 --flush-ms 1 --rotate-size-bytes 1048576 --rotate-interval-seconds 0 --compress-archives 1 --checkpoint-enabled 1 --truncate-on-start 1
```

运行 benchmark：

```bash
./build/log_engine_bench --log-dir ./logs --archive-dir ./archive --messages 200000 --payload-size 256 --batch-size 1024 --inflight 256 --checkpoint-enabled 1
```

校验日志文件：

```bash
./build/log_engine_verify --path ./logs/shard-0.log
```

读取日志：

```bash
./build/log_engine_read --log-dir ./logs --archive-dir ./archive --include-archive 1 --seq-from 100 --seq-to 200 --limit 50
```

恢复模式启动：

```bash
./build/log_engine_demo --log-dir ./logs --archive-dir ./archive --messages 1000 --truncate-on-start 0 --checkpoint-enabled 1
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
- 绑定式 `compat_glog` 写日志与 flush
- 按时间滚动后的 gzip 归档读取
- active log 追加损坏尾部后的恢复扫描

对比 benchmark：

```bash
./script/compare_bench.sh 50000 128
```
