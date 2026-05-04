# TODO

更新时间：`2026-05-03`

## 进行中

- [ ] 继续做 `record format` 优化
  - 已完成：
    - timestamp 前缀缓存
    - `record_crc_enabled=true` 时改为编码时增量计算 CRC，去掉对整条 body 的二次扫描
    - CRC 开启时把“字段/ payload 拷贝 + CRC 更新”的双遍历合并
    - 字段拼装里的固定字符串 / 干净片段改为块拷贝后批量更新 CRC
    - payload sanitize 改为按 clean span 批量拷贝，只对 `\n/\r/\t` 单字节替换
    - 固定 benchmark profile 脚本已补齐，可直接量化 `timestamp/crc/structured fields` 开关开销
    - 已跑 3 次 profile，`timestamp` 吞吐跌幅约 `9%`，`crc` 约 `33%`，全量 structured fields 约 `42%`
    - 已补多 payload profile；`payload=256/1024/4096` 下 `crc` 相对 baseline 的吞吐跌幅都稳定在约 `33%`
    - 已补优化尝试记录文档，按“路径 / 验证 / 结果”整理每次尝试
    - 已尝试 `zlib crc32`，收益不稳定，已放弃
    - 已尝试 `slicing-by-8` bulk CRC，`payload=1024/4096` 下 `crc on` 吞吐改善约 `1%` 到 `2%`
    - 已支持 `record_crc_class=none|header|payload_hash|full`，可按 payload 覆盖范围做分级校验
    - 已完成 `payload_hash` 原型：对 payload 先做 64-bit hash，再对 metadata + hash 做 CRC
    - 当前基准里 `payload_hash` 相对 `full`：
      - `payload=1024`：约 `149.9k -> 157.0k msg/s`，提升约 `4.7%`
      - `payload=4096`：约 `60.4k -> 64.9k msg/s`，提升约 `7.5%`
  - 待继续：
    - 继续评估 `payload_hash` 在更大 payload 和真实负载下的收益是否稳定
    - 明确 `payload_hash` 相对 `full` 的使用边界和风险说明

## 待做

- [x] 增强恢复链路鲁棒性
  - 已完成：
    - `checkpoint` 文件现在要求完整包含 `logical_size / sequence / rotation_index`
    - 遇到残缺或截断的 `checkpoint` 文件时，恢复逻辑会忽略该 checkpoint，回退到 active log 校验扫描结果
    - 遇到“完整但过旧”的 `checkpoint` 文件时，恢复逻辑不再把 `logical_size` 回退到旧值
    - archive 枚举时如果同一段归档同时出现 `.log` 和 `.log.gz`，查询路径现在优先使用 `.log`
    - gzip 归档改为先写 `.gz.tmp` 再原子重命名，避免压缩中断时把半成品 `.gz` 暴露给读路径
    - 启动时会清理遗留 `.checkpoint.tmp` / `.gz.tmp` sidecar，checkpoint 写失败也会回收临时文件
    - query 读 segment 时一旦遇到损坏行，现在会停止继续读取该 segment，只保留前面已验证通过的记录
    - archive cleanup / query 并发导致文件在枚举后消失时，查询会保守跳过，不计为损坏
    - `log_reader` 现在会对损坏 segment 和损坏 gzip 输出 `warn`，并累计 `corrupted_segments / corrupted_lines / gzip_read_errors` 计数
    - reader 异常计数已经接入 Prometheus `/metrics`，和现有 writer metrics 统一暴露
    - HTTP `/v1/status` 和 gRPC `status` 现在也会返回 reader 异常计数，便于不用 Prometheus 时直接查看
    - `status` 现在会直接输出 `recovery_fallback_reason`
    - `status` 现在会输出 `health_reason / health_reason_basis`
    - 已补“仅剩 `sequence=...` 的残缺 checkpoint”单测
    - 已补“完整但 logical_size 落后于 active log”的 stale checkpoint 单测
    - 已补“同一 archive 同时存在 `.log/.log.gz`”单测
    - 已补“archive 中间出现损坏行”单测
    - 已补“截断 gzip archive”单测
    - 已补“archive cleanup 后文件消失”单测
    - 已补“启动清理 `.checkpoint.tmp/.gz.tmp`”单测

- [x] 收敛运维健康度闭环
  - 已完成：
    - HTTP `/v1/status` 和 gRPC `status` 已增加 `health` 汇总字段
    - 已补更细的健康判定优先级：
      - `checkpoint_write_failures_recent` / `gzip_archive_failures_recent` 任一非零即 `unhealthy`
      - `recovery_fallbacks_recent` / `reader_gzip_read_errors_recent` 超过小阈值即 `unhealthy`
      - `reader_corrupted_segments_recent` / `reader_corrupted_lines_recent` 超过较高阈值即 `unhealthy`
      - 其余 recent error 非零时为 `degraded`
    - `status` 现在会直接输出 `health_reason / health_reason_basis`
    - 已补 Prometheus 告警规则样例、Grafana dashboard 草案、故障排查手册

- [x] 增加长稳与故障注入验证闭环
  - 已完成：
    - 已有 `script/test_fault_injection.sh`
    - 已有 `script/test_soak_and_fault.sh`
    - 已有 `script/bench_query_jitter.sh`
    - `test_soak_and_fault.sh` 已改成真正按时长执行的 timed soak
    - soak loop 已覆盖 rotate / restart / archive reset / recover 组合验证
    - `test_fault_injection.sh` 已增加 active write failure 注入
    - 多 shard 恢复一致性现在有固定判定：所有 shard 可验证且 `total_valid_records > 0`

- [ ] 补多 shard 与真实负载性能画像
  - 已完成：
    - 已有单次 query/write 并发抖动测量脚本 `script/bench_query_jitter.sh`
    - 已有固定 profile 脚本 `script/bench_profiles.sh`
    - 已补 `LogEngine::append_batch()`，会先按目标 shard 聚合，再做批量跨 shard 提交
    - benchmark 已补 `--submit-group-size`，可量化“逐条跨 shard submit”和“按 shard 批量 submit”的差异
    - 当前手工基准里，`2 shard + route_keys=16` 下批量提交相对逐条提交已有小幅吞吐改善：
      - `payload=512`：约 `338k -> 347k msg/s`
      - `payload=2048`：约 `143.7k -> 154.5k msg/s`
  - 待做：
    - 把 `submit-group-size` 纳入固定多 shard benchmark 矩阵
    - 增加不同 ack-mode / payload / route-key 分布下的尾延迟统计
    - 增加 backpressure 触发区间的吞吐/延迟曲线
    - 把 query 与 write 并发抖动测量纳入可复现报告

- [x] 清理过时脚本和文档
  - 已完成：
    - 已清理主干脚本里残留的 `--mode fast|full`
    - 已清理主干脚本和说明里的 `memory_ack`
    - 已同步 `doc/README.md` 里的过时 `query_client` 链接问题描述
    - 已补 `skip-query-checks` 和受限环境说明

## 已完成

- [x] 补 benchmark profile
  - 已完成：
    - 新增 `script/bench_profiles.sh`
    - 固化 `timestamp on/off`
    - 固化 `crc on/off`
    - 固化 `structured fields on/off`
    - 固化 `checkpoint on/off`
    - 固化 `rotate on/off`
    - 输出可复现的 TSV / Markdown 摘要

- [x] 增加 query HTTP / gRPC 一致性测试
  - 已完成：
    - 新增 `script/test_query_consistency.sh`
    - 覆盖 `status` / `route` / `records` 三类接口的 HTTP 与 gRPC 输出一致性校验
    - `records` 测试场景包含 rotate 后的 archive + active 读回
    - 修复 `RouteReply` 缺少 `route_key` 导致 HTTP / gRPC route 输出不一致的问题

- [x] 增加恢复与 rotate 组合测试
  - 已完成：
    - 单测覆盖 `checkpoint on/off`
    - 单测覆盖 `compress_archives on/off`
    - 单测覆盖大 payload 触发 size rotate 后的 archive 读回
    - 单测覆盖 active 尾部损坏后重启恢复再追加
    - 单测覆盖残缺 `checkpoint` 被忽略并回退到 verified recovery scan
    - 单测覆盖过旧 `checkpoint` 被忽略并保留较新的有效 active records
    - 单测覆盖 archive `.log/.log.gz` 冲突时优先保留 `.log`
    - 单测覆盖损坏 segment 在 query 读路径上按“前缀有效、后缀截断”处理
    - 单测覆盖损坏 gzip archive 触发 `warn`/计数并继续保留后续 active 结果
    - 顺带修复未压缩 archive `*.log` 未被 query / read 路径识别的问题

- [x] 增加 per-shard 背压 / 水位控制
  - 已完成：
    - 增加 `max_pending_bytes` / `pending_bytes_low_watermark`
    - `AsyncWriter` 在超水位时触发 flush 并等待 pending queue 下降
    - 增加 `backpressure_waits` / `waiting_submitters` metrics
    - 打通 `config_loader`、`main`、`bench`、示例配置和单测

- [x] 补 log-manager / health 指标
  - 已完成：
    - 已增加 `rotate_operations`
    - 已增加 `checkpoint_write_successes / checkpoint_write_failures`
    - 已增加 `recovery_fallbacks`
    - 已增加 `gzip_archive_successes / gzip_archive_failures`
    - 上述计数已经统一接入 Prometheus `/metrics` 和 `status`

- [x] 补运维样板文档
  - 已完成：
    - 已增加 Prometheus 告警规则样例
    - 已增加 Grafana dashboard 草案
    - 已增加故障排查手册和健康状态说明

- [x] 修复 `log_engine_query_client` 的 `fmt` 链接问题，确保全量构建通过

- [x] 清理 README 中主要过时的 `fast/full`、`memory_ack` 说法
  - 说明：
    - 根目录 `README.md` 已完成主要清理

- [x] 删除历史快/慢双写路径，统一到单一 DMA 对齐写入链路
- [x] 引入 `log_layout` 并迁移到 `SegmentDescriptor`
- [x] 修复 `force_flush()` 导致 time rotation 场景 active record 丢失的问题
- [x] 去掉 chunked flush 里多余的 aligned buffer 拷贝
- [x] 恢复路径去掉 `stringstream` 整文件二次拷贝
- [x] 修正单测里对 `route_key` 和 time rotation 的过时假设
