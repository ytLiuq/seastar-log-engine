# Record Format Optimization Log 2026-05-02

目标：

- 持续优化 `record format` 编码路径
- 特别关注 `record_crc_enabled=true` 时的大 payload 成本

相关代码：

- [src/record_codec.cc](/root/workspace/seastar-log-engine/src/record_codec.cc:1)
- [src/async_writer.cc](/root/workspace/seastar-log-engine/src/async_writer.cc:200)
- [src/unit_tests.cc](/root/workspace/seastar-log-engine/src/unit_tests.cc:60)

相关 benchmark / 分析：

- [benchmark-profile-analysis-2026-05-02.md](/root/workspace/seastar-log-engine/doc/benchmark-profile-analysis-2026-05-02.md)
- [benchmark-profile-payload-analysis-2026-05-02.md](/root/workspace/seastar-log-engine/doc/benchmark-profile-payload-analysis-2026-05-02.md)
- [benchmark-profiles-2026-05-02.tsv](/root/workspace/seastar-log-engine/doc/benchmark-profiles-2026-05-02.tsv)
- [benchmark-profiles-payloads-2026-05-02.tsv](/root/workspace/seastar-log-engine/doc/benchmark-profiles-payloads-2026-05-02.tsv)
- [benchmark-profiles-payloads-post-zlib-2026-05-02.tsv](/root/workspace/seastar-log-engine/doc/benchmark-profiles-payloads-post-zlib-2026-05-02.tsv)
- [benchmark-profiles-payloads-post-slicing8-2026-05-02.tsv](/root/workspace/seastar-log-engine/doc/benchmark-profiles-payloads-post-slicing8-2026-05-02.tsv)

## 尝试记录

### 1. timestamp 前缀缓存

- 路径：
  - `AsyncWriter::format_timestamp()`
- 目的：
  - 降低每条日志都重新格式化完整时间字符串的成本
- 结果：
  - 生效
  - 后续 profile 显示 `payload=256` 时 `timestamp on` 相对 baseline 吞吐跌幅约 `9%` 到 `11%`
  - 说明 `timestamp` 已不是最重的热点

### 2. CRC 改为编码期增量计算，去掉整条 body 二次扫描

- 路径：
  - `encode_record_buffer()`
- 目的：
  - 避免先拼完 body，再单独再扫一次 body 计算 CRC
- 结果：
  - 生效
  - 把 CRC 成本压回到“单次编码路径”里，但没有改变 CRC 仍是主热点这一事实

### 3. 合并“字段/ payload 拷贝 + CRC 更新”的双遍历

- 路径：
  - `append_literal()`
  - `append_decimal()`
  - `append_sanitized_payload()`
- 目的：
  - 让字段写入和 CRC 更新在同一遍完成
- 结果：
  - 生效
  - 避免了字段路径上明显的重复扫描

### 4. 把字段固定字符串和 payload clean span 改成批量拷贝

- 路径：
  - `append_literal()`
  - `append_sanitized_payload()`
- 目的：
  - 去掉逐字节字段写入
  - payload 只在 `\\n/\\r/\\t` 上走单字节替换，其余区段批量拷贝
- 结果：
  - 生效
  - sanitize 回归测试已补
  - 这一轮之后，profile 明确显示瓶颈更多集中到 CRC 本身，而不是字符串拼装

### 5. 固定 benchmark profile，并建立 record-format / payload 分析基线

- 路径：
  - [script/bench_profiles.sh](/root/workspace/seastar-log-engine/script/bench_profiles.sh:1)
- 目的：
  - 固化 `timestamp/crc/structured fields/checkpoint/rotate` 的可复现 profile
  - 再扩展到多 payload profile
- 结果：
  - 生效
  - 关键结论：
    - `payload=256` 时，`crc on` 相对 baseline 吞吐跌幅约 `33%`
    - `payload=256/1024/4096` 下，这个跌幅都稳定在约 `33%`
    - 说明 CRC 是稳定主热点，不是小 payload 特例

### 6. 尝试切到 `zlib crc32`

- 路径：
  - `crc32_update()`
  - `crc32_update_byte()`
- 目的：
  - 用库实现替代手写逐字节 CRC，希望直接吃到更成熟的优化
- 验证：
  - `./build/log_engine_unit_tests -c 2`
  - `bash script/bench_profiles.sh --messages 30000 --payload-sizes 1024,4096 --batch-size 4096 --inflight 16 --repeats 3 --output-prefix benchmark-profiles-payloads-post-zlib-2026-05-02`
- 结果：
  - 不理想，已放弃
  - `record-crc-on`
    - `payload=1024` 吞吐约 `-1.8%`
    - `payload=4096` 吞吐约 `+1.8%`
  - `record-structured-on`
    - `payload=1024` 吞吐约 `-1.5%`
    - `payload=4096` 吞吐约 `+0.5%`
  - submit latency 没有改善，`1024` 甚至回退
  - 结论：
    - 这条路径收益太不稳定，不值得保留

### 7. 尝试手写 `slicing-by-8` CRC bulk update

- 路径：
  - `crc32_update()`
- 目的：
  - 保留当前编码边界不变，只优化 bulk CRC 更新算法
- 验证：
  - `./build/log_engine_unit_tests -c 2`
  - `bash script/bench_profiles.sh --messages 30000 --payload-sizes 1024,4096 --batch-size 4096 --inflight 16 --repeats 3 --output-prefix benchmark-profiles-payloads-post-slicing8-2026-05-02`
- 结果：
  - 有轻微改善，但量级不大
  - `record-crc-on`
    - `payload=1024` 吞吐约 `+1.4%`
    - `payload=4096` 吞吐约 `+1.9%`
  - `record-structured-on`
    - `payload=1024` 吞吐约 `+0.9%`
    - `payload=4096` 吞吐约 `-0.3%`
  - 结论：
    - 比 `zlib` 更贴近当前热点
    - 但还没有把 `crc` 从“大 payload 主瓶颈”降到次要问题

### 8. 尝试把 clean span 改成“拷贝 + CRC”单遍历

- 路径：
  - `append_literal(..., crc)`
- 目的：
  - 避免当前 `memcpy` 一遍、`crc32_update` 再扫一遍的双读取
- 验证：
  - `./build/log_engine_unit_tests -c 2`
  - `bash script/bench_profiles.sh --messages 30000 --payload-sizes 1024,4096 --batch-size 4096 --inflight 16 --repeats 3 --output-prefix benchmark-profiles-payloads-post-copycrc-2026-05-02`
- 结果：
  - 不理想，已放弃
  - `record-crc-on`
    - `payload=1024` 吞吐约 `-3.1%`
    - `payload=4096` 吞吐约 `+2.4%`
  - `record-structured-on`
    - `payload=1024` 吞吐约 `-1.9%`
    - `payload=4096` 基本持平
  - submit latency 整体没有改善
  - 结论：
    - 在当前实现里，把 copy 和 CRC 强绑成同一循环没有带来稳定收益
    - 目前保留 `slicing-by-8` bulk CRC + `memcpy` 的版本更稳妥

### 9. 尝试针对 `\n/\r/\t` 做专用 special-char 扫描

- 路径：
  - `append_sanitized_payload()`
- 目的：
  - 用定制扫描替代 `find_first_of("\n\r\t")`
  - 优化“大 payload 且大多数不含控制字符”的常见路径
- 验证：
  - `./build/log_engine_unit_tests -c 2`
  - `bash script/bench_profiles.sh --messages 30000 --payload-sizes 1024,4096 --batch-size 4096 --inflight 16 --repeats 3 --output-prefix benchmark-profiles-payloads-post-specialscan-2026-05-02`
- 结果：
  - 不理想，已放弃
  - 相对 `slicing-by-8` 版本：
    - `record-crc-on`
      - `payload=1024` 吞吐约 `-3.0%`
      - `payload=4096` 吞吐约 `-0.4%`
    - `record-structured-on`
      - `payload=1024` 吞吐约 `-7.0%`
      - `payload=4096` 吞吐约 `-4.2%`
  - 结论：
    - 专用扫描没有带来更好的热点表现
    - 当前保留 `find_first_of("\n\r\t")` 的实现更合适

## 当前结论

1. `timestamp` 和普通字段拼装已经不是 record-format 主瓶颈。
2. `record_crc_enabled=true` 仍然是最值得继续投时间的路径。
3. 已经试过“库实现替换”、“更快的 bulk CRC 算法”、“copy+CRC 单遍历”、“专用 special-char 扫描”四条路，说明单纯微调 CRC 内核或局部遍历形态只能拿到很有限的收益。
4. 下一步更可能有效的方向不是继续微调 timestamp，而是继续减少 CRC 路径上的总处理量，或者改变 CRC 路径与 payload 写入的耦合方式。

## 鲁棒性记录

### 10. 启动恢复时忽略残缺 checkpoint，回退到 verified scan

- 路径：
  - [src/log_manager.cc](/root/workspace/seastar-log-engine/src/log_manager.cc:23)
  - [src/unit_tests.cc](/root/workspace/seastar-log-engine/src/unit_tests.cc:384)
- 问题：
  - 之前 `read_checkpoint_file()` 只要能打开文件，就会返回一个 `CheckpointState`
  - 如果 checkpoint 被部分写入或截断，缺失字段会保持默认值 `0`
  - 恢复时 `valid_size = min(valid_size, checkpoint.logical_size)` 可能因此被错误压成 `0`
  - 结果是 active log 明明有有效记录，却会被当成“全损坏尾部”处理
- 修复：
  - 只有当 `logical_size / sequence / rotation_index` 三个 key 都存在时，checkpoint 才视为有效
  - 否则直接忽略 checkpoint，回退到 active log 的 verified recovery scan 结果
- 验证：
  - `cmake --build build --target log_engine_unit_tests -j$(nproc)`
  - `./build/log_engine_unit_tests -c 2`
- 结果：
  - 生效
  - 新增 `test_partial_checkpoint_ignored()`
  - 用仅包含 `sequence=999999` 的残缺 checkpoint 验证恢复逻辑
  - 重启恢复后 `logical_size > 0` 且 `sequence == 2`，确认不会再把有效日志错误截断

### 11. 启动恢复时忽略“完整但过旧”的 checkpoint

- 路径：
  - [src/log_manager.cc](/root/workspace/seastar-log-engine/src/log_manager.cc:147)
  - [src/unit_tests.cc](/root/workspace/seastar-log-engine/src/unit_tests.cc:421)
- 问题：
  - 之前只要 checkpoint 的 `logical_size <= verified.valid_size`，恢复逻辑就会采信它
  - 这会让“完整但落后于 active log”的旧 checkpoint 把 `valid_size` 回退到更小值
  - 结果是较新的有效记录已经落盘，但重启后仍可能被错误截断
- 修复：
  - 只有当 checkpoint 的 `logical_size` 与 verified scan 完全一致时，才采用 checkpoint 的 `sequence / rotation_index`
  - 其余情况一律保守回退到 verified scan 结果
- 验证：
  - `cmake --build build --target log_engine_unit_tests -j$(nproc)`
  - `./build/log_engine_unit_tests -c 2`
- 结果：
  - 生效
  - 新增 `test_stale_checkpoint_ignored()`
  - 用“完整但 logical_size 只覆盖第一条记录”的旧 checkpoint 验证恢复逻辑
  - 重启恢复后 `logical_size == verified.valid_size`，确认不会再把较新的有效记录裁掉

### 12. 归档枚举时去重 `.log/.log.gz` 冲突，并把 gzip 输出改成临时文件原子落盘

- 路径：
  - [src/log_layout.cc](/root/workspace/seastar-log-engine/src/log_layout.cc:81)
  - [src/log_manager.cc](/root/workspace/seastar-log-engine/src/log_manager.cc:210)
  - [src/unit_tests.cc](/root/workspace/seastar-log-engine/src/unit_tests.cc:361)
- 问题：
  - 如果压缩归档过程中异常中断，目录里可能同时留下同一段 archive 的 `.log` 和 `.log.gz`
  - 查询路径之前会把两个文件都当成独立 segment 读入，导致重复结果，最坏情况还会读到压缩半成品
- 修复：
  - archive 枚举时，按 `shard_id + timestamp_ms + rotation_index` 去重
  - 同一段同时出现 `.log` 和 `.log.gz` 时，优先选择未压缩 `.log`
  - gzip 输出改为先写 `.gz.tmp`，完成后再原子重命名到最终 `.gz`
- 验证：
  - `cmake --build build --target log_engine_unit_tests -j$(nproc)`
  - `./build/log_engine_unit_tests -c 2`
- 结果：
  - 生效
  - 新增 `test_archive_duplicate_prefers_plain_log()`
  - 手工构造同一 archive 的 `.log` 与 `.log.gz` 冲突文件后，读路径只返回 `.log` 中的那份 archived record

### 13. query 读取损坏 segment 时按“前缀有效、后缀截断”处理

- 路径：
  - [src/log_reader.cc](/root/workspace/seastar-log-engine/src/log_reader.cc:84)
  - [src/unit_tests.cc](/root/workspace/seastar-log-engine/src/unit_tests.cc:413)
- 问题：
  - 之前 query 读路径遇到无法解析的坏行时，只会跳过该行并继续读同一个 segment
  - 这意味着 archive 中间一旦出现损坏，后面的内容仍会被继续返回，语义上过于激进
- 修复：
  - 一旦某个 segment 里出现解析失败的记录，立即停止读取该 segment
  - 当前 segment 只保留损坏点之前已经验证通过的记录
  - 后续 segment 仍继续读取，避免一个坏 archive 把整个查询链路拖死
- 验证：
  - `cmake --build build --target log_engine_unit_tests -j$(nproc)`
  - `./build/log_engine_unit_tests -c 2`
- 结果：
  - 生效
  - 新增 `test_reader_stops_after_corrupted_segment_line()`
  - 手工构造“有效 archive 记录 + CRC 错误坏行 + 后续合法记录”的场景后，查询结果只保留损坏前 archive 记录，并继续返回 active segment 中的记录

### 14. 为损坏 segment / 损坏 gzip 增加 reader 告警和计数

- 路径：
  - [include/log_engine/log_reader.hh](/root/workspace/seastar-log-engine/include/log_engine/log_reader.hh:24)
  - [src/log_reader.cc](/root/workspace/seastar-log-engine/src/log_reader.cc:12)
  - [src/unit_tests.cc](/root/workspace/seastar-log-engine/src/unit_tests.cc:451)
- 问题：
  - 之前 query 读路径即使已经做了保守截断，外部也很难知道“这次查询到底有没有碰到损坏 archive”
  - 特别是损坏 `.gz`，如果没有显式告警和计数，线上排查会很被动
- 修复：
  - 增加 `ReaderStats`
    - `corrupted_segments`
    - `corrupted_lines`
    - `gzip_read_errors`
  - `log_reader` 在遇到 parse failure 或 gzip stream 读错时输出 `warn`
  - 保留现有保守语义：
    - 当前 segment 到损坏点为止
    - 后续 segment 继续读取
- 验证：
  - `cmake --build build --target log_engine_unit_tests -j$(nproc)`
  - `./build/log_engine_unit_tests -c 2`
- 结果：
  - 生效
  - 新增 `test_reader_skips_broken_gzip_archive()`
  - 用“合法 gzip record 后截断文件尾”的方式构造损坏 gzip
  - 查询结果会保留 gzip 中可读前缀记录，并继续返回 active segment 记录
  - 单测同时验证 `gzip_read_errors == 1` 且 `corrupted_segments == 1`

### 15. 将 reader 异常计数统一接入 Prometheus `/metrics`

- 路径：
  - [include/log_engine/log_reader.hh](/root/workspace/seastar-log-engine/include/log_engine/log_reader.hh:31)
  - [src/log_reader.cc](/root/workspace/seastar-log-engine/src/log_reader.cc:163)
  - [src/query_server.cc](/root/workspace/seastar-log-engine/src/query_server.cc:380)
  - [doc/README.md](/root/workspace/seastar-log-engine/doc/README.md:181)
- 目的：
  - 不再让 reader 侧异常计数只停留在进程内 helper 和日志里
  - 与现有 `log_engine_writer` 指标统一到同一个 Prometheus 端点暴露
- 修复：
  - 为 `log_reader` 增加 metrics 注册/反注册接口
  - `query_server` 启动时注册 `log_engine_reader` 分组
  - 文档补充 reader metrics 的查看位置和指标名
- 验证：
  - `cmake --build build --target log_engine_query_server log_engine_unit_tests -j$(nproc)`
  - `./build/log_engine_unit_tests -c 2`
- 结果：
  - 生效
  - 当前 `/metrics` 中除了 `log_engine_writer_*` 外，还会暴露 `log_engine_reader_*`
  - reader 侧核心指标统一为：
    - `corrupted_segments`
    - `corrupted_lines`
    - `gzip_read_errors`

### 16. 将 reader 异常计数补进 HTTP / gRPC status 接口

- 路径：
  - [proto/log_engine_query.proto](/root/workspace/seastar-log-engine/proto/log_engine_query.proto:7)
  - [src/query_server.cc](/root/workspace/seastar-log-engine/src/query_server.cc:109)
  - [src/query_client.cc](/root/workspace/seastar-log-engine/src/query_client.cc:22)
  - [doc/README.md](/root/workspace/seastar-log-engine/doc/README.md:181)
- 目的：
  - 不依赖 Prometheus 时，也能直接从状态接口看到 query 读路径的异常累计情况
  - 保持 HTTP `/v1/status` 与 gRPC `status` 输出一致
- 修复：
  - `StatusReply` 增加：
    - `reader_corrupted_segments`
    - `reader_corrupted_lines`
    - `reader_gzip_read_errors`
  - HTTP `/v1/status` 输出增加 `reader_stats` 子对象
  - gRPC client 的 `status` 输出拼成与 HTTP 一致的 JSON
- 验证：
  - `cmake --build build --target log_engine_query_server log_engine_query_client -j$(nproc)`
  - `bash script/test_query_consistency.sh`
- 结果：
  - 生效
  - `status` 接口现在能直接展示 reader 异常计数
  - HTTP / gRPC `status`、`route`、`records` 一致性测试继续通过

### 17. 为 status 增加 `health` 汇总字段

- 路径：
  - [proto/log_engine_query.proto](/root/workspace/seastar-log-engine/proto/log_engine_query.proto:7)
  - [src/query_server.cc](/root/workspace/seastar-log-engine/src/query_server.cc:109)
  - [src/query_client.cc](/root/workspace/seastar-log-engine/src/query_client.cc:22)
  - [TODO.md](/root/workspace/seastar-log-engine/TODO.md:47)
- 目的：
  - 让运维侧不用先理解所有原始计数，也能直接看出当前 query 读路径是否已经退化
- 修复：
  - `StatusReply` 增加 `health`
  - HTTP `/v1/status` 与 gRPC `status` 同步输出 `health`
  - 当前保守规则：
    - `reader_stats.corrupted_segments > 0` 或
    - `reader_stats.corrupted_lines > 0` 或
    - `reader_stats.gzip_read_errors > 0`
    - 任一成立即返回 `degraded`
    - 否则返回 `ok`
- 验证：
  - `cmake --build build --target log_engine_query_server log_engine_query_client -j$(nproc)`
  - `bash script/test_query_consistency.sh`
- 结果：
  - 生效
  - `status` 现在除了原始计数，还会给出明确的健康汇总
  - HTTP / gRPC 输出保持一致

### 18. 补 reader 侧 query 命中统计

- 路径：
  - [include/log_engine/log_reader.hh](/root/workspace/seastar-log-engine/include/log_engine/log_reader.hh:23)
  - [src/log_reader.cc](/root/workspace/seastar-log-engine/src/log_reader.cc:19)
  - [proto/log_engine_query.proto](/root/workspace/seastar-log-engine/proto/log_engine_query.proto:7)
  - [src/unit_tests.cc](/root/workspace/seastar-log-engine/src/unit_tests.cc:358)
- 目的：
  - 把“query 到底读了多少 segment、命中了多少 archive、返回了多少记录”纳入统一观测
- 修复：
  - `ReaderStats` 增加：
    - `segments_read`
    - `archive_segments_read`
    - `active_segments_read`
    - `records_returned`
  - 这些计数统一接入：
    - Prometheus `log_engine_reader_*`
    - HTTP `/v1/status`
    - gRPC `status`
- 验证：
  - `cmake --build build --target log_engine_query_server log_engine_query_client log_engine_unit_tests -j$(nproc)`
  - `bash script/test_query_consistency.sh`
  - `./build/log_engine_unit_tests -c 2`
- 结果：
  - 生效
  - 新增/扩展单测验证 archive + active 读回后，reader 计数符合预期
  - query consistency 测试继续通过
