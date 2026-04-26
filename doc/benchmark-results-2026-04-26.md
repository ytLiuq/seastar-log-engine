# Benchmark Results 2026-04-26

本次结果来自本机直接执行对比脚本：

```bash
./script/compare_bench.sh
```

脚本默认参数：

- `messages=50000`
- `payload-size=128`

测试时间：

- `2026-04-26`

## Results

| Target | Messages | Elapsed (us) | Throughput (msg/s) | Avg submit (us) |
| --- | ---: | ---: | ---: | ---: |
| `log_engine_bench` | 50000 | 128955 | 387732.15 | 2.5791 |
| `glog_bench` | 50000 | 77108 | 648441 | N/A |
| `spdlog_bench` | 50000 | 48806 | 1024460 | N/A |

## Raw Output

```text
[log_engine_bench]
messages=50000 elapsed_us=128955 throughput_msg_per_sec=387732.15 avg_submit_us=2.5791
[glog_bench]
messages=50000 elapsed_us=77108 throughput_msg_per_sec=648441
[spdlog_bench]
messages=50000 elapsed_us=48806 throughput_msg_per_sec=1.02446e+06
```

## Notes

- `log_engine_bench` 运行时还会打印 Seastar 环境信息，例如 `perf_event_paranoid` 和 IO queue 提示；这些不是 benchmark 失败信号。
- 这是一组单次本机结果，只适合做当前环境下的相对对比，不应当外推为稳定结论。
- `log_engine_bench` 与 `glog/spdlog` 的实现模型不同：前者包含 Seastar 分片、批量、刷盘、checkpoint 和归档策略路径，后两者是更直接的日志写入 benchmark，因此吞吐差异需要结合功能路径一起看。
