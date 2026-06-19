# Delivery Scanner Benchmark Plan

## Goal

Verify that per-shard scanners plus the bounded sink dispatcher can keep up with local ingest, and identify whether scanning, dispatch concurrency, remote RTT, or durable state updates limit throughput.

## Test Matrix

- Shards: `1,2,4,8`
- Payload: `128,1024,4096` bytes
- Sink batch: `10,100,500,1000`
- Dispatcher concurrency: `1,2,4,8`
- Dispatch queue capacity: `16,64,256`
- Simulated sink RTT: `0,1,10,50,100` ms
- Sink failures: none, intermittent `503`, outage/recovery

Run each stable case at least three times after one warm-up run.

Use the parent commit in a separate worktree as the centralized-scanner baseline.
Run the same source data, shard count, sink delay, and batch size against both
versions; do not use `dispatchers=1` as a substitute because it still retains
the new per-shard scanner behavior.

## Required Metrics

- Source committed records per second
- Sink ACKed records per second
- End-to-end append-to-ACK latency P50/P95/P99
- Peak and average dispatch queue depth
- Per-shard pending batch age
- Per-shard delivery lag (`latest local sequence - next delivery sequence`)
- Scanner records and bytes per second
- Dispatcher worker utilization
- Pending/offset state persistence latency
- Reactor utilization and stall reports
- Process RSS and disk usage

The provided script currently records elapsed time, ACK throughput, peak
backlog, peak queued batches, peak active workers, and completion status. Use
Seastar metrics and reactor stall reports
for CPU/reactor observations. Per-shard lag, queue wait latency, and persistence
latency require additional production metrics before they can be used as
continuous regression gates.

## Bottleneck Criteria

The delivery pipeline is the bottleneck when local source commits remain above sink ACK throughput, delivery lag grows continuously, the remote sink still has spare capacity, and scanner or dispatcher utilization is saturated.

Head-of-line blocking is present when one shard's failures or latency cause unrelated shards' delivery lag to increase.

## Commands

Basic sweep:

```bash
bash ./script/bench_delivery_scanner.sh \
  --messages 20000 \
  --payload-size 128 \
  --shards 1,2,4,8 \
  --batch-sizes 100,500 \
  --dispatchers 1,2,4,8 \
  --sink-delays-ms 0,10,50 \
  --repeats 3
```

Large payload:

```bash
bash ./script/bench_delivery_scanner.sh \
  --messages 10000 \
  --payload-size 4096 \
  --shards 1,4,8 \
  --batch-sizes 100,500 \
  --dispatchers 2,4,8 \
  --sink-delays-ms 0,10 \
  --repeats 3
```

Failure and recovery:

```bash
bash ./script/bench_delivery_scanner.sh \
  --messages 20000 \
  --shards 4 \
  --dispatchers 4 \
  --sink-delays-ms 10 \
  --sink-fail-first 20 \
  --repeats 3
```

## Acceptance Targets

- Increasing shards from 1 to 4 should not reduce ACK throughput when the sink has capacity.
- Increasing dispatcher concurrency should improve throughput for non-zero sink RTT until the sink saturates.
- A failing shard should not stop successful delivery from other shards.
- Queue capacity must remain bounded and source backpressure must activate before disk limits are exceeded.
- Restart must replay each shard's pending batch and resume from its persisted offset.
