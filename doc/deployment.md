# Single-Machine Deployment

## Overview

`seastar-log-engine` runs as a Seastar-based daemon. Two binaries are needed for
a full deployment:

- `log_engine_demo` (or `log_engine_bench`) — the **writer**, ingests log messages
- `log_engine_query_server` — the **query server**, exposes HTTP + gRPC read APIs

Both binaries share the same `log_dir` and `archive_dir` so the query server can
read what the writer produces.

## System Requirements

- Linux kernel 5.x+ (io_uring support recommended)
- Seastar built with `--release` mode
- Sufficient disk space on `log_dir` and `archive_dir`

## Step 1: Build

```bash
cd /path/to/seastar-log-engine
cmake --build build -j$(nproc)
```

Binaries land in `build/`:
- `log_engine_demo` — demo writer with synthetic payloads
- `log_engine_query_server` — HTTP + gRPC query server
- `log_engine_bench` — benchmark writer

## Step 2: Create directories

```bash
mkdir -p /data/log-engine/logs
mkdir -p /data/log-engine/archive
```

## Step 3: Configuration file

Create `/etc/log-engine/engine.conf`:

```ini
log-dir=/data/log-engine/logs
archive-dir=/data/log-engine/archive
shard-file-prefix=shard

# Writer settings
ack-mode=write_ack
batch-size=256
flush-ms=1
write-behind=4
write-retry-count=3
write-retry-backoff-ms=2

# Backpressure (per shard)
max-pending-bytes=262144
pending-bytes-low-watermark=131072

# Rotation
rotate-size-bytes=67108864
rotate-interval-seconds=3600
archive-retention-seconds=86400
max-archived-files=16

# Features
checkpoint-enabled=true
compress-archives=true
truncate-on-start=false

# Record format
record-timestamp-enabled=true
record-crc-enabled=true
record-level-enabled=true
```

## Step 4: systemd unit (writer)

`/etc/systemd/system/log-engine-writer.service`:

```ini
[Unit]
Description=Seastar Log Engine Writer
After=network.target

[Service]
Type=simple
ExecStart=/opt/log-engine/bin/log_engine_demo \
    --config /etc/log-engine/engine.conf \
    --messages 0 \
    --payload-size 512 \
    --route-keys 16 \
    --emit-delay-ms 0 \
    -c 4
Restart=always
RestartSec=5
LimitNOFILE=65536
LimitMEMLOCK=infinity
CPUAffinity=0-3

[Install]
WantedBy=multi-user.target
```

Note: `-c 4` uses 4 Seastar shards (match to CPU cores). Adjust `CPUAffinity`
to pin shards to specific cores.

## Step 5: systemd unit (query server)

`/etc/systemd/system/log-engine-query.service`:

```ini
[Unit]
Description=Seastar Log Engine Query Server
After=network.target

[Service]
Type=simple
ExecStart=/opt/log-engine/bin/log_engine_query_server \
    --config /etc/log-engine/engine.conf \
    --http-address 0.0.0.0 \
    --http-port 18080 \
    --grpc-address 0.0.0.0 \
    --grpc-port 19090 \
    --metrics-address 0.0.0.0 \
    --metrics-port 19181 \
    --routing-shards 4 \
    -c 2
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

## Step 6: Start

```bash
systemctl daemon-reload
systemctl enable --now log-engine-writer log-engine-query
```

## Step 7: Verify

```bash
# Health check
curl http://localhost:18080/healthz
# {"ok":true}

# Full status
curl http://localhost:18080/v1/status

# Query recent records
curl "http://localhost:18080/v1/records?limit=10"

# Prometheus metrics
curl http://localhost:19181/metrics
```
