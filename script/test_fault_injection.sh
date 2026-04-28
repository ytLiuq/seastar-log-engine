#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

cleanup() {
  if [[ -n "${demo_pid:-}" ]]; then
    kill -9 "${demo_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

./script/clean_logs.sh >/tmp/log_engine_clean_fault.out 2>&1 || true
mkdir -p "${ROOT_DIR}/logs" "${ROOT_DIR}/archive"

timeout 15s ./build/log_engine_demo \
  --mode full \
  --ack-mode sync_ack \
  --log-dir "${ROOT_DIR}/logs" \
  --archive-dir "${ROOT_DIR}/archive" \
  --messages 1000000 \
  --payload-size 128 \
  --batch-size 64 \
  --emit-delay-ms 1 \
  --checkpoint-enabled 1 \
  --truncate-on-start 1 \
  -c 2 >/tmp/log_engine_fault_kill.out 2>&1 &
demo_pid=$!
sleep 1
kill -9 "${demo_pid}"
wait "${demo_pid}" || true
demo_pid=""

./build/log_engine_demo \
  --mode full \
  --ack-mode sync_ack \
  --log-dir "${ROOT_DIR}/logs" \
  --archive-dir "${ROOT_DIR}/archive" \
  --messages 32 \
  --payload-size 128 \
  --batch-size 8 \
  --checkpoint-enabled 1 \
  --truncate-on-start 0 \
  -c 2 >/tmp/log_engine_fault_recover.out 2>&1

./build/log_engine_verify --path "${ROOT_DIR}/logs/shard-0.log"

printf 'BROKEN_TAIL' >> "${ROOT_DIR}/logs/shard-0.log"
printf 'logical_size=7\nsequence=999999\nrotation_index=0\n' > "${ROOT_DIR}/logs/shard-0.log.checkpoint"

./build/log_engine_demo \
  --mode full \
  --ack-mode sync_ack \
  --log-dir "${ROOT_DIR}/logs" \
  --archive-dir "${ROOT_DIR}/archive" \
  --messages 8 \
  --payload-size 64 \
  --batch-size 4 \
  --checkpoint-enabled 1 \
  --truncate-on-start 0 \
  -c 2 >/tmp/log_engine_fault_corrupt.out 2>&1

./build/log_engine_verify --path "${ROOT_DIR}/logs/shard-0.log"
echo "fault injection test passed"
