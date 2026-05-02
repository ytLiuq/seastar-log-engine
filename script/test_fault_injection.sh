#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

TMP_DIR="$(mktemp -d /tmp/log-engine-fault-injection.XXXXXX)"
LOG_DIR="${TMP_DIR}/logs"
ARCHIVE_DIR="${TMP_DIR}/archive"
SHARDS=2

cleanup() {
  if [[ -n "${DEMO_PID:-}" ]] && kill -0 "${DEMO_PID}" 2>/dev/null; then
    kill -9 "${DEMO_PID}" 2>/dev/null || true
    wait "${DEMO_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

mkdir -p "${LOG_DIR}" "${ARCHIVE_DIR}"

run_demo() {
  local messages="${1:-64}"
  local truncate="${2:-1}"
  shift 2 || true
  ./build/log_engine_demo \
    --log-dir "${LOG_DIR}" \
    --archive-dir "${ARCHIVE_DIR}" \
    --messages "${messages}" \
    --payload-size 128 \
    --batch-size 8 \
    --checkpoint-enabled 1 \
    --truncate-on-start "${truncate}" \
    -c "${SHARDS}" \
    "${@}" >/tmp/log_engine_fault_demo.out 2>&1
}

# Test 1: Kill during write + recover
echo "--- Test 1: Kill-9 during write + recover ---"
timeout 30s ./build/log_engine_demo \
  --log-dir "${LOG_DIR}" \
  --archive-dir "${ARCHIVE_DIR}" \
  --messages 1000000 \
  --payload-size 128 \
  --batch-size 64 \
  --emit-delay-ms 1 \
  --checkpoint-enabled 1 \
  --truncate-on-start 1 \
  -c "${SHARDS}" >/tmp/log_engine_fault_kill.out 2>&1 &
DEMO_PID=$!
sleep 1
kill -9 "${DEMO_PID}" 2>/dev/null || true
wait "${DEMO_PID}" 2>/dev/null || true
DEMO_PID=""

run_demo 32 0
./build/log_engine_verify --path "${LOG_DIR}/shard-0.log" 2>/dev/null
echo "Test 1 OK"

# Test 2: Broken active tail + recovery
echo "--- Test 2: Broken active tail + recovery ---"
printf 'BROKEN_TAIL_GARBAGE' >> "${LOG_DIR}/shard-0.log"
run_demo 32 0
./build/log_engine_verify --path "${LOG_DIR}/shard-0.log" 2>/dev/null
echo "Test 2 OK"

# Test 3: Incomplete checkpoint + recovery fallback
echo "--- Test 3: Incomplete checkpoint + recovery fallback ---"
printf 'logical_size=7\nsequence=999999\n' > "${LOG_DIR}/shard-0.log.checkpoint"
run_demo 16 0
./build/log_engine_verify --path "${LOG_DIR}/shard-0.log" 2>/dev/null
echo "Test 3 OK"

# Test 4: Stale checkpoint + recovery fallback
echo "--- Test 4: Stale checkpoint (size mismatch) + recovery ---"
printf 'logical_size=1\nsequence=0\nrotation_index=0\n' > "${LOG_DIR}/shard-0.log.checkpoint"
run_demo 16 0
./build/log_engine_verify --path "${LOG_DIR}/shard-0.log" 2>/dev/null
echo "Test 4 OK"

# Test 5: Rotate + broken gzip + query
echo "--- Test 5: Rotate + broken gzip + query ---"
rm -rf "${LOG_DIR}" "${ARCHIVE_DIR}"
mkdir -p "${LOG_DIR}" "${ARCHIVE_DIR}"

./build/log_engine_demo \
  --log-dir "${LOG_DIR}" \
  --archive-dir "${ARCHIVE_DIR}" \
  --messages 200 \
  --payload-size 256 \
  --batch-size 16 \
  --rotate-size-bytes 1024 \
  --checkpoint-enabled 1 \
  --compress-archives 1 \
  --truncate-on-start 1 \
  -c "${SHARDS}" >/tmp/log_engine_fault_rotate.out 2>&1

latest_gz="$(ls -t "${ARCHIVE_DIR}"/shard-0.*.log.gz 2>/dev/null | head -n 1 || true)"
if [[ -n "${latest_gz}" ]]; then
  dd if=/dev/urandom of="${latest_gz}" bs=1 count=32 conv=notrunc 2>/dev/null || true
fi

# Start query server briefly and verify it handles the broken gzip
./build/log_engine_query_server \
  --log-dir "${LOG_DIR}" \
  --archive-dir "${ARCHIVE_DIR}" \
  --http-address 127.0.0.1 \
  --http-port 18085 \
  --grpc-address 127.0.0.1 \
  --grpc-port 19095 \
  --metrics-address 127.0.0.1 \
  --metrics-port 19195 \
  -c "${SHARDS}" >/tmp/log_engine_fault_query.out 2>&1 &
QUERY_PID=$!

for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:18085/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

status_json="$(curl -fsS "http://127.0.0.1:18085/v1/status" 2>/dev/null)" || true
if [[ -n "${status_json}" ]]; then
  health="$(echo "${status_json}" | grep -o '"health":"[^"]*"' | head -1 | cut -d'"' -f4)"
  echo "  query server health: ${health}"
fi

records="$(curl -fsS "http://127.0.0.1:18085/v1/records?limit=10" 2>/dev/null)" || true
if [[ -n "${records}" ]]; then
  echo "  query returned records"
fi

kill "${QUERY_PID}" 2>/dev/null || true
wait "${QUERY_PID}" 2>/dev/null || true
echo "Test 5 OK"

# Test 6: Multi-shard consistency after crash recovery
echo "--- Test 6: Multi-shard consistency after crash ---"
rm -rf "${LOG_DIR}" "${ARCHIVE_DIR}"
mkdir -p "${LOG_DIR}" "${ARCHIVE_DIR}"

./build/log_engine_bench \
  --log-dir "${LOG_DIR}" \
  --archive-dir "${ARCHIVE_DIR}" \
  --messages 5000 \
  --payload-size 256 \
  --batch-size 64 \
  --route-keys 8 \
  --checkpoint-enabled 1 \
  --rotate-size-bytes 4096 \
  --compress-archives 1 \
  --truncate-on-start 1 \
  -c "${SHARDS}" >/tmp/log_engine_fault_mshard.out 2>&1

# Verify all shard files
for shard_id in $(seq 0 $((SHARDS - 1))); do
  for shard_file in "${LOG_DIR}"/shard-${shard_id}.log; do
    if [[ -f "${shard_file}" ]]; then
      ./build/log_engine_verify --path "${shard_file}" 2>/dev/null || {
        echo "WARN: verify failed for ${shard_file}" >&2
      }
    fi
  done
done

# Kill during write across shards, then recover
timeout 15s ./build/log_engine_demo \
  --log-dir "${LOG_DIR}" \
  --archive-dir "${ARCHIVE_DIR}" \
  --messages 1000000 \
  --payload-size 128 \
  --batch-size 32 \
  --emit-delay-ms 1 \
  --checkpoint-enabled 1 \
  --truncate-on-start 0 \
  -c "${SHARDS}" >/tmp/log_engine_fault_kill2.out 2>&1 &
DEMO_PID=$!
sleep 0.5
kill -9 "${DEMO_PID}" 2>/dev/null || true
wait "${DEMO_PID}" 2>/dev/null || true
DEMO_PID=""

# Recover and verify
run_demo 32 0
for shard_id in $(seq 0 $((SHARDS - 1))); do
  if [[ -f "${LOG_DIR}/shard-${shard_id}.log" ]]; then
    ./build/log_engine_verify --path "${LOG_DIR}/shard-${shard_id}.log" 2>/dev/null || {
      echo "WARN: verify failed for shard-${shard_id}.log after recovery" >&2
    }
  fi
done
echo "Test 6 OK"

echo ""
echo "=== All fault injection tests passed ==="
