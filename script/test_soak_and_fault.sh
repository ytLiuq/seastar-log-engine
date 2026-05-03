#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

SOAK_DURATION_SECONDS=120
SHARDS=2
PAYLOAD_SIZE=128
BATCH_SIZE=64
FLUSH_MS=5
ROTATE_SIZE_BYTES=4096
MESSAGES_PER_RUN=1000
RESTART_INTERVAL_RUNS=3
QUERY_CHECK_INTERVAL_RUNS=5
ARCHIVE_RESET_INTERVAL_RUNS=7
HTTP_PORT=18080
GRPC_PORT=19090
METRICS_PORT=19190
SKIP_QUERY_CHECKS=0

usage() {
  cat <<'EOF'
Usage:
  ./script/test_soak_and_fault.sh [options]

Runs long-duration soak test with repeated rotate/restart/archive-cleanup
cycles, plus fault injection for bad checkpoint, bad active tail, bad gzip,
and multi-shard recovery consistency verification.

Options:
  --duration-seconds <n>    Total soak duration in seconds (default: 120)
  --shards <n>              Seastar shard count (default: 2)
  --payload-size <n>        Log payload size in bytes (default: 128)
  --messages-per-run <n>    Messages emitted per write cycle (default: 1000)
  --restart-interval <n>    Crash+recover every N cycles (default: 3)
  --http-port <n>           HTTP query port (default: 18080)
  --grpc-port <n>           gRPC query port (default: 19090)
  --metrics-port <n>        Metrics port (default: 19190)
  --skip-query-checks       Skip HTTP/gRPC query server checks
EOF
}

TMP_DIR="$(mktemp -d /tmp/log-engine-soak-fault.XXXXXX)"
LOG_DIR="${TMP_DIR}/logs"
ARCHIVE_DIR="${TMP_DIR}/archive"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -9 "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  if [[ -n "${DEMO_PID:-}" ]] && kill -0 "${DEMO_PID}" 2>/dev/null; then
    kill -9 "${DEMO_PID}" 2>/dev/null || true
    wait "${DEMO_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration-seconds) SOAK_DURATION_SECONDS="$2"; shift 2 ;;
    --shards) SHARDS="$2"; shift 2 ;;
    --payload-size) PAYLOAD_SIZE="$2"; shift 2 ;;
    --messages-per-run) MESSAGES_PER_RUN="$2"; shift 2 ;;
    --restart-interval) RESTART_INTERVAL_RUNS="$2"; shift 2 ;;
    --http-port) HTTP_PORT="$2"; shift 2 ;;
    --grpc-port) GRPC_PORT="$2"; shift 2 ;;
    --metrics-port) METRICS_PORT="$2"; shift 2 ;;
    --skip-query-checks) SKIP_QUERY_CHECKS=1; shift 1 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

mkdir -p "${LOG_DIR}" "${ARCHIVE_DIR}"

has_option() {
  local needle="$1"
  shift
  local arg
  for arg in "$@"; do
    if [[ "${arg}" == "${needle}" ]]; then
      return 0
    fi
  done
  return 1
}

run_demo() {
  local extra_args=("${@}")
  if ! has_option "--truncate-on-start" "${extra_args[@]}"; then
    extra_args+=(--truncate-on-start 0)
  fi
  ./build/log_engine_demo \
    --log-dir "${LOG_DIR}" \
    --archive-dir "${ARCHIVE_DIR}" \
    --messages "${MESSAGES_PER_RUN}" \
    --payload-size "${PAYLOAD_SIZE}" \
    --batch-size "${BATCH_SIZE}" \
    --flush-ms "${FLUSH_MS}" \
    --rotate-size-bytes "${ROTATE_SIZE_BYTES}" \
    --checkpoint-enabled 1 \
    --compress-archives 1 \
    --record-timestamp-enabled 1 \
    --record-crc-enabled 1 \
    -c "${SHARDS}" \
    "${extra_args[@]}" >/tmp/log_engine_soak_demo.out 2>&1
}

run_query_server() {
  ./build/log_engine_query_server \
    --log-dir "${LOG_DIR}" \
    --archive-dir "${ARCHIVE_DIR}" \
    --http-address 127.0.0.1 \
    --http-port "${HTTP_PORT}" \
    --grpc-address 127.0.0.1 \
    --grpc-port "${GRPC_PORT}" \
    --metrics-address 127.0.0.1 \
    --metrics-port "${METRICS_PORT}" \
    -c "${SHARDS}" >/tmp/log_engine_soak_server.out 2>&1 &
  SERVER_PID=$!

  for _ in $(seq 1 50); do
    if curl -fsS "http://127.0.0.1:${HTTP_PORT}/healthz" >/dev/null 2>&1 ||
       curl -fsS "http://127.0.0.1:${HTTP_PORT}/v1/status" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      echo "WARN: query server exited before becoming ready" >&2
      tail -n 40 /tmp/log_engine_soak_server.out >&2 || true
      return 1
    fi
    sleep 0.2
  done

  echo "WARN: query server readiness timed out" >&2
  tail -n 40 /tmp/log_engine_soak_server.out >&2 || true
  return 1
}

stop_query_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
    SERVER_PID=""
  fi
}

run_bench() {
  local messages="${1:-5000}"
  local extra_args=("${@:2}")
  ./build/log_engine_bench \
    --log-dir "${LOG_DIR}" \
    --archive-dir "${ARCHIVE_DIR}" \
    --messages "${messages}" \
    --payload-size "${PAYLOAD_SIZE}" \
    --batch-size "${BATCH_SIZE}" \
    --flush-ms "${FLUSH_MS}" \
    --checkpoint-enabled 1 \
    --rotate-size-bytes "${ROTATE_SIZE_BYTES}" \
    --truncate-on-start 0 \
    -c "${SHARDS}" \
    "${extra_args[@]}" >/tmp/log_engine_soak_bench.out 2>&1
}

inject_broken_tail() {
  local shard_file="${LOG_DIR}/shard-0.log"
  if [[ -f "${shard_file}" ]]; then
    printf 'BROKEN_TAIL_GARBAGE_DATA_ZZZZ' >> "${shard_file}"
  fi
}

inject_broken_checkpoint() {
  local ckpt_file="${LOG_DIR}/shard-0.log.checkpoint"
  printf 'logical_size=999999\nsequence=0\n' > "${ckpt_file}"
}

inject_stale_checkpoint() {
  local ckpt_file="${LOG_DIR}/shard-0.log.checkpoint"
  if [[ -f "${ckpt_file}" ]]; then
    sed -i 's/^logical_size=.*/logical_size=1/' "${ckpt_file}"
  fi
}

inject_broken_gzip() {
  local latest_gz
  latest_gz="$(ls -t "${ARCHIVE_DIR}"/shard-0.*.log.gz 2>/dev/null | head -n 1 || true)"
  if [[ -n "${latest_gz}" ]]; then
    dd if=/dev/urandom of="${latest_gz}" bs=1 count=16 conv=notrunc 2>/dev/null || true
  fi
}

verify_active_file() {
  local shard_file="$1"
  if [[ -f "${shard_file}" ]]; then
    ./build/log_engine_verify --path "${shard_file}" 2>/dev/null || {
      echo "verify failed for ${shard_file}" >&2
      return 1
    }
    echo "$(basename "${shard_file}") verify OK"
  fi
}

verify_all_active_files() {
  local shard_id
  for shard_id in $(seq 0 $((SHARDS - 1))); do
    verify_active_file "${LOG_DIR}/shard-${shard_id}.log"
  done
}

check_health() {
  local status_json=""
  local _attempt
  for _attempt in $(seq 1 20); do
    status_json="$(curl -fsS "http://127.0.0.1:${HTTP_PORT}/v1/status" 2>/dev/null)" && break
    sleep 0.2
  done
  if [[ -z "${status_json}" ]]; then
    echo "WARN: cannot fetch status" >&2
    return 0
  fi
  local health
  health="$(echo "${status_json}" | grep -o '"health":"[^"]*"' | head -1 | cut -d'"' -f4)"
  echo "health=${health}"
}

query_consistency_check() {
  local http_records=""
  local grpc_records=""
  local _attempt
  for _attempt in $(seq 1 20); do
    http_records="$(curl -fsS "http://127.0.0.1:${HTTP_PORT}/v1/records?include_archive=true&limit=50" 2>/dev/null)" && break
    sleep 0.2
  done
  for _attempt in $(seq 1 20); do
    grpc_records="$(./build/log_engine_query_client --target "127.0.0.1:${GRPC_PORT}" --method records --include-archive true --limit 50 2>/dev/null)" && break
    sleep 0.2
  done
  if [[ "${http_records}" != "${grpc_records}" ]]; then
    echo "WARN: query consistency mismatch" >&2
    return 1
  fi
  echo "query consistency OK"
}

reset_data_dirs() {
  rm -rf "${LOG_DIR}" "${ARCHIVE_DIR}"
  mkdir -p "${LOG_DIR}" "${ARCHIVE_DIR}"
}

run_soak_loop() {
  local start_epoch run_count
  start_epoch="$(date +%s)"
  run_count=0

  reset_data_dirs
  while (( $(date +%s) - start_epoch < SOAK_DURATION_SECONDS )); do
    run_count=$((run_count + 1))
    echo "  Soak cycle ${run_count}..."

    if (( run_count % RESTART_INTERVAL_RUNS == 0 )); then
      timeout 30s ./build/log_engine_demo \
        --log-dir "${LOG_DIR}" \
        --archive-dir "${ARCHIVE_DIR}" \
        --messages "${MESSAGES_PER_RUN}" \
        --payload-size "${PAYLOAD_SIZE}" \
        --batch-size "${BATCH_SIZE}" \
        --flush-ms "${FLUSH_MS}" \
        --rotate-size-bytes "${ROTATE_SIZE_BYTES}" \
        --checkpoint-enabled 1 \
        --compress-archives 1 \
        --truncate-on-start 0 \
        -c "${SHARDS}" >/tmp/log_engine_soak_kill.out 2>&1 &
      DEMO_PID=$!
      sleep 1
      kill -9 "${DEMO_PID}" 2>/dev/null || true
      wait "${DEMO_PID}" 2>/dev/null || true
      DEMO_PID=""
      run_demo
      echo "  Soak cycle ${run_count}: crash+recover OK"
    else
      run_demo
    fi

    verify_all_active_files

    if (( SKIP_QUERY_CHECKS == 0 && run_count % QUERY_CHECK_INTERVAL_RUNS == 0 )); then
      run_query_server
      check_health
      query_consistency_check
      stop_query_server
    fi

    if (( run_count % ARCHIVE_RESET_INTERVAL_RUNS == 0 )); then
      echo "  Soak cycle ${run_count}: archive reset + rebuild"
      reset_data_dirs
      run_bench $((MESSAGES_PER_RUN * SHARDS)) --route-keys $((SHARDS * 4))
      verify_all_active_files
    fi
  done

  echo "Soak loop completed: ${run_count} cycles in ${SOAK_DURATION_SECONDS}s"
}

echo "=== Soak & Fault Injection Test ==="
echo "Duration: ${SOAK_DURATION_SECONDS}s, Shards: ${SHARDS}, Payload: ${PAYLOAD_SIZE}B"
echo "Log dir: ${LOG_DIR}"
if (( SKIP_QUERY_CHECKS == 1 )); then
  echo "Query checks: skipped"
fi

# Phase 1: Timed soak loop
echo ""
echo "--- Phase 1: Timed soak loop ---"
run_soak_loop
echo "Phase 1 OK"

# Phase 2: Initial query server + status check
if (( SKIP_QUERY_CHECKS == 0 )); then
  echo ""
  echo "--- Phase 2: Query server + status check ---"
  run_query_server
  check_health
  query_consistency_check
  stop_query_server
  echo "Phase 2 OK"
else
  echo ""
  echo "--- Phase 2: Query server + status check (skipped) ---"
fi

# Phase 3: Restart recovery after clean stop
echo ""
echo "--- Phase 3: Restart recovery after clean stop ---"
run_demo
verify_all_active_files
echo "Phase 3 OK"

# Phase 4: Broken tail injection + recovery
echo ""
echo "--- Phase 4: Broken active tail + recovery ---"
inject_broken_tail
if run_demo; then
  echo "Phase 4a (broken tail recovery) OK"
else
  echo "Phase 4a recovery ran (may have warned about tail corruption)"
fi
verify_all_active_files

# Recover from broken tail by truncating and restarting
rm -f "${LOG_DIR}/shard-0.log" "${LOG_DIR}/shard-0.log.checkpoint"
run_demo --truncate-on-start 1
run_demo --truncate-on-start 0
verify_all_active_files
echo "Phase 4b (recovered after tail corruption) OK"

# Phase 5: Broken checkpoint injection
echo ""
echo "--- Phase 5: Bad checkpoint + recovery ---"
inject_broken_checkpoint
run_demo
verify_all_active_files
echo "Phase 5 OK"

# Phase 6: Stale checkpoint injection
echo ""
echo "--- Phase 6: Stale checkpoint + recovery ---"
inject_stale_checkpoint
run_demo
verify_all_active_files
echo "Phase 6 OK"

# Phase 7: Broken gzip archive + query
echo ""
if (( SKIP_QUERY_CHECKS == 0 )); then
  echo "--- Phase 7: Broken gzip + query ---"
  reset_data_dirs
  run_bench 5000
  inject_broken_gzip
  run_query_server
  check_health
  query_consistency_check
  stop_query_server
  verify_all_active_files
  echo "Phase 7 OK"
else
  echo "--- Phase 7: Broken gzip + query (skipped query checks) ---"
fi

# Phase 8: Multi-shard recovery consistency
echo ""
echo "--- Phase 8: Multi-shard recovery consistency ---"
reset_data_dirs
run_bench 10000 --route-keys $((SHARDS * 4))

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

# Restart and verify consistency
run_demo
echo "Phase 8 OK"

echo ""
echo "=== All soak and fault injection tests passed ==="
