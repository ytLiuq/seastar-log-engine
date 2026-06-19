#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

MESSAGES=20000
PAYLOAD_SIZE=128
SHARDS="1,2,4"
BATCH_SIZES="100,500"
DISPATCHERS="1,4"
SINK_DELAYS_MS="0,10,50"
QUEUE_CAPACITY=64
SINK_FAIL_FIRST=0
REPEATS=1
OUTPUT_PREFIX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --messages) MESSAGES="$2"; shift 2 ;;
    --payload-size) PAYLOAD_SIZE="$2"; shift 2 ;;
    --shards) SHARDS="$2"; shift 2 ;;
    --batch-sizes) BATCH_SIZES="$2"; shift 2 ;;
    --dispatchers) DISPATCHERS="$2"; shift 2 ;;
    --sink-delays-ms) SINK_DELAYS_MS="$2"; shift 2 ;;
    --queue-capacity) QUEUE_CAPACITY="$2"; shift 2 ;;
    --sink-fail-first) SINK_FAIL_FIRST="$2"; shift 2 ;;
    --repeats) REPEATS="$2"; shift 2 ;;
    --output-prefix) OUTPUT_PREFIX="$2"; shift 2 ;;
    *)
      echo "unknown option: $1" >&2
      exit 1
      ;;
  esac
done

timestamp="$(date +%F-%H%M%S)"
prefix="${OUTPUT_PREFIX:-delivery-scanner-${timestamp}}"
tsv_path="${ROOT_DIR}/doc/${prefix}.tsv"
md_path="${ROOT_DIR}/doc/${prefix}.md"

printf 'run\tshards\tmessages\tpayload_size\tbatch_size\tdispatchers\tqueue_capacity\tsink_delay_ms\tsink_fail_first\telapsed_ms\tthroughput_msg_per_sec\tpeak_backlog\tpeak_queue_batches\tpeak_active_workers\tstatus\n' >"${tsv_path}"

wait_http() {
  local url="$1"
  for _ in $(seq 1 200); do
    if python3 - "${url}" <<'PY' >/dev/null 2>&1
import sys
from urllib.request import urlopen
with urlopen(sys.argv[1], timeout=1) as response:
    raise SystemExit(0 if 200 <= response.status < 300 else 1)
PY
    then
      return 0
    fi
    sleep 0.05
  done
  return 1
}

run_case() (
  local run_id="$1"
  local shards="$2"
  local batch_size="$3"
  local dispatchers="$4"
  local sink_delay_ms="$5"
  local tmp_dir
  tmp_dir="$(mktemp -d /tmp/seastar-delivery-bench.XXXXXX)"
  local source_file="${tmp_dir}/source.log"
  local sink_out="${tmp_dir}/sink.ndjson"
  local sink_port=$((20000 + RANDOM % 10000))
  local agent_port=$((30000 + RANDOM % 10000))
  local sink_pid=""
  local agent_pid=""

  cleanup_case() {
    if [[ -n "${agent_pid}" ]] && kill -0 "${agent_pid}" 2>/dev/null; then
      kill "${agent_pid}" 2>/dev/null || true
      wait "${agent_pid}" 2>/dev/null || true
    fi
    if [[ -n "${sink_pid}" ]] && kill -0 "${sink_pid}" 2>/dev/null; then
      kill "${sink_pid}" 2>/dev/null || true
      wait "${sink_pid}" 2>/dev/null || true
    fi
    rm -rf "${tmp_dir}"
  }
  trap cleanup_case EXIT

  : >"${source_file}"
  python3 script/fake_http_sink.py \
    --host 127.0.0.1 \
    --port "${sink_port}" \
    --out "${sink_out}" \
    --fail-first "${SINK_FAIL_FIRST}" \
    --delay-ms "${sink_delay_ms}" &
  sink_pid=$!
  wait_http "http://127.0.0.1:${sink_port}/healthz"

  ./build/log_engine_agent \
    --config config/agent.conf \
    --log-dir "${tmp_dir}/logs" \
    --archive-dir "${tmp_dir}/archive" \
    --http-ingest-address 127.0.0.1 \
    --http-ingest-port "${agent_port}" \
    --file-source-path "${source_file}" \
    --source-offset-path "${tmp_dir}/source.offset" \
    --source-poll-ms 10 \
    --source-max-lines 4096 \
    --sink-kind http \
    --sink-http-url "http://127.0.0.1:${sink_port}/ingest" \
    --sink-batch-size "${batch_size}" \
    --sink-dispatcher-concurrency "${dispatchers}" \
    --sink-dispatch-queue-capacity "${QUEUE_CAPACITY}" \
    --delivery-scan-idle-ms 1 \
    --delivery-offset-path "${tmp_dir}/delivery.offset" \
    --pending-delivery-path "${tmp_dir}/delivery.pending" \
    --sink-retry-backoff-ms 10 \
    --sink-retry-max-backoff-ms 100 \
    -c "${shards}" >"${tmp_dir}/agent.out" 2>&1 &
  agent_pid=$!
  wait_http "http://127.0.0.1:${agent_port}/healthz"

  local start_ms
  start_ms="$(date +%s%3N)"
  python3 - "${source_file}" "${MESSAGES}" "${PAYLOAD_SIZE}" <<'PY'
import sys
path, messages, payload_size = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
payload = "x" * payload_size
with open(path, "a", encoding="utf-8") as out:
    for index in range(messages):
        out.write(f"{index}:{payload}\n")
PY

  local peak_backlog=0
  local peak_queue_batches=0
  local peak_active_workers=0
  local status="timeout"
  local elapsed_ms=0
  for _ in $(seq 1 1200); do
    local snapshot
    snapshot="$(python3 - "http://127.0.0.1:${agent_port}/v1/status" <<'PY' 2>/dev/null || true
import json
import sys
from urllib.request import urlopen
with urlopen(sys.argv[1], timeout=1) as response:
    data = json.load(response)
print(
    data.get("source_committed", 0),
    data.get("sink_sent", 0),
    data.get("sink_backlog_records", 0),
    data.get("sink_dispatch_queue_batches", 0),
    data.get("sink_dispatch_active_workers", 0),
)
PY
)"
    if [[ -n "${snapshot}" ]]; then
      read -r committed sent backlog queue_batches active_workers <<<"${snapshot}"
      if (( backlog > peak_backlog )); then
        peak_backlog="${backlog}"
      fi
      if (( queue_batches > peak_queue_batches )); then
        peak_queue_batches="${queue_batches}"
      fi
      if (( active_workers > peak_active_workers )); then
        peak_active_workers="${active_workers}"
      fi
      if (( committed >= MESSAGES && sent >= MESSAGES )); then
        elapsed_ms=$(( $(date +%s%3N) - start_ms ))
        status="completed"
        break
      fi
    fi
    sleep 0.05
  done
  if [[ "${status}" != "completed" ]]; then
    elapsed_ms=$(( $(date +%s%3N) - start_ms ))
  fi

  local throughput
  throughput="$(awk -v messages="${MESSAGES}" -v elapsed="${elapsed_ms}" 'BEGIN { if (elapsed == 0) print 0; else printf "%.2f", messages * 1000 / elapsed }')"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${run_id}" "${shards}" "${MESSAGES}" "${PAYLOAD_SIZE}" "${batch_size}" \
    "${dispatchers}" "${QUEUE_CAPACITY}" "${sink_delay_ms}" "${SINK_FAIL_FIRST}" "${elapsed_ms}" \
    "${throughput}" "${peak_backlog}" "${peak_queue_batches}" "${peak_active_workers}" \
    "${status}" >>"${tsv_path}"
)

IFS=',' read -r -a shard_values <<<"${SHARDS}"
IFS=',' read -r -a batch_values <<<"${BATCH_SIZES}"
IFS=',' read -r -a dispatcher_values <<<"${DISPATCHERS}"
IFS=',' read -r -a delay_values <<<"${SINK_DELAYS_MS}"

for run_id in $(seq 1 "${REPEATS}"); do
  for shards in "${shard_values[@]}"; do
    for batch_size in "${batch_values[@]}"; do
      for dispatchers in "${dispatcher_values[@]}"; do
        for sink_delay in "${delay_values[@]}"; do
          echo "run=${run_id} shards=${shards} batch=${batch_size} dispatchers=${dispatchers} sink_delay_ms=${sink_delay}"
          run_case "${run_id}" "${shards}" "${batch_size}" "${dispatchers}" "${sink_delay}"
        done
      done
    done
  done
done

{
  echo "# Delivery Scanner Benchmark ${timestamp}"
  echo
  echo '```tsv'
  cat "${tsv_path}"
  echo '```'
} >"${md_path}"

echo "wrote ${tsv_path}"
echo "wrote ${md_path}"
