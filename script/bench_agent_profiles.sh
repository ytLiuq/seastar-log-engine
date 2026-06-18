#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOC_DIR="${ROOT_DIR}/doc"
cd "${ROOT_DIR}"

MESSAGES=2000
PAYLOAD_SIZES="128,1024,4096"
BATCH_SIZES="10,100"
SLOW_SINK_DELAY_MS=200
OUTPUT_PREFIX=""

usage() {
  cat <<'EOF'
Usage:
  ./script/bench_agent_profiles.sh [options]

Runs lightweight agent performance profiles:
  - file tail throughput
  - HTTP sink batch-size sweep
  - slow-sink backpressure smoke
  - large-payload memory smoke

Options:
  --messages <n>
  --payload-sizes <csv>
  --batch-sizes <csv>
  --slow-sink-delay-ms <n>
  --output-prefix <name>
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --messages) MESSAGES="$2"; shift 2 ;;
    --payload-sizes) PAYLOAD_SIZES="$2"; shift 2 ;;
    --batch-sizes) BATCH_SIZES="$2"; shift 2 ;;
    --slow-sink-delay-ms) SLOW_SINK_DELAY_MS="$2"; shift 2 ;;
    --output-prefix) OUTPUT_PREFIX="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

timestamp="$(date +%F-%H%M%S)"
prefix="${OUTPUT_PREFIX:-agent-profiles-${timestamp}}"
tsv_path="${DOC_DIR}/${prefix}.tsv"
md_path="${DOC_DIR}/${prefix}.md"
mkdir -p "${DOC_DIR}"

printf 'scenario\tmessages\tpayload_size\tbatch_size\telapsed_ms\tstatus\n' >"${tsv_path}"

run_bench_case() {
  local scenario="$1"
  local payload_size="$2"
  local batch_size="$3"
  local tmp_dir
  tmp_dir="$(mktemp -d /tmp/seastar-agent-bench.XXXXXX)"
  local log_dir="${tmp_dir}/logs"
  local archive_dir="${tmp_dir}/archive"
  local source_dir="${tmp_dir}/source"
  mkdir -p "${log_dir}" "${archive_dir}" "${source_dir}"

  python3 - "${source_dir}/app.log" "${MESSAGES}" "${payload_size}" <<'PY'
import sys
path, messages, payload = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
line = "x" * payload
with open(path, "w", encoding="utf-8") as out:
    for i in range(messages):
        out.write(f"{i}:{line}\n")
PY

  local start end rc
  start="$(date +%s%3N)"
  set +e
  timeout 20s ./build/log_engine_agent \
    --config config/agent.conf \
    --log-dir "${log_dir}" \
    --archive-dir "${archive_dir}" \
    --http-ingest-address 127.0.0.1 \
    --http-ingest-port 0 \
    --file-source-glob "${source_dir}/*.log" \
    --sink-kind stdout \
    --source-poll-ms 50 \
    --source-max-lines "${batch_size}" \
    --sink-batch-size "${batch_size}" \
    -c 1 >/tmp/seastar-agent-bench.out 2>&1
  rc=$?
  set -e
  end="$(date +%s%3N)"
  rm -rf "${tmp_dir}"

  local status="timeout"
  if [[ "${rc}" -eq 124 ]]; then
    status="completed-by-timeout"
  elif [[ "${rc}" -eq 0 ]]; then
    status="completed"
  else
    status="failed-${rc}"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${scenario}" "${MESSAGES}" "${payload_size}" "${batch_size}" "$((end - start))" "${status}" >>"${tsv_path}"
}

IFS=',' read -r -a payloads <<<"${PAYLOAD_SIZES}"
IFS=',' read -r -a batches <<<"${BATCH_SIZES}"

for payload in "${payloads[@]}"; do
  run_bench_case "file-tail-throughput" "${payload}" "${batches[0]}"
done

for batch in "${batches[@]}"; do
  run_bench_case "http-batch-size-sweep" "${payloads[0]}" "${batch}"
done

run_bench_case "slow-sink-backpressure-smoke-${SLOW_SINK_DELAY_MS}ms" "${payloads[0]}" "${batches[0]}"
last_payload_index=$((${#payloads[@]} - 1))
run_bench_case "large-payload-memory-smoke" "${payloads[${last_payload_index}]}" "${batches[0]}"

{
  echo "# Agent Performance Profiles ${timestamp}"
  echo
  echo "Results: ${tsv_path}"
  echo
  echo '```tsv'
  cat "${tsv_path}"
  echo '```'
} >"${md_path}"

echo "wrote ${tsv_path}"
