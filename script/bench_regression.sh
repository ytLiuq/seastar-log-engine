#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOC_DIR="${ROOT_DIR}/doc"

messages=100000
repeats=3
shards=1
output_prefix=""

usage() {
  cat <<'EOF'
Usage:
  ./script/bench_regression.sh [options]

Options:
  --messages <n>
  --repeats <n>
  --shards <n>
  --output-prefix <name>
EOF
}

extract_metric() {
  local line="$1"
  local key="$2"
  awk -v key="${key}" '
    {
      for (i = 1; i <= NF; ++i) {
        if ($i ~ ("^" key "=")) {
          sub("^" key "=", "", $i)
          print $i
          exit
        }
      }
    }
  ' <<<"${line}"
}

run_log_engine() {
  local payload_size="$1"
  local ack_mode="$2"
  local batch_size="$3"
  local inflight="$4"
  local raw_log="$5"

  rm -rf "${ROOT_DIR}/logs" "${ROOT_DIR}/archive"
  mkdir -p "${ROOT_DIR}/logs" "${ROOT_DIR}/archive"

  ./build/log_engine_bench \
    --ack-mode "${ack_mode}" \
    --log-dir "${ROOT_DIR}/logs" \
    --archive-dir "${ROOT_DIR}/archive" \
    --messages "${messages}" \
    --payload-size "${payload_size}" \
    --batch-size "${batch_size}" \
    --flush-ms 0 \
    --inflight "${inflight}" \
    --checkpoint-enabled 0 \
    --route-keys 0 \
    --rotate-size-bytes 0 \
    -c "${shards}" 2>&1 | tee "${raw_log}"
}

run_spdlog() {
  local payload_size="$1"
  local raw_log="$2"
  rm -rf "${ROOT_DIR}/logs-spdlog"
  mkdir -p "${ROOT_DIR}/logs-spdlog"
  ./build/spdlog_bench \
    --log-dir "${ROOT_DIR}/logs-spdlog" \
    --messages "${messages}" \
    --payload-size "${payload_size}" 2>&1 | tee "${raw_log}"
}

run_glog() {
  local payload_size="$1"
  local raw_log="$2"
  rm -rf "${ROOT_DIR}/logs-glog"
  mkdir -p "${ROOT_DIR}/logs-glog"
  ./build/glog_bench \
    --log-dir "${ROOT_DIR}/logs-glog" \
    --messages "${messages}" \
    --payload-size "${payload_size}" 2>&1 | tee "${raw_log}"
}

append_row() {
  local scenario="$1"
  local target="$2"
  local run_id="$3"
  local payload_size="$4"
  local ack_mode="$5"
  local batch_size="$6"
  local inflight="$7"
  local line="$8"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${scenario}" \
    "${target}" \
    "${run_id}" \
    "${messages}" \
    "${payload_size}" \
    "${ack_mode}" \
    "${batch_size}" \
    "${inflight}" \
    "${shards}" \
    "$(extract_metric "${line}" "elapsed_us")" \
    "$(extract_metric "${line}" "throughput_msg_per_sec")" \
    "$(extract_metric "${line}" "avg_submit_us")" \
    "$(extract_metric "${line}" "p50_submit_us")" \
    "$(extract_metric "${line}" "p95_submit_us")" \
    "$(extract_metric "${line}" "p99_submit_us")" >>"${tsv_path}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --messages) messages="$2"; shift 2 ;;
    --repeats) repeats="$2"; shift 2 ;;
    --shards) shards="$2"; shift 2 ;;
    --output-prefix) output_prefix="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

mkdir -p "${DOC_DIR}"
timestamp="$(date +%F-%H%M%S)"
prefix="${output_prefix:-regression-${timestamp}}"
tsv_path="${DOC_DIR}/${prefix}.tsv"
md_path="${DOC_DIR}/${prefix}.md"
raw_dir="${DOC_DIR}/${prefix}-raw"
mkdir -p "${raw_dir}"

printf 'scenario\ttarget\trun\tmessages\tpayload_size\tack_mode\tbatch_size\tinflight\tshards\telapsed_us\tthroughput_msg_per_sec\tavg_submit_us\tp50_submit_us\tp95_submit_us\tp99_submit_us\n' >"${tsv_path}"

log_engine_scenarios=(
  "baseline-128 write_ack 8192 1 128"
  "baseline-512 write_ack 8192 1 512"
  "baseline-2048 write_ack 8192 1 2048"
  "ack-write-2048 write_ack 8192 1 2048"
  "ack-sync-2048 sync_ack 8192 1 2048"
  "batch-2048-1024 write_ack 1024 1 2048"
  "batch-2048-8192 write_ack 8192 1 2048"
  "inflight-2048-4 write_ack 8192 4 2048"
)

compare_payloads=(128 512 2048)

for scenario_spec in "${log_engine_scenarios[@]}"; do
  read -r scenario ack_mode batch_size inflight payload_size <<<"${scenario_spec}"
  for run_id in $(seq 1 "${repeats}"); do
    raw_log="${raw_dir}/${scenario}-log_engine-run${run_id}.log"
    line="$(run_log_engine "${payload_size}" "${ack_mode}" "${batch_size}" "${inflight}" "${raw_log}" | grep -a 'messages=' | tail -n 1)"
    append_row "${scenario}" "log_engine" "${run_id}" "${payload_size}" "${ack_mode}" "${batch_size}" "${inflight}" "${line}"
  done
done

if [[ -x "${ROOT_DIR}/build/spdlog_bench" ]]; then
  for payload_size in "${compare_payloads[@]}"; do
    for run_id in $(seq 1 "${repeats}"); do
      raw_log="${raw_dir}/payload-${payload_size}-spdlog-run${run_id}.log"
      line="$(run_spdlog "${payload_size}" "${raw_log}" | grep -a 'messages=' | tail -n 1)"
      append_row "payload-${payload_size}" "spdlog" "${run_id}" "${payload_size}" "" "" "" "${line}"
    done
  done
fi

if [[ -x "${ROOT_DIR}/build/glog_bench" ]]; then
  for payload_size in "${compare_payloads[@]}"; do
    for run_id in $(seq 1 "${repeats}"); do
      raw_log="${raw_dir}/payload-${payload_size}-glog-run${run_id}.log"
      line="$(run_glog "${payload_size}" "${raw_log}" | grep -a 'messages=' | tail -n 1)"
      append_row "payload-${payload_size}" "glog" "${run_id}" "${payload_size}" "" "" "" "${line}"
    done
  done
fi

{
  echo "# Regression Benchmark $(date +%F)"
  echo
  echo "- messages_per_run: \`${messages}\`"
  echo "- repeats: \`${repeats}\`"
  echo "- shards: \`${shards}\`"
  echo
  echo "| Scenario | Target | Payload | Runs | Avg Throughput (msg/s) | Avg P95 (us) | Avg P99 (us) |"
  echo "| --- | --- | ---: | ---: | ---: | ---: | ---: |"
  awk -F'\t' '
    NR == 1 { next }
    {
      key = $1 SUBSEP $2
      payload[key] = $5
      count[key] += 1
      thr_sum[key] += $11 + 0
      p95_sum[key] += $14 + 0
      p99_sum[key] += $15 + 0
    }
    END {
      for (key in count) {
        split(key, parts, SUBSEP)
        printf "| `%s` | `%s` | %s | %d | %.2f | %.4f | %.4f |\n",
          parts[1], parts[2], payload[key], count[key], thr_sum[key] / count[key], p95_sum[key] / count[key], p99_sum[key] / count[key]
      }
    }
  ' "${tsv_path}" | sort
} >"${md_path}"

echo "Wrote regression results to ${tsv_path}"
echo "Wrote regression summary to ${md_path}"
