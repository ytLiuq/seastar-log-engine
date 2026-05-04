#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOC_DIR="${ROOT_DIR}/doc"

target="log_engine"
duration_seconds=60
messages=50000
payload_size=2048
batch_size=8192
inflight=1
shards=1
ack_mode="write_ack"
flush_ms=0
checkpoint_enabled=0
output_prefix=""

usage() {
  cat <<'EOF'
Usage:
  ./script/bench_soak.sh [options]

Options:
  --target <log_engine|spdlog|glog>
  --duration-seconds <n>
  --messages <n>
  --payload-size <n>
  --batch-size <n>
  --inflight <n>
  --shards <n>
  --ack-mode <write_ack|sync_ack>
  --flush-ms <n>
  --checkpoint-enabled <0|1>
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

cleanup_target_dirs() {
  case "${target}" in
    log_engine)
      rm -rf "${ROOT_DIR}/logs" "${ROOT_DIR}/archive"
      mkdir -p "${ROOT_DIR}/logs" "${ROOT_DIR}/archive"
      ;;
    spdlog)
      rm -rf "${ROOT_DIR}/logs-spdlog"
      mkdir -p "${ROOT_DIR}/logs-spdlog"
      ;;
    glog)
      rm -rf "${ROOT_DIR}/logs-glog"
      mkdir -p "${ROOT_DIR}/logs-glog"
      ;;
    *)
      echo "unsupported target: ${target}" >&2
      exit 1
      ;;
  esac
}

run_once() {
  cleanup_target_dirs
  local raw_log="$1"

  case "${target}" in
    log_engine)
      ./build/log_engine_bench \
        --ack-mode "${ack_mode}" \
        --log-dir "${ROOT_DIR}/logs" \
        --archive-dir "${ROOT_DIR}/archive" \
        --messages "${messages}" \
        --payload-size "${payload_size}" \
        --batch-size "${batch_size}" \
        --flush-ms "${flush_ms}" \
        --inflight "${inflight}" \
        --checkpoint-enabled "${checkpoint_enabled}" \
        --route-keys 0 \
        --rotate-size-bytes 0 \
        -c "${shards}" 2>&1 | tee "${raw_log}"
      ;;
    spdlog)
      if [[ ! -x "${ROOT_DIR}/build/spdlog_bench" ]]; then
        echo "spdlog_bench not built" >&2
        exit 1
      fi
      ./build/spdlog_bench \
        --log-dir "${ROOT_DIR}/logs-spdlog" \
        --messages "${messages}" \
        --payload-size "${payload_size}" 2>&1 | tee "${raw_log}"
      ;;
    glog)
      if [[ ! -x "${ROOT_DIR}/build/glog_bench" ]]; then
        echo "glog_bench not built" >&2
        exit 1
      fi
      ./build/glog_bench \
        --log-dir "${ROOT_DIR}/logs-glog" \
        --messages "${messages}" \
        --payload-size "${payload_size}" 2>&1 | tee "${raw_log}"
      ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) target="$2"; shift 2 ;;
    --duration-seconds) duration_seconds="$2"; shift 2 ;;
    --messages) messages="$2"; shift 2 ;;
    --payload-size) payload_size="$2"; shift 2 ;;
    --batch-size) batch_size="$2"; shift 2 ;;
    --inflight) inflight="$2"; shift 2 ;;
    --shards) shards="$2"; shift 2 ;;
    --ack-mode) ack_mode="$2"; shift 2 ;;
    --flush-ms) flush_ms="$2"; shift 2 ;;
    --checkpoint-enabled) checkpoint_enabled="$2"; shift 2 ;;
    --output-prefix) output_prefix="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

mkdir -p "${DOC_DIR}"
timestamp="$(date +%F-%H%M%S)"
prefix="${output_prefix:-soak-${target}-${payload_size}b-${timestamp}}"
tsv_path="${DOC_DIR}/${prefix}.tsv"
md_path="${DOC_DIR}/${prefix}.md"
raw_dir="${DOC_DIR}/${prefix}-raw"
mkdir -p "${raw_dir}"

printf 'iteration\ttarget\tack_mode\tmessages\tpayload_size\tbatch_size\tinflight\tshards\tflush_ms\tcheckpoint_enabled\telapsed_us\tthroughput_msg_per_sec\tavg_submit_us\tp50_submit_us\tp95_submit_us\tp99_submit_us\n' >"${tsv_path}"

start_epoch="$(date +%s)"
iteration=0
while (( "$(date +%s)" - start_epoch < duration_seconds )); do
  iteration=$((iteration + 1))
  raw_log="${raw_dir}/run-${iteration}.log"
  line="$(run_once "${raw_log}" | grep -a 'messages=' | tail -n 1)"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${iteration}" \
    "${target}" \
    "${ack_mode}" \
    "${messages}" \
    "${payload_size}" \
    "${batch_size}" \
    "${inflight}" \
    "${shards}" \
    "${flush_ms}" \
    "${checkpoint_enabled}" \
    "$(extract_metric "${line}" "elapsed_us")" \
    "$(extract_metric "${line}" "throughput_msg_per_sec")" \
    "$(extract_metric "${line}" "avg_submit_us")" \
    "$(extract_metric "${line}" "p50_submit_us")" \
    "$(extract_metric "${line}" "p95_submit_us")" \
    "$(extract_metric "${line}" "p99_submit_us")" >>"${tsv_path}"
done

awk -F'\t' -v target="${target}" -v ack_mode="${ack_mode}" -v messages="${messages}" -v payload_size="${payload_size}" -v batch_size="${batch_size}" -v inflight="${inflight}" -v shards="${shards}" -v duration_seconds="${duration_seconds}" '
  NR == 1 { next }
  {
    runs += 1
    thr = $12 + 0
    avg = $13 + 0
    p95 = $15 + 0
    p99 = $16 + 0
    thr_sum += thr
    avg_sum += avg
    p95_sum += p95
    p99_sum += p99
    if (runs == 1 || thr < thr_min) thr_min = thr
    if (runs == 1 || thr > thr_max) thr_max = thr
    if (runs == 1 || p99 > p99_max) p99_max = p99
  }
  END {
    printf "# Soak Benchmark %s\n\n", strftime("%Y-%m-%d")
    printf "- target: `%s`\n", target
    printf "- ack_mode: `%s`\n", ack_mode
    printf "- duration_seconds: `%s`\n", duration_seconds
    printf "- messages_per_run: `%s`\n", messages
    printf "- payload_size: `%s`\n", payload_size
    printf "- batch_size: `%s`\n", batch_size
    printf "- inflight: `%s`\n", inflight
    printf "- shards: `%s`\n\n", shards
    printf "| Runs | Avg Throughput (msg/s) | Min Throughput | Max Throughput | Avg Submit (us) | Avg P95 (us) | Avg P99 (us) | Max P99 (us) |\n"
    printf "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n"
    if (runs == 0) {
      printf "| 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |\n"
    } else {
      printf "| %d | %.2f | %.2f | %.2f | %.4f | %.4f | %.4f | %.4f |\n",
        runs, thr_sum / runs, thr_min, thr_max, avg_sum / runs, p95_sum / runs, p99_sum / runs, p99_max
    }
  }
' "${tsv_path}" >"${md_path}"

echo "Wrote soak results to ${tsv_path}"
echo "Wrote soak summary to ${md_path}"
