#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOC_DIR="${ROOT_DIR}/doc"
cd "${ROOT_DIR}"

messages=50000
payload_size=256
payload_sizes=""
batch_size=4096
inflight=16
shards=1
flush_ms=5
route_keys=0
repeats=1
output_prefix=""

usage() {
  cat <<'EOF'
Usage:
  ./script/bench_profiles.sh [options]

Options:
  --messages <n>
  --payload-size <n>
  --payload-sizes <csv>
  --batch-size <n>
  --inflight <n>
  --shards <n>
  --flush-ms <n>
  --route-keys <n>
  --repeats <n>
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

run_profile() {
  local scenario="$1"
  local scenario_payload_size="$2"
  shift
  shift
  local raw_log="${raw_dir}/${scenario}.log"

  rm -rf "${ROOT_DIR}/logs" "${ROOT_DIR}/archive"
  mkdir -p "${ROOT_DIR}/logs" "${ROOT_DIR}/archive"

  ./build/log_engine_bench \
    --log-dir "${ROOT_DIR}/logs" \
    --archive-dir "${ROOT_DIR}/archive" \
    --messages "${messages}" \
    --payload-size "${scenario_payload_size}" \
    --batch-size "${batch_size}" \
    --flush-ms "${flush_ms}" \
    --inflight "${inflight}" \
    --route-keys "${route_keys}" \
    "$@" \
    -c "${shards}" 2>&1 | tee "${raw_log}"
}

append_row() {
  local scenario="$1"
  local profile_group="$2"
  local run_id="$3"
  local row_payload_size="$4"
  local timestamp_enabled="$5"
  local crc_enabled="$6"
  local structured_enabled="$7"
  local checkpoint_enabled="$8"
  local rotate_enabled="$9"
  local line="${10}"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${scenario}" \
    "${profile_group}" \
    "${run_id}" \
    "${messages}" \
    "${row_payload_size}" \
    "${batch_size}" \
    "${inflight}" \
    "${timestamp_enabled}" \
    "${crc_enabled}" \
    "${structured_enabled}" \
    "${checkpoint_enabled}" \
    "${rotate_enabled}" \
    "$(extract_metric "${line}" "elapsed_us")" \
    "$(extract_metric "${line}" "throughput_msg_per_sec")" \
    "$(extract_metric "${line}" "avg_submit_us")" \
    "$(extract_metric "${line}" "p95_submit_us")" \
    "$(extract_metric "${line}" "p99_submit_us")" >>"${tsv_path}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --messages) messages="$2"; shift 2 ;;
    --payload-size) payload_size="$2"; shift 2 ;;
    --payload-sizes) payload_sizes="$2"; shift 2 ;;
    --batch-size) batch_size="$2"; shift 2 ;;
    --inflight) inflight="$2"; shift 2 ;;
    --shards) shards="$2"; shift 2 ;;
    --flush-ms) flush_ms="$2"; shift 2 ;;
    --route-keys) route_keys="$2"; shift 2 ;;
    --repeats) repeats="$2"; shift 2 ;;
    --output-prefix) output_prefix="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

mkdir -p "${DOC_DIR}"
timestamp="$(date +%F-%H%M%S)"
prefix="${output_prefix:-benchmark-profiles-${timestamp}}"
tsv_path="${DOC_DIR}/${prefix}.tsv"
md_path="${DOC_DIR}/${prefix}.md"
raw_dir="${DOC_DIR}/${prefix}-raw"
mkdir -p "${raw_dir}"

printf 'scenario\tprofile_group\trun\tmessages\tpayload_size\tbatch_size\tinflight\ttimestamp_enabled\tcrc_enabled\tstructured_enabled\tcheckpoint_enabled\trotate_enabled\telapsed_us\tthroughput_msg_per_sec\tavg_submit_us\tp95_submit_us\tp99_submit_us\n' >"${tsv_path}"

run_scenario() {
  local payload_value="$1"
  local scenario="$2"
  local profile_group="$3"
  local timestamp_enabled="$4"
  local crc_enabled="$5"
  local structured_enabled="$6"
  local checkpoint_enabled="$7"
  local rotate_enabled="$8"
  shift 8

  local run_id line
  for run_id in $(seq 1 "${repeats}"); do
    line="$(run_profile "${scenario}-p${payload_value}-run${run_id}" "${payload_value}" "$@" | grep -a 'messages=' | tail -n 1)"
    append_row "${scenario}" "${profile_group}" "${run_id}" "${payload_value}" "${timestamp_enabled}" "${crc_enabled}" "${structured_enabled}" "${checkpoint_enabled}" "${rotate_enabled}" "${line}"
  done
}

if [[ -n "${payload_sizes}" ]]; then
  IFS=',' read -r -a payload_matrix <<<"${payload_sizes}"
else
  payload_matrix=("${payload_size}")
fi

for current_payload_size in "${payload_matrix[@]}"; do
  run_scenario "${current_payload_size}" "record-baseline" "record_format" 0 0 0 0 0 \
    --checkpoint-enabled 0 \
    --rotate-size-bytes 0 \
    --record-timestamp-enabled 0 \
    --record-crc-enabled 0 \
    --record-level-enabled 0 \
    --record-shard-id-enabled 0 \
    --record-sequence-enabled 0

  run_scenario "${current_payload_size}" "record-timestamp-on" "record_format" 1 0 0 0 0 \
    --checkpoint-enabled 0 \
    --rotate-size-bytes 0 \
    --record-timestamp-enabled 1 \
    --record-crc-enabled 0 \
    --record-level-enabled 0 \
    --record-shard-id-enabled 0 \
    --record-sequence-enabled 0

  run_scenario "${current_payload_size}" "record-crc-on" "record_format" 0 1 0 0 0 \
    --checkpoint-enabled 0 \
    --rotate-size-bytes 0 \
    --record-timestamp-enabled 0 \
    --record-crc-enabled 1 \
    --record-level-enabled 0 \
    --record-shard-id-enabled 0 \
    --record-sequence-enabled 0

  run_scenario "${current_payload_size}" "record-structured-on" "record_format" 1 1 1 0 0 \
    --checkpoint-enabled 0 \
    --rotate-size-bytes 0 \
    --record-timestamp-enabled 1 \
    --record-crc-enabled 1 \
    --record-level-enabled 1 \
    --record-shard-id-enabled 1 \
    --record-sequence-enabled 1

  run_scenario "${current_payload_size}" "checkpoint-off" "checkpoint" 0 0 0 0 0 \
    --checkpoint-enabled 0 \
    --rotate-size-bytes 0 \
    --record-timestamp-enabled 0 \
    --record-crc-enabled 0 \
    --record-level-enabled 0 \
    --record-shard-id-enabled 0 \
    --record-sequence-enabled 0

  run_scenario "${current_payload_size}" "checkpoint-on" "checkpoint" 0 0 0 1 0 \
    --checkpoint-enabled 1 \
    --rotate-size-bytes 0 \
    --record-timestamp-enabled 0 \
    --record-crc-enabled 0 \
    --record-level-enabled 0 \
    --record-shard-id-enabled 0 \
    --record-sequence-enabled 0

  run_scenario "${current_payload_size}" "rotate-off" "rotate" 0 0 0 0 0 \
    --checkpoint-enabled 0 \
    --rotate-size-bytes 0 \
    --record-timestamp-enabled 0 \
    --record-crc-enabled 0 \
    --record-level-enabled 0 \
    --record-shard-id-enabled 0 \
    --record-sequence-enabled 0

  run_scenario "${current_payload_size}" "rotate-on" "rotate" 0 0 0 0 1 \
    --checkpoint-enabled 0 \
    --rotate-size-bytes 1048576 \
    --record-timestamp-enabled 0 \
    --record-crc-enabled 0 \
    --record-level-enabled 0 \
    --record-shard-id-enabled 0 \
    --record-sequence-enabled 0
done

{
  echo "# Benchmark Profiles $(date +%F)"
  echo
  echo "- messages: \`${messages}\`"
  if [[ -n "${payload_sizes}" ]]; then
    echo "- payload_sizes: \`${payload_sizes}\`"
  else
    echo "- payload_size: \`${payload_size}\`"
  fi
  echo "- batch_size: \`${batch_size}\`"
  echo "- inflight: \`${inflight}\`"
  echo "- shards: \`${shards}\`"
  echo "- flush_ms: \`${flush_ms}\`"
  echo "- route_keys: \`${route_keys}\`"
  echo "- repeats: \`${repeats}\`"
  echo
  echo "| Scenario | Group | Payload | Runs | Avg Throughput (msg/s) | Avg Submit (us) | Avg P95 (us) | Avg P99 (us) |"
  echo "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"
  awk -F'\t' '
    NR == 1 { next }
    {
      key = $1 SUBSEP $2 SUBSEP $5
      count[key] += 1
      thr_sum[key] += $14 + 0
      avg_sum[key] += $15 + 0
      p95_sum[key] += $16 + 0
      p99_sum[key] += $17 + 0
    }
    END {
      for (key in count) {
        split(key, parts, SUBSEP)
        printf "| `%s` | `%s` | %s | %d | %.2f | %.4f | %.4f | %.4f |\n",
          parts[1], parts[2], parts[3], count[key], thr_sum[key] / count[key], avg_sum[key] / count[key], p95_sum[key] / count[key], p99_sum[key] / count[key]
      }
    }
  ' "${tsv_path}" | sort
} >"${md_path}"

echo "Wrote benchmark profiles to ${tsv_path}"
echo "Wrote benchmark summary to ${md_path}"
