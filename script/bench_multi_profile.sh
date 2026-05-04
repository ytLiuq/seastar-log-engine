#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

MESSAGES=50000
BATCH_SIZE=512
FLUSH_MS=1
INFLIGHT=16
SHARDS="1,2,4"
ACK_MODES="write_ack,sync_ack"
PAYLOADS="128,512,2048"
ROUTE_KEYS="0,4,16"
MAX_PENDING_VALUES="0,131072,524288"
SUBMIT_GROUP_SIZES="1,16"

usage() {
  cat <<'EOF'
Usage:
  ./script/bench_multi_profile.sh [options]

Runs multi-dimensional benchmark profiling across shard count, ack mode,
payload size, route key distribution, and backpressure settings.

Options:
  --messages <n>         Messages per run (default: 50000)
  --batch-size <n>       Batch size (default: 512)
  --shards <csv>         Shard counts (default: 1,2,4)
  --ack-modes <csv>      Ack modes (default: write_ack,sync_ack)
  --payloads <csv>       Payload sizes (default: 128,512,2048)
  --route-keys <csv>     Route-key counts (default: 0,4,16)
  --max-pending <csv>    Backpressure byte limits (default: 0,131072,524288)
  --submit-groups <csv>  Submit group sizes (default: 1,16)
  --output-prefix <name> Prefix for output files
  --help                 Show this help
EOF
}

output_prefix="bench-multi-profile-$(date +%F-%H%M%S)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --messages) MESSAGES="$2"; shift 2 ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --shards) SHARDS="$2"; shift 2 ;;
    --ack-modes) ACK_MODES="$2"; shift 2 ;;
    --payloads) PAYLOADS="$2"; shift 2 ;;
    --route-keys) ROUTE_KEYS="$2"; shift 2 ;;
    --max-pending) MAX_PENDING_VALUES="$2"; shift 2 ;;
    --submit-groups) SUBMIT_GROUP_SIZES="$2"; shift 2 ;;
    --output-prefix) output_prefix="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

DOC_DIR="${ROOT_DIR}/doc"
mkdir -p "${DOC_DIR}"

tsv_path="${DOC_DIR}/${output_prefix}.tsv"
md_path="${DOC_DIR}/${output_prefix}.md"

run_bench() {
  local shards="$1"
  local ack_mode="$2"
  local payload="$3"
  local route_keys="$4"
  local max_pending="$5"
  local submit_group_size="$6"
  shift 6

  local low_watermark=0
  if [[ "${max_pending}" -gt 0 ]]; then
    low_watermark=$((max_pending / 2))
  fi

  local output
  output="$(
    ./build/log_engine_bench \
      --messages "${MESSAGES}" \
      --payload-size "${payload}" \
      --batch-size "${BATCH_SIZE}" \
      --flush-ms "${FLUSH_MS}" \
      --inflight "${INFLIGHT}" \
      --ack-mode "${ack_mode}" \
      --route-keys "${route_keys}" \
      --submit-group-size "${submit_group_size}" \
      --max-pending-bytes "${max_pending}" \
      --pending-bytes-low-watermark "${low_watermark}" \
      -c "${shards}" \
      "${@}" 2>&1
  )"
  awk '/messages=/{line=$0} END{print line}' <<<"${output}"
}

extract_metric() {
  awk -v key="$2" '{
    for (i = 1; i <= NF; ++i) {
      if ($i ~ ("^" key "=")) {
        sub("^" key "=", "", $i)
        print $i
        exit
      }
    }
  }' <<<"$1"
}

csv_to_array() {
  local value="$1"
  local -n out_ref="$2"
  IFS=',' read -r -a out_ref <<<"${value}"
}

echo "=== Multi-Shard Performance Profile ==="
echo "Messages/run: ${MESSAGES}, Batch: ${BATCH_SIZE}, Inflight: ${INFLIGHT}"

# Header
{
  printf 'shards\tack_mode\tpayload_size\troute_keys\tmax_pending_bytes\tsubmit_group_size\tthroughput_msg_per_sec\tavg_submit_us\tp50_submit_us\tp95_submit_us\tp99_submit_us\tavg_group_submit_us\tp50_group_submit_us\tp95_group_submit_us\tp99_group_submit_us\n'
} >"${tsv_path}"

total_runs=0
csv_to_array "${SHARDS}" shard_values
csv_to_array "${ACK_MODES}" ack_mode_values
csv_to_array "${PAYLOADS}" payload_values
csv_to_array "${ROUTE_KEYS}" route_key_values
csv_to_array "${MAX_PENDING_VALUES}" max_pending_values
csv_to_array "${SUBMIT_GROUP_SIZES}" submit_group_values

for shards in "${shard_values[@]}"; do
  for ack_mode in "${ack_mode_values[@]}"; do
    for payload in "${payload_values[@]}"; do
      for route_keys in "${route_key_values[@]}"; do
        # Skip route_keys > 0 for shards=1 (no cross-shard distribution)
        if [[ "${shards}" -eq 1 && "${route_keys}" -gt 0 ]]; then
          continue
        fi

        for max_pending in "${max_pending_values[@]}"; do
          # Backpressure with write_ack only (sync_ack already provides backpressure)
          if [[ "${ack_mode}" == "sync_ack" && "${max_pending}" -gt 0 ]]; then
            continue
          fi

          for submit_group_size in "${submit_group_values[@]}"; do
            if [[ "${route_keys}" -eq 0 && "${submit_group_size}" -gt 1 ]]; then
              continue
            fi

            total_runs=$((total_runs + 1))
            echo -n "  [${total_runs}] shards=${shards} ack=${ack_mode} payload=${payload} route_keys=${route_keys} max_pending=${max_pending} submit_group=${submit_group_size} ... "

            line="$(run_bench "${shards}" "${ack_mode}" "${payload}" "${route_keys}" "${max_pending}" "${submit_group_size}")"

            if [[ -z "${line}" ]]; then
              echo "FAILED"
              continue
            fi

            thr="$(extract_metric "${line}" "throughput_msg_per_sec")"
            avg="$(extract_metric "${line}" "avg_submit_us")"
            p50="$(extract_metric "${line}" "p50_submit_us")"
            p95="$(extract_metric "${line}" "p95_submit_us")"
            p99="$(extract_metric "${line}" "p99_submit_us")"
            avg_group="$(extract_metric "${line}" "avg_group_submit_us")"
            p50_group="$(extract_metric "${line}" "p50_group_submit_us")"
            p95_group="$(extract_metric "${line}" "p95_group_submit_us")"
            p99_group="$(extract_metric "${line}" "p99_group_submit_us")"

            printf '%d\t%s\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
              "${shards}" "${ack_mode}" "${payload}" "${route_keys}" "${max_pending}" "${submit_group_size}" \
              "${thr}" "${avg}" "${p50}" "${p95}" "${p99}" "${avg_group}" "${p50_group}" "${p95_group}" "${p99_group}" >>"${tsv_path}"

            printf 'thr=%.0f p99=%s\n' "${thr}" "${p99}"
          done
        done
      done
    done
  done
done

# Generate summary markdown
{
  printf '# Multi-Shard Benchmark Profile\n\n'
  printf '%s\n\n' "Generated: $(date)"
  printf '%s\n' "- Messages per run: ${MESSAGES}"
  printf '%s\n' "- Batch size: ${BATCH_SIZE}"
  printf '%s\n' "- Inflight: ${INFLIGHT}"
  printf '%s\n\n' "- Total runs: ${total_runs}"

  printf '## Throughput by Shard Count (write_ack, payload=512, no backpressure)\n\n'
  printf '| Shards | Throughput (msg/s) | P50 (us) | P99 (us) |\n'
  printf '| ---: | ---: | ---: | ---: |\n'
  awk -F'\t' '$2=="write_ack" && $3+0==512 && $4+0==0 && $5+0==0 && $6+0==1 {
    printf "| %d | %.0f | %s | %s |\n", $1+0, $7+0, $9, $11
  }' "${tsv_path}"

  printf '\n## Ack Mode Comparison (shards=2, payload=512, route_keys=0, no backpressure)\n\n'
  printf '| Ack Mode | Throughput (msg/s) | P50 (us) | P99 (us) |\n'
  printf '| ---: | ---: | ---: | ---: |\n'
  awk -F'\t' '$1+0==2 && $3+0==512 && $4+0==0 && $5+0==0 && $6+0==1 {
    printf "| %s | %.0f | %s | %s |\n", $2, $7+0, $9, $11
  }' "${tsv_path}"

  printf '\n## Payload Size Impact (shards=2, write_ack, route_keys=0, no backpressure)\n\n'
  printf '| Payload (B) | Throughput (msg/s) | P50 (us) | P99 (us) |\n'
  printf '| ---: | ---: | ---: | ---: |\n'
  awk -F'\t' '$1+0==2 && $2=="write_ack" && $4+0==0 && $5+0==0 && $6+0==1 {
    printf "| %d | %.0f | %s | %s |\n", $3+0, $7+0, $9, $11
  }' "${tsv_path}"

  printf '\n## Route Key Distribution (shards=2, write_ack, payload=512, no backpressure)\n\n'
  printf '| Route Keys | Throughput (msg/s) | P50 (us) | P99 (us) |\n'
  printf '| ---: | ---: | ---: | ---: |\n'
  awk -F'\t' '$1+0==2 && $2=="write_ack" && $3+0==512 && $5+0==0 && $6+0==1 {
    printf "| %d | %.0f | %s | %s |\n", $4+0, $7+0, $9, $11
  }' "${tsv_path}"

  printf '\n## Backpressure Impact (shards=2, write_ack, payload=512, route_keys=4)\n\n'
  printf '| Max Pending (B) | Throughput (msg/s) | P50 (us) | P95 (us) | P99 (us) |\n'
  printf '| ---: | ---: | ---: | ---: | ---: |\n'
  awk -F'\t' '$1+0==2 && $2=="write_ack" && $3+0==512 && $4+0==4 && $6+0==1 {
    printf "| %d | %.0f | %s | %s | %s |\n", $5+0, $7+0, $9, $10, $11
  }' "${tsv_path}"

  printf '\n## Grouped Submit Impact (write_ack, route_keys=16, no backpressure)\n\n'
  printf '| Shards | Payload (B) | Submit Group | Throughput (msg/s) | P99 Group Submit (us) |\n'
  printf '| ---: | ---: | ---: | ---: | ---: |\n'
  awk -F'\t' '$2=="write_ack" && $4+0==16 && $5+0==0 && ($3+0==512 || $3+0==2048) && ($1+0==2 || $1+0==4) {
    printf "| %d | %d | %d | %.0f | %s |\n", $1+0, $3+0, $6+0, $7+0, $15
  }' "${tsv_path}"

  printf '\n## Scaling Efficiency\n\n'
  printf '| Shards | write_ack thr (msg/s) | Efficiency |\n'
  printf '| ---: | ---: | ---: |\n'
  awk -F'\t' '$2=="write_ack" && $3+0==512 && $4+0==0 && $5+0==0 && $6+0==1 {
    thr[$1+0] = $7+0
  }
  END {
    base = thr[1]
    if (base == 0) base = 1
    for (s = 1; s <= 4; s *= 2) {
      if (thr[s] > 0) {
        eff = (thr[s] / (s * base)) * 100
        printf "| %d | %.0f | %.1f%% |\n", s, thr[s], eff
      }
    }
  }' "${tsv_path}"

} >"${md_path}"

echo ""
echo "Wrote TSV: ${tsv_path}"
echo "Wrote summary: ${md_path}"
echo "Done: ${total_runs} benchmark runs completed"
