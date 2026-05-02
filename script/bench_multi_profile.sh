#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

MESSAGES=50000
BATCH_SIZE=512
FLUSH_MS=1
INFLIGHT=16

usage() {
  cat <<'EOF'
Usage:
  ./script/bench_multi_profile.sh [options]

Runs multi-dimensional benchmark profiling across shard count, ack mode,
payload size, route key distribution, and backpressure settings.

Options:
  --messages <n>         Messages per run (default: 50000)
  --batch-size <n>       Batch size (default: 512)
  --output-prefix <name> Prefix for output files
  --help                 Show this help
EOF
}

output_prefix="bench-multi-profile-$(date +%F-%H%M%S)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --messages) MESSAGES="$2"; shift 2 ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
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
  shift 5

  local low_watermark=0
  if [[ "${max_pending}" -gt 0 ]]; then
    low_watermark=$((max_pending / 2))
  fi

  ./build/log_engine_bench \
    --messages "${MESSAGES}" \
    --payload-size "${payload}" \
    --batch-size "${BATCH_SIZE}" \
    --flush-ms "${FLUSH_MS}" \
    --inflight "${INFLIGHT}" \
    --ack-mode "${ack_mode}" \
    --route-keys "${route_keys}" \
    --max-pending-bytes "${max_pending}" \
    --pending-bytes-low-watermark "${low_watermark}" \
    -c "${shards}" \
    "${@}" 2>&1 | grep -a 'messages=' | tail -n 1
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

echo "=== Multi-Shard Performance Profile ==="
echo "Messages/run: ${MESSAGES}, Batch: ${BATCH_SIZE}, Inflight: ${INFLIGHT}"

# Header
{
  printf 'shards\tack_mode\tpayload_size\troute_keys\tmax_pending_bytes\tthroughput_msg_per_sec\tavg_submit_us\tp50_submit_us\tp95_submit_us\tp99_submit_us\n'
} >"${tsv_path}"

total_runs=0

for shards in 1 2 4; do
  for ack_mode in write_ack sync_ack; do
    for payload in 128 512 2048; do
      for route_keys in 0 4 16; do
        # Skip route_keys > 0 for shards=1 (no cross-shard distribution)
        if [[ "${shards}" -eq 1 && "${route_keys}" -gt 0 ]]; then
          continue
        fi

        for max_pending in 0 131072 524288; do
          # Backpressure with write_ack only (sync_ack already provides backpressure)
          if [[ "${ack_mode}" == "sync_ack" && "${max_pending}" -gt 0 ]]; then
            continue
          fi

          total_runs=$((total_runs + 1))
          echo -n "  [${total_runs}] shards=${shards} ack=${ack_mode} payload=${payload} route_keys=${route_keys} max_pending=${max_pending} ... "

          line="$(run_bench "${shards}" "${ack_mode}" "${payload}" "${route_keys}" "${max_pending}")"

          if [[ -z "${line}" ]]; then
            echo "FAILED"
            continue
          fi

          thr="$(extract_metric "${line}" "throughput_msg_per_sec")"
          avg="$(extract_metric "${line}" "avg_submit_us")"
          p50="$(extract_metric "${line}" "p50_submit_us")"
          p95="$(extract_metric "${line}" "p95_submit_us")"
          p99="$(extract_metric "${line}" "p99_submit_us")"

          printf '%d\t%s\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\n' \
            "${shards}" "${ack_mode}" "${payload}" "${route_keys}" "${max_pending}" \
            "${thr}" "${avg}" "${p50}" "${p95}" "${p99}" >>"${tsv_path}"

          printf 'thr=%.0f p99=%s\n' "${thr}" "${p99}"
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
  awk -F'\t' '$2=="write_ack" && $3+0==512 && $4+0==0 && $5+0==0 {
    printf "| %d | %.0f | %s | %s |\n", $1+0, $6+0, $8, $10
  }' "${tsv_path}"

  printf '\n## Ack Mode Comparison (shards=2, payload=512, route_keys=0, no backpressure)\n\n'
  printf '| Ack Mode | Throughput (msg/s) | P50 (us) | P99 (us) |\n'
  printf '| ---: | ---: | ---: | ---: |\n'
  awk -F'\t' '$1+0==2 && $3+0==512 && $4+0==0 && $5+0==0 {
    printf "| %s | %.0f | %s | %s |\n", $2, $6+0, $8, $10
  }' "${tsv_path}"

  printf '\n## Payload Size Impact (shards=2, write_ack, route_keys=0, no backpressure)\n\n'
  printf '| Payload (B) | Throughput (msg/s) | P50 (us) | P99 (us) |\n'
  printf '| ---: | ---: | ---: | ---: |\n'
  awk -F'\t' '$1+0==2 && $2=="write_ack" && $4+0==0 && $5+0==0 {
    printf "| %d | %.0f | %s | %s |\n", $3+0, $6+0, $8, $10
  }' "${tsv_path}"

  printf '\n## Route Key Distribution (shards=2, write_ack, payload=512, no backpressure)\n\n'
  printf '| Route Keys | Throughput (msg/s) | P50 (us) | P99 (us) |\n'
  printf '| ---: | ---: | ---: | ---: |\n'
  awk -F'\t' '$1+0==2 && $2=="write_ack" && $3+0==512 && $5+0==0 {
    printf "| %d | %.0f | %s | %s |\n", $4+0, $6+0, $8, $10
  }' "${tsv_path}"

  printf '\n## Backpressure Impact (shards=2, write_ack, payload=512, route_keys=4)\n\n'
  printf '| Max Pending (B) | Throughput (msg/s) | P50 (us) | P95 (us) | P99 (us) |\n'
  printf '| ---: | ---: | ---: | ---: | ---: |\n'
  awk -F'\t' '$1+0==2 && $2=="write_ack" && $3+0==512 && $4+0==4 {
    printf "| %d | %.0f | %s | %s | %s |\n", $5+0, $6+0, $8, $9, $10
  }' "${tsv_path}"

  printf '\n## Scaling Efficiency\n\n'
  printf '| Shards | write_ack thr (msg/s) | Efficiency |\n'
  printf '| ---: | ---: | ---: |\n'
  awk -F'\t' '$2=="write_ack" && $3+0==512 && $4+0==0 && $5+0==0 {
    thr[$1+0] = $6+0
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
