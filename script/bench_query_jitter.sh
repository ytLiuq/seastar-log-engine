#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

SHARDS=2
COLD_ITERATIONS=30
HOT_ITERATIONS=60
WRITER_MESSAGES=200000
PAYLOAD_SIZE=512
WRITER_BATCH=256
WRITER_FLUSH_MS=1

usage() {
  cat <<'EOF'
Usage:
  ./script/bench_query_jitter.sh [options]

Measures query latency jitter under concurrent write load.
Compares "cold" (writer idle) vs "hot" (writer active) query performance.

Options:
  --shards <n>             Seastar shard count (default: 2)
  --cold-iterations <n>    Cold query cycles (default: 30)
  --hot-iterations <n>     Hot query cycles (default: 60)
  --payload-size <n>       Writer payload size (default: 512)
  --help                   Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --shards) SHARDS="$2"; shift 2 ;;
    --cold-iterations) COLD_ITERATIONS="$2"; shift 2 ;;
    --hot-iterations) HOT_ITERATIONS="$2"; shift 2 ;;
    --payload-size) PAYLOAD_SIZE="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

TMP_DIR="$(mktemp -d /tmp/log-engine-query-jitter.XXXXXX)"
LOG_DIR="${TMP_DIR}/logs"
ARCHIVE_DIR="${TMP_DIR}/archive"
HTTP_PORT=18086
GRPC_PORT=19096
METRICS_PORT=19196
COLD_LOG="${TMP_DIR}/cold_latency.tsv"
HOT_LOG="${TMP_DIR}/hot_latency.tsv"

cleanup() {
  if [[ -n "${QUERY_PID:-}" ]] && kill -0 "${QUERY_PID}" 2>/dev/null; then
    kill -9 "${QUERY_PID}" 2>/dev/null || true
    wait "${QUERY_PID}" 2>/dev/null || true
  fi
  if [[ -n "${WRITER_PID:-}" ]] && kill -0 "${WRITER_PID}" 2>/dev/null; then
    kill -9 "${WRITER_PID}" 2>/dev/null || true
    wait "${WRITER_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

mkdir -p "${LOG_DIR}" "${ARCHIVE_DIR}"

# Pre-populate some data so queries have records to read
echo "=== Pre-populating data ==="
./build/log_engine_demo \
  --log-dir "${LOG_DIR}" \
  --archive-dir "${ARCHIVE_DIR}" \
  --messages 5000 \
  --payload-size "${PAYLOAD_SIZE}" \
  --batch-size "${WRITER_BATCH}" \
  --flush-ms "${WRITER_FLUSH_MS}" \
  --rotate-size-bytes 32768 \
  --checkpoint-enabled 1 \
  --compress-archives 1 \
  --truncate-on-start 1 \
  -c "${SHARDS}" >/tmp/log_engine_jitter_prep.out 2>&1

# Start query server
echo "=== Starting query server ==="
./build/log_engine_query_server \
  --log-dir "${LOG_DIR}" \
  --archive-dir "${ARCHIVE_DIR}" \
  --http-address 127.0.0.1 \
  --http-port "${HTTP_PORT}" \
  --grpc-address 127.0.0.1 \
  --grpc-port "${GRPC_PORT}" \
  --metrics-address 127.0.0.1 \
  --metrics-port "${METRICS_PORT}" \
  -c "${SHARDS}" >/tmp/log_engine_jitter_query.out 2>&1 &
QUERY_PID=$!

for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:${HTTP_PORT}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

measure_latency() {
  local url="$1"
  curl -fsS -o /dev/null -w '%{time_total}\t%{time_connect}\t%{time_starttransfer}\t%{http_code}' "${url}" 2>/dev/null
}

run_query_cycle() {
  local label="$1"
  local iterations="$2"
  local log_file="$3"

  printf 'iteration\tmethod\ttotal_s\tconnect_s\tttfb_s\thttp_code\n' >"${log_file}"

  for i in $(seq 1 "${iterations}"); do
    # Status query
    local result
    result="$(measure_latency "http://127.0.0.1:${HTTP_PORT}/v1/status")" || true
    if [[ -n "${result}" ]]; then
      printf '%d\tstatus\t%s\n' "${i}" "${result}" >>"${log_file}"
    fi

    # Records query
    result="$(measure_latency "http://127.0.0.1:${HTTP_PORT}/v1/records?limit=20&include_archive=true")" || true
    if [[ -n "${result}" ]]; then
      printf '%d\trecords\t%s\n' "${i}" "${result}" >>"${log_file}"
    fi

    # Route query
    result="$(measure_latency "http://127.0.0.1:${HTTP_PORT}/v1/route?key=test-key-${i}")" || true
    if [[ -n "${result}" ]]; then
      printf '%d\troute\t%s\n' "${i}" "${result}" >>"${log_file}"
    fi
  done
}

# Phase 1: Cold queries (no writer)
echo "=== Phase 1: Cold queries (${COLD_ITERATIONS} cycles) ==="
run_query_cycle "cold" "${COLD_ITERATIONS}" "${COLD_LOG}"

# Phase 2: Start writer in background
echo "=== Phase 2: Start writer + hot queries (${HOT_ITERATIONS} cycles) ==="
./build/log_engine_bench \
  --log-dir "${LOG_DIR}" \
  --archive-dir "${ARCHIVE_DIR}" \
  --messages "${WRITER_MESSAGES}" \
  --payload-size "${PAYLOAD_SIZE}" \
  --batch-size "${WRITER_BATCH}" \
  --flush-ms "${WRITER_FLUSH_MS}" \
  --inflight 4 \
  --checkpoint-enabled 1 \
  --rotate-size-bytes 65536 \
  --compress-archives 1 \
  --truncate-on-start 0 \
  -c "${SHARDS}" >/tmp/log_engine_jitter_writer.out 2>&1 &
WRITER_PID=$!

# Give writer a moment to start producing
sleep 1

# Run hot queries while writer is active
run_query_cycle "hot" "${HOT_ITERATIONS}" "${HOT_LOG}"

# Wait for writer to finish
wait "${WRITER_PID}" 2>/dev/null || true
WRITER_PID=""

# Stop query server
kill "${QUERY_PID}" 2>/dev/null || true
wait "${QUERY_PID}" 2>/dev/null || true
QUERY_PID=""

# Generate summary
echo ""
echo "=== Query Jitter Report ==="

generate_stats() {
  local label="$1"
  local log_file="$2"

  awk -F'\t' -v label="${label}" '
    NR == 1 { next }
    {
      method = $2
      total = $3 + 0
      ttfb = $5 + 0
      if (total > 0) {
        count[method]++
        sum[method] += total
        if (count[method] == 1 || total < min[method]) min[method] = total
        if (count[method] == 1 || total > max[method]) max[method] = total
        ttfb_sum[method] += ttfb
      }
    }
    END {
      for (m in count) {
        if (count[m] > 0) {
          avg = sum[m] / count[m]
          avg_ttfb = ttfb_sum[m] / count[m]
          printf "  %-8s %5d  %8.4f  %8.4f  %8.4f  %8.4f\n", m, count[m], min[m], avg, max[m], avg_ttfb
        }
      }
    }
  ' "${log_file}"
}

printf '\n%-10s %-8s %5s  %8s  %8s  %8s  %8s\n' "Phase" "Method" "Count" "Min(s)" "Avg(s)" "Max(s)" "AvgTTFB(s)"
printf '%-10s %-8s %5s  %8s  %8s  %8s  %8s\n' "----------" "--------" "-----" "--------" "--------" "--------" "----------"

echo ""
echo "Cold (writer idle):"
generate_stats "cold" "${COLD_LOG}"

echo ""
echo "Hot (writer active):"
generate_stats "hot" "${HOT_LOG}"

# Compare
echo ""
echo "=== Comparison (avg latency, method=records) ==="
cold_avg="$(awk -F'\t' 'NR>1 && $2=="records" && $3+0>0 { sum+=$3; n++ } END { if(n>0) printf "%.6f", sum/n; else print "N/A" }' "${COLD_LOG}")"
hot_avg="$(awk -F'\t' 'NR>1 && $2=="records" && $3+0>0 { sum+=$3; n++ } END { if(n>0) printf "%.6f", sum/n; else print "N/A" }' "${HOT_LOG}")"
cold_p99="$(awk -F'\t' 'NR>1 && $2=="records" && $3+0>0 { a[NR]=$3+0 } END { n=length(a); if(n>0) { asort(a); printf "%.6f", a[int(n*0.99)] } else print "N/A" }' "${COLD_LOG}")"
hot_p99="$(awk -F'\t' 'NR>1 && $2=="records" && $3+0>0 { a[NR]=$3+0 } END { n=length(a); if(n>0) { asort(a); printf "%.6f", a[int(n*0.99)] } else print "N/A" }' "${HOT_LOG}")"

echo "  cold avg: ${cold_avg}s  p99: ${cold_p99}s"
echo "  hot  avg: ${hot_avg}s  p99: ${hot_p99}s"

if [[ "${cold_avg}" != "N/A" && "${hot_avg}" != "N/A" ]]; then
  ratio="$(awk "BEGIN { printf \"%.2f\", ${hot_avg} / ${cold_avg} }")"
  echo "  hot/cold ratio: ${ratio}x"
fi

echo ""
echo "Raw data: ${COLD_LOG} ${HOT_LOG}"
echo "Done."
