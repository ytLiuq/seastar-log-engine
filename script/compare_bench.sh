#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

usage() {
  cat <<'EOF'
Usage:
  ./script/compare_bench.sh [messages] [payload_size]
  ./script/compare_bench.sh --scan [output_tsv]

Default mode:
  Run one comparison across log_engine / glog / spdlog.

Scan mode:
  Run a small benchmark matrix and write structured TSV output.
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
  local messages="$1"
  local payload_size="$2"
  local batch_size="$3"
  local inflight="$4"
  local checkpoint_enabled="$5"
  local shard_count="${6:-2}"
  local flush_ms="${7:-0}"

  rm -rf "${ROOT_DIR}/logs" "${ROOT_DIR}/archive"
  mkdir -p "${ROOT_DIR}/logs" "${ROOT_DIR}/archive"

  ./build/log_engine_bench \
    --mode fast \
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
    -c "${shard_count}" 2>&1 | tee /tmp/log_engine_compare.out
  grep -a 'messages=' /tmp/log_engine_compare.out | tail -n 1
}

run_glog() {
  local messages="$1"
  local payload_size="$2"

  rm -rf "${ROOT_DIR}/logs-glog"
  mkdir -p "${ROOT_DIR}/logs-glog"

  "${ROOT_DIR}/build/glog_bench" \
    --log-dir "${ROOT_DIR}/logs-glog" \
    --messages "${messages}" \
    --payload-size "${payload_size}" 2>&1 | tee /tmp/glog_compare.out
  grep -a 'messages=' /tmp/glog_compare.out | tail -n 1
}

run_spdlog() {
  local messages="$1"
  local payload_size="$2"

  rm -rf "${ROOT_DIR}/logs-spdlog"
  mkdir -p "${ROOT_DIR}/logs-spdlog"

  "${ROOT_DIR}/build/spdlog_bench" \
    --log-dir "${ROOT_DIR}/logs-spdlog" \
    --messages "${messages}" \
    --payload-size "${payload_size}" 2>&1 | tee /tmp/spdlog_compare.out
  grep -a 'messages=' /tmp/spdlog_compare.out | tail -n 1
}

emit_scan_row() {
  local scenario="$1"
  local target="$2"
  local messages="$3"
  local payload_size="$4"
  local batch_size="$5"
  local inflight="$6"
  local checkpoint_enabled="$7"
  local raw_line="$8"

  local elapsed_us throughput avg_submit_us
  elapsed_us="$(extract_metric "${raw_line}" "elapsed_us")"
  throughput="$(extract_metric "${raw_line}" "throughput_msg_per_sec")"
  avg_submit_us="$(extract_metric "${raw_line}" "avg_submit_us")"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${scenario}" \
    "${target}" \
    "${messages}" \
    "${payload_size}" \
    "${batch_size}" \
    "${inflight}" \
    "${checkpoint_enabled}" \
    "${elapsed_us:-}" \
    "${throughput:-}" \
    "${avg_submit_us:-}"
}

run_default_compare() {
  local messages="${1:-50000}"
  local payload_size="${2:-128}"

  echo "[log_engine_bench]"
  run_log_engine "${messages}" "${payload_size}" 32 1 0 1 0

  if [[ -x "${ROOT_DIR}/build/glog_bench" ]]; then
    echo "[glog_bench]"
    run_glog "${messages}" "${payload_size}"
  else
    echo "glog_bench not built"
  fi

  if [[ -x "${ROOT_DIR}/build/spdlog_bench" ]]; then
    echo "[spdlog_bench]"
    run_spdlog "${messages}" "${payload_size}"
  else
    echo "spdlog_bench not built"
  fi
}

run_scan() {
  local output_path="${1:-${ROOT_DIR}/doc/benchmark-scan-$(date +%F).tsv}"

  {
    printf 'scenario\ttarget\tmessages\tpayload_size\tbatch_size\tinflight\tcheckpoint_enabled\telapsed_us\tthroughput_msg_per_sec\tavg_submit_us\n'

    local payload messages line
    messages=50000
    for payload in 128 512; do
      line="$(run_log_engine "${messages}" "${payload}" 32 1 0 1 0)"
      emit_scan_row "payload-${payload}" "log_engine" "${messages}" "${payload}" 32 1 0 "${line}"

      if [[ -x "${ROOT_DIR}/build/glog_bench" ]]; then
        line="$(run_glog "${messages}" "${payload}")"
        emit_scan_row "payload-${payload}" "glog" "${messages}" "${payload}" "" "" "" "${line}"
      fi

      if [[ -x "${ROOT_DIR}/build/spdlog_bench" ]]; then
        line="$(run_spdlog "${messages}" "${payload}")"
        emit_scan_row "payload-${payload}" "spdlog" "${messages}" "${payload}" "" "" "" "${line}"
      fi
    done

    local batch
    for batch in 64 256 1024 8192; do
      line="$(run_log_engine "${messages}" 128 "${batch}" 1 0 1 0)"
      emit_scan_row "batch-${batch}" "log_engine" "${messages}" 128 "${batch}" 1 0 "${line}"
    done

    local inflight
    for inflight in 1 4 16; do
      line="$(run_log_engine "${messages}" 128 32 "${inflight}" 0 1 0)"
      emit_scan_row "inflight-${inflight}" "log_engine" "${messages}" 128 32 "${inflight}" 0 "${line}"
    done

    local checkpoint
    for checkpoint in 0 1; do
      line="$(run_log_engine "${messages}" 128 32 1 "${checkpoint}" 1 0)"
      emit_scan_row "checkpoint-${checkpoint}" "log_engine" "${messages}" 128 32 1 "${checkpoint}" "${line}"
    done
  } >"${output_path}"

  echo "Wrote scan results to ${output_path}"
}

main() {
  if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
  fi

  if [[ "${1:-}" == "--scan" ]]; then
    shift
    run_scan "${1:-}"
    exit 0
  fi

  run_default_compare "${1:-50000}" "${2:-128}"
}

main "$@"
