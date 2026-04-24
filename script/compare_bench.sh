#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

MESSAGES="${1:-50000}"
PAYLOAD_SIZE="${2:-128}"

echo "[log_engine_bench]"
./build/log_engine_bench \
  --log-dir "${ROOT_DIR}/logs" \
  --archive-dir "${ROOT_DIR}/archive" \
  --messages "${MESSAGES}" \
  --payload-size "${PAYLOAD_SIZE}" \
  --batch-size 256 \
  --inflight 64 \
  --rotate-size-bytes 1048576 \
  -c 2

if [[ -x "${ROOT_DIR}/build/glog_bench" ]]; then
  echo "[glog_bench]"
  rm -rf "${ROOT_DIR}/logs-glog"
  mkdir -p "${ROOT_DIR}/logs-glog"
  "${ROOT_DIR}/build/glog_bench" \
    --log-dir "${ROOT_DIR}/logs-glog" \
    --messages "${MESSAGES}" \
    --payload-size "${PAYLOAD_SIZE}"
else
  echo "glog_bench not built"
fi
