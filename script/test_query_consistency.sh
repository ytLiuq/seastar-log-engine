#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

TMP_DIR="$(mktemp -d /tmp/log-engine-query-consistency.XXXXXX)"
LOG_DIR="${TMP_DIR}/logs"
ARCHIVE_DIR="${TMP_DIR}/archive"
HTTP_PORT=18081
GRPC_PORT=19091
METRICS_PORT=19191
QUERY_PID=""

cleanup() {
  if [[ -n "${QUERY_PID}" ]] && kill -0 "${QUERY_PID}" 2>/dev/null; then
    kill "${QUERY_PID}" 2>/dev/null || true
    wait "${QUERY_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT

mkdir -p "${LOG_DIR}" "${ARCHIVE_DIR}"

./build/log_engine_demo \
  --log-dir "${LOG_DIR}" \
  --archive-dir "${ARCHIVE_DIR}" \
  --messages 40 \
  --payload-size 160 \
  --batch-size 4 \
  --rotate-size-bytes 1024 \
  --truncate-on-start 1 \
  -c 2 >/tmp/log_engine_query_consistency_demo.out 2>&1

./build/log_engine_query_server \
  --log-dir "${LOG_DIR}" \
  --archive-dir "${ARCHIVE_DIR}" \
  --http-address 127.0.0.1 \
  --http-port "${HTTP_PORT}" \
  --grpc-address 127.0.0.1 \
  --grpc-port "${GRPC_PORT}" \
  --metrics-address 127.0.0.1 \
  --metrics-port "${METRICS_PORT}" \
  -c 2 >/tmp/log_engine_query_consistency_server.out 2>&1 &
QUERY_PID=$!

for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:${HTTP_PORT}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

curl -fsS "http://127.0.0.1:${HTTP_PORT}/healthz" >/dev/null

HTTP_STATUS="$(curl -fsS "http://127.0.0.1:${HTTP_PORT}/v1/status")"
GRPC_STATUS="$(./build/log_engine_query_client --target "127.0.0.1:${GRPC_PORT}" --method status)"
if [[ "${HTTP_STATUS}" != "${GRPC_STATUS}" ]]; then
  printf 'status mismatch\nHTTP: %s\ngRPC: %s\n' "${HTTP_STATUS}" "${GRPC_STATUS}" >&2
  exit 1
fi

HTTP_ROUTE="$(curl -fsS "http://127.0.0.1:${HTTP_PORT}/v1/route?key=route-a")"
GRPC_ROUTE="$(./build/log_engine_query_client --target "127.0.0.1:${GRPC_PORT}" --method route --route-key route-a)"
if [[ "${HTTP_ROUTE}" != "${GRPC_ROUTE}" ]]; then
  printf 'route mismatch\nHTTP: %s\ngRPC: %s\n' "${HTTP_ROUTE}" "${GRPC_ROUTE}" >&2
  exit 1
fi

HTTP_RECORDS="$(curl -fsS "http://127.0.0.1:${HTTP_PORT}/v1/records?include_archive=true&limit=100")"
GRPC_RECORDS="$(./build/log_engine_query_client --target "127.0.0.1:${GRPC_PORT}" --method records --include-archive true --limit 100)"
if [[ "${HTTP_RECORDS}" != "${GRPC_RECORDS}" ]]; then
  printf 'records mismatch\nHTTP: %s\ngRPC: %s\n' "${HTTP_RECORDS}" "${GRPC_RECORDS}" >&2
  exit 1
fi

printf 'query consistency OK\n'
