#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

TMP_DIR="$(mktemp -d /tmp/seastar-log-agent-integration.XXXXXX)"
LOG_DIR="${TMP_DIR}/logs"
ARCHIVE_DIR="${TMP_DIR}/archive"
SOURCE_DIR="${TMP_DIR}/source"
SINK_OUT="${TMP_DIR}/sink.ndjson"
AGENT_CONFIG="${TMP_DIR}/agent.conf"
HTTP_PORT=18181
SINK_PORT=19191

cleanup() {
  if [[ -n "${AGENT_PID:-}" ]] && kill -0 "${AGENT_PID}" 2>/dev/null; then
    kill "${AGENT_PID}" 2>/dev/null || true
    wait "${AGENT_PID}" 2>/dev/null || true
  fi
  if [[ -n "${SINK_PID:-}" ]] && kill -0 "${SINK_PID}" 2>/dev/null; then
    kill "${SINK_PID}" 2>/dev/null || true
    wait "${SINK_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

mkdir -p "${LOG_DIR}" "${ARCHIVE_DIR}" "${SOURCE_DIR}"
: > "${AGENT_CONFIG}"

wait_http() {
  local url="$1"
  for _ in $(seq 1 100); do
    if python3 - "${url}" <<'PY' >/dev/null 2>&1
import sys
from urllib.request import urlopen

with urlopen(sys.argv[1], timeout=1) as response:
    raise SystemExit(0 if 200 <= response.status < 300 else 1)
PY
    then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for ${url}" >&2
  return 1
}

start_sink() {
  python3 script/fake_http_sink.py \
    --host 127.0.0.1 \
    --port "${SINK_PORT}" \
    --out "${SINK_OUT}" \
    --fail-first "${1:-0}" &
  SINK_PID=$!
  wait_http "http://127.0.0.1:${SINK_PORT}/healthz"
}

start_agent() {
  ./build/log_engine_agent \
    --config "${AGENT_CONFIG}" \
    --log-dir "${LOG_DIR}" \
    --archive-dir "${ARCHIVE_DIR}" \
    --ack-mode sync_ack \
    --batch-size 1 \
    --flush-ms 50 \
    --checkpoint-enabled true \
    --truncate-on-start false \
    --record-crc-enabled true \
    --record-crc-class full \
    --record-timestamp-enabled true \
    --record-level-enabled true \
    --record-shard-id-enabled true \
    --record-sequence-enabled true \
    --rotate-size-bytes 512 \
    --http-ingest-address 127.0.0.1 \
    --http-ingest-port "${HTTP_PORT}" \
    --agent-id integration-agent \
    --file-source-glob "${SOURCE_DIR}/*.log" \
    --source-offset-path "${TMP_DIR}/agent-source.offset" \
    --source-poll-ms 100 \
    --sink-kind http \
    --sink-http-url "http://127.0.0.1:${SINK_PORT}/ingest" \
    --sink-batch-size 2 \
    --sink-retry-backoff-ms 100 \
    --sink-retry-max-backoff-ms 500 \
    --delivery-offset-path "${TMP_DIR}/agent-delivery.offset" \
    --pending-delivery-path "${TMP_DIR}/agent-delivery.pending" \
    -c 1 >/tmp/seastar-log-agent-integration-agent.out 2>&1 &
  AGENT_PID=$!
  if ! wait_http "http://127.0.0.1:${HTTP_PORT}/healthz"; then
    echo "--- agent output ---" >&2
    cat /tmp/seastar-log-agent-integration-agent.out >&2 || true
    return 1
  fi
}

stop_agent() {
  if [[ -n "${AGENT_PID:-}" ]] && kill -0 "${AGENT_PID}" 2>/dev/null; then
    kill "${AGENT_PID}" 2>/dev/null || true
    wait "${AGENT_PID}" 2>/dev/null || true
    AGENT_PID=""
  fi
}

require_sink_contains() {
  local needle="$1"
  for _ in $(seq 1 100); do
    if [[ -f "${SINK_OUT}" ]] && grep -q "${needle}" "${SINK_OUT}"; then
      return 0
    fi
    sleep 0.1
  done
  echo "sink output did not contain ${needle}" >&2
  cat "${SINK_OUT}" >&2 || true
  echo "--- agent output ---" >&2
  cat /tmp/seastar-log-agent-integration-agent.out >&2 || true
  echo "--- agent status ---" >&2
  python3 - "http://127.0.0.1:${HTTP_PORT}/v1/status" <<'PY' >&2 || true
import sys
from urllib.request import urlopen

with urlopen(sys.argv[1], timeout=1) as response:
    print(response.read().decode("utf-8", errors="replace"))
PY
  echo "--- log files ---" >&2
  find "${LOG_DIR}" "${ARCHIVE_DIR}" -maxdepth 1 -type f -print -exec wc -c {} \; >&2 || true
  return 1
}

echo "--- agent integration: crash/restart replay + HTTP retry ---"
start_sink 1
start_agent

python3 - "http://127.0.0.1:${HTTP_PORT}/v1/logs" <<'PY' >/dev/null
import sys
from urllib.request import Request, urlopen

body = b'{"records":[{"message":"http-a","service":"svc"},{"message":"http-b","service":"svc"}]}'
request = Request(sys.argv[1], data=body, headers={"Content-Type": "application/json"}, method="POST")
with urlopen(request, timeout=5) as response:
    raise SystemExit(0 if 200 <= response.status < 300 else 1)
PY
require_sink_contains "http-a"
require_sink_contains "http-b"

stop_agent
printf 'file-a\nfile-b\npartial' > "${SOURCE_DIR}/app.log"
start_agent
require_sink_contains "file-a"
require_sink_contains "file-b"

echo "--- agent integration: rotate + tail + delivery offset ---"
printf 'file-c\nfile-d\n' >> "${SOURCE_DIR}/app.log"
require_sink_contains "file-c"
require_sink_contains "file-d"
test -f "${TMP_DIR}/agent-delivery.offset"

echo "--- agent integration: permission error smoke ---"
set +e
bash -c './build/log_engine_agent \
  --config "$1" \
  --log-dir /dev/full/logs \
  --archive-dir /dev/full/archive \
  --http-ingest-address 127.0.0.1 \
  --http-ingest-port 18182 \
  -c 1 >/tmp/seastar-log-agent-permission.out 2>&1' \
  bash "${AGENT_CONFIG}" >/tmp/seastar-log-agent-permission-wrapper.out 2>&1
PERM_RC=$?
set -e
if [[ "${PERM_RC}" -eq 0 ]]; then
  echo "expected permission-error smoke to fail" >&2
  exit 1
fi

echo "agent integration OK"
