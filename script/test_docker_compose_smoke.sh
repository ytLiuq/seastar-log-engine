#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE_FILE="${1:-deploy/compose/docker-compose.stdout.yml}"
PROJECT_NAME="seastar-log-agent-smoke"

cleanup() {
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" up -d --build

for _ in $(seq 1 120); do
  if curl -fsS http://127.0.0.1:18081/healthz >/dev/null 2>&1; then
    curl -fsS -X POST http://127.0.0.1:18081/v1/logs \
      -H 'Content-Type: application/json' \
      -d '{"message":"compose-smoke","service":"demo"}' >/dev/null
    echo "docker compose smoke OK"
    exit 0
  fi
  sleep 1
done

docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" logs
echo "docker compose smoke failed" >&2
exit 1
