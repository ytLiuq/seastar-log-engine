#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

./build/log_engine_read \
  --log-dir "${ROOT_DIR}/logs" \
  --archive-dir "${ROOT_DIR}/archive" \
  --seq-from 0 \
  --seq-to 10 \
  --limit 20
