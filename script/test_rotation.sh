#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

rm -f logs/* archive/*
./build/log_engine_demo \
  --log-dir "${ROOT_DIR}/logs" \
  --archive-dir "${ROOT_DIR}/archive" \
  --messages 200 \
  --payload-size 256 \
  --batch-size 8 \
  --rotate-size-bytes 4096 \
  --max-archived-files 2 \
  -c 2 >/tmp/log_engine_rotation.out 2>&1

find "${ROOT_DIR}/archive" -maxdepth 1 -type f | sort
