#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

rm -f logs/* archive/*
./build/log_engine_demo \
  --log-dir "${ROOT_DIR}/logs" \
  --archive-dir "${ROOT_DIR}/archive" \
  --messages 20 \
  --payload-size 64 \
  --batch-size 4 \
  --checkpoint-enabled 1 \
  --truncate-on-start 1 \
  -c 2 >/tmp/log_engine_recovery_1.out 2>&1

printf 'BROKEN_TAIL' >> "${ROOT_DIR}/logs/shard-0.log"

./build/log_engine_demo \
  --log-dir "${ROOT_DIR}/logs" \
  --archive-dir "${ROOT_DIR}/archive" \
  --messages 4 \
  --payload-size 64 \
  --batch-size 2 \
  --checkpoint-enabled 1 \
  --truncate-on-start 0 \
  -c 2 >/tmp/log_engine_recovery_2.out 2>&1

./build/log_engine_verify --path "${ROOT_DIR}/logs/shard-0.log"
