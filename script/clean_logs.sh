#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

patterns=(
  "logs"
  "archive"
  "logs-glog"
  "logs-spdlog"
  "logs-bench-fast*"
  "logs-spdlog-fast*"
)

shopt -s nullglob
for pattern in "${patterns[@]}"; do
  for path in "${ROOT_DIR}"/${pattern}; do
    rm -rf "${path}"
    printf 'removed %s\n' "${path}"
  done
done
