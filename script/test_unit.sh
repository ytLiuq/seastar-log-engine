#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

mkdir -p "${ROOT_DIR}/test-tmp"
./build/log_engine_unit_tests --root-dir "${ROOT_DIR}/test-tmp" -c 2
