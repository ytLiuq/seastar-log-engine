#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
SEASTAR_BUILD_DIR="${SEASTAR_BUILD_DIR:-${ROOT_DIR}/../seastar/build/release}"
C_COMPILER="${CC:-/usr/bin/gcc-13}"
CXX_COMPILER="${CXX:-/usr/bin/g++-13}"

rm -rf "${BUILD_DIR}"

CCACHE_TEMPDIR="${CCACHE_TEMPDIR:-/tmp/ccache-tmp}" \
CCACHE_DIR="${CCACHE_DIR:-/tmp/ccache}" \
cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" \
    -DSEASTAR_BUILD_DIR="${SEASTAR_BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER="${C_COMPILER}" \
    -DCMAKE_CXX_COMPILER="${CXX_COMPILER}"
cmake --build "${BUILD_DIR}" -j"$(nproc)"
