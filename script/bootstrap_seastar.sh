#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEASTAR_ROOT="${SEASTAR_ROOT:-${ROOT_DIR}/.deps/seastar}"
SEASTAR_REF="${SEASTAR_REF:-master}"
SEASTAR_BUILD_MODE="${SEASTAR_BUILD_MODE:-release}"

if [[ ! -d "${SEASTAR_ROOT}/.git" ]]; then
  mkdir -p "$(dirname "${SEASTAR_ROOT}")"
  git clone --recursive https://github.com/scylladb/seastar.git "${SEASTAR_ROOT}"
fi

git -C "${SEASTAR_ROOT}" fetch --tags origin
git -C "${SEASTAR_ROOT}" checkout "${SEASTAR_REF}"
git -C "${SEASTAR_ROOT}" submodule update --init --recursive

if [[ -x "${SEASTAR_ROOT}/install-dependencies.sh" && "${INSTALL_SEASTAR_DEPS:-0}" == "1" ]]; then
  if [[ "$(id -u)" == "0" ]]; then
    "${SEASTAR_ROOT}/install-dependencies.sh"
  else
    sudo "${SEASTAR_ROOT}/install-dependencies.sh"
  fi
fi

python3 "${SEASTAR_ROOT}/configure.py" --mode="${SEASTAR_BUILD_MODE}"

cmake --build "${SEASTAR_ROOT}/build/${SEASTAR_BUILD_MODE}" -j"$(nproc)"
