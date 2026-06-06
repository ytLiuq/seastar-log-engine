FROM ubuntu:24.04 AS base

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    ccache \
    cmake \
    g++-13 \
    gcc-13 \
    git \
    libboost-program-options-dev \
    libfmt-dev \
    libglog-dev \
    libgrpc++-dev \
    libprotobuf-dev \
    libspdlog-dev \
    ninja-build \
    pkg-config \
    protobuf-compiler \
    protobuf-compiler-grpc \
    python3 \
    sudo \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace/seastar-log-engine

FROM base AS seastar

ARG SEASTAR_REF=master
ENV CC=/usr/bin/gcc-13 \
    CXX=/usr/bin/g++-13 \
    INSTALL_SEASTAR_DEPS=1 \
    SEASTAR_REF=${SEASTAR_REF} \
    SEASTAR_ROOT=/workspace/.deps/seastar

COPY script/bootstrap_seastar.sh /workspace/seastar-log-engine/script/bootstrap_seastar.sh
RUN bash /workspace/seastar-log-engine/script/bootstrap_seastar.sh

FROM seastar AS build

COPY . /workspace/seastar-log-engine
RUN ./script/build.sh

FROM build AS test

RUN ./script/test_unit.sh
