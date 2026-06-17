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
    libgoogle-glog-dev \
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

ARG SEASTAR_REF=seastar-25.05.0
ENV CC=/usr/bin/gcc-13 \
    CXX=/usr/bin/g++-13 \
    INSTALL_SEASTAR_DEPS=1 \
    SEASTAR_FETCH=0 \
    SEASTAR_REF=${SEASTAR_REF} \
    SEASTAR_BUILD_JOBS=2 \
    SEASTAR_BUILD_TARGET=seastar \
    SEASTAR_ROOT=/workspace/.deps/seastar

COPY script/bootstrap_seastar.sh /workspace/seastar-log-engine/script/bootstrap_seastar.sh
RUN apt-get update
RUN bash /workspace/seastar-log-engine/script/bootstrap_seastar.sh

FROM seastar AS build

COPY . /workspace/seastar-log-engine
RUN ./script/build.sh

FROM build AS test

RUN ./script/test_unit.sh

FROM ubuntu:24.04 AS agent-runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libboost-program-options1.83.0 \
    libfmt9 \
    libgrpc++1.51 \
    libprotobuf32t64 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system logagent \
    && useradd --system --gid logagent --home-dir /var/lib/seastar-log-agent --create-home logagent

WORKDIR /app
COPY --from=build /workspace/seastar-log-engine/build/log_engine_agent /usr/local/bin/log_engine_agent
COPY config/agent.conf /etc/seastar-log-agent/agent.conf

RUN mkdir -p /var/lib/seastar-log-agent/logs /var/lib/seastar-log-agent/archive \
    && chown -R logagent:logagent /var/lib/seastar-log-agent

USER logagent
VOLUME ["/var/lib/seastar-log-agent"]
EXPOSE 18081

ENTRYPOINT ["/usr/local/bin/log_engine_agent"]
CMD ["--config", "/etc/seastar-log-agent/agent.conf", "--log-dir", "/var/lib/seastar-log-agent/logs", "--archive-dir", "/var/lib/seastar-log-agent/archive"]
