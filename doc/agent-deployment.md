# Seastar Log Agent Deployment

## Docker Compose

Three compose examples are provided under `deploy/compose`.

### Stdout-only local demo

```bash
docker compose -f deploy/compose/docker-compose.stdout.yml up --build
curl -fsS http://127.0.0.1:18081/healthz
curl -fsS -X POST http://127.0.0.1:18081/v1/logs \
  -H 'Content-Type: application/json' \
  -d '{"service":"demo","message":"hello"}'
```

### HTTP sink demo with fake receiver

```bash
docker compose -f deploy/compose/docker-compose.http-sink.yml up --build
curl -fsS -X POST http://127.0.0.1:18081/v1/logs \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"service":"demo","message":"a"},{"service":"demo","message":"b"}]}'
```

The fake receiver stores request bodies in its `receiver-data` volume.

### Mounted host log directory

```bash
mkdir -p logs
printf 'first\nsecond\n' > logs/app.log
docker compose -f deploy/compose/docker-compose.host-logs.yml up --build
```

The source directory is mounted read-only at `/host-logs`, while agent state is kept in the `agent-data` volume.

## Kubernetes

`deploy/kubernetes/seastar-log-agent-daemonset.yaml` contains a DaemonSet example:

- host `/var/log` is mounted read-only.
- `/var/lib/seastar-log-agent` is mounted writable for local logs, checkpoints, source offsets, delivery offsets, and pending delivery batches.
- readiness and liveness probes use `/healthz`.
- the container runs as non-root.

Apply it after creating the namespace and publishing the runtime image:

```bash
kubectl create namespace logging
kubectl apply -f deploy/kubernetes/seastar-log-agent-daemonset.yaml
kubectl -n logging rollout status daemonset/seastar-log-agent
```

## Non-root Permissions

The runtime image creates a `logagent` user and writes under `/var/lib/seastar-log-agent`.

Required permissions:

- read access to mounted source log files.
- write access to the agent data directory.
- permission to create or bind a Unix socket if `unix-socket-source-path` is enabled.
- permission to bind the HTTP ingest port. Use a non-privileged port such as `18081` unless the container has extra capabilities.

For host log files that are readable only by privileged groups, prefer adding a group mapping or using Kubernetes `fsGroup` instead of running the container as root.

## Verification

Local smoke scripts:

```bash
./script/test_agent_integration.sh
./script/test_docker_compose_smoke.sh deploy/compose/docker-compose.stdout.yml
```

Performance profile entry point:

```bash
./script/bench_agent_profiles.sh --messages 2000 --payload-sizes 128,1024,4096 --batch-sizes 10,100
```
