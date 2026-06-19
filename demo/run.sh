#!/usr/bin/env bash
# Showcase: run a real Python @env.task on Armada THROUGH a Flyte 2 backend, in one command.
# The task is registered with FlyteAdmin, runs as an Armada-scheduled pod, and shows up in the
# Flyte UI with its real result.
#
# This script does the fiddly wiring for you:
#   - builds the task image and loads it into the Armada (kind) cluster,
#   - reads the backend's blob-store endpoint and credentials,
#   - starts the connector service pointed at that store,
#   - runs the example and prints the result.
#
# One-time prerequisites are in demo/README.md (an Armada cluster, and a Flyte backend whose
# executor routes `armada` tasks to the connector). Override defaults with env vars if needed:
#   KIND_CLUSTER (default armada-test), DEVBOX (default flyte-devbox), HOST_IP (auto-detected).
set -euo pipefail

cd "$(dirname "$0")/.."
PY=./.venv/bin/python
KIND_CLUSTER="${KIND_CLUSTER:-armada-test}"
DEVBOX="${DEVBOX:-flyte-devbox}"
IMAGE=armada-flyte-task:latest
HOST_IP="${HOST_IP:-$(ipconfig getifaddr en0 2>/dev/null || hostname -I | awk '{print $1}')}"
KC="env KUBECONFIG=/etc/rancher/k3s/k3s.yaml"

echo "==> 1/3  Build the task image and load it into the Armada cluster ($KIND_CLUSTER)"
BUILD="$(mktemp -d)"
mkdir -p "$BUILD/pkg"
cp -R pyproject.toml src "$BUILD/pkg/"
cat > "$BUILD/Dockerfile" <<'EOF'
FROM python:3.11-slim
RUN pip install --no-cache-dir "flyte==2.5.1"
COPY pkg /pkg
RUN pip install --no-cache-dir /pkg
EOF
docker build -t "$IMAGE" "$BUILD" >/dev/null
rm -rf "$BUILD"
kind load docker-image "$IMAGE" --name "$KIND_CLUSTER" >/dev/null

echo "==> 2/3  Point the connector at the backend's blob store"
# The Armada pods (on the kind cluster) read/write the backend's bucket through its host-published
# NodePort, with the backend's own credentials.
key=$(docker exec "$DEVBOX" sh -c "$KC kubectl get secret rustfs-secret -n flyte -o jsonpath='{.data.RUSTFS_ACCESS_KEY}' | base64 -d")
secret=$(docker exec "$DEVBOX" sh -c "$KC kubectl get secret rustfs-secret -n flyte -o jsonpath='{.data.RUSTFS_SECRET_KEY}' | base64 -d")
port=$(docker exec "$DEVBOX" sh -c "$KC kubectl get svc rustfs-svc -n flyte -o jsonpath='{.spec.ports[0].nodePort}'")
blob="http://$HOST_IP:$port"
echo "    blob store: $blob"

pkill -f "bin/c0 --port 8000" 2>/dev/null || true
sleep 2
ARMADA_URL="${ARMADA_URL:-localhost:50051}" BINOCULARS_URL="${BINOCULARS_URL:-localhost:50053}" \
  FLYTE_BLOB_ENDPOINT="$blob" FLYTE_BLOB_ACCESS_KEY="$key" FLYTE_BLOB_SECRET_KEY="$secret" \
  nohup ./.venv/bin/c0 --port 8000 --prometheus_port 9099 >/tmp/armada-flyte-c0.log 2>&1 &
for _ in $(seq 1 15); do
  grep -aq "armada (0)" /tmp/armada-flyte-c0.log 2>/dev/null && break
  sleep 1
done
echo "    connector ready (log: /tmp/armada-flyte-c0.log)"

echo "==> 3/3  Run a real @env.task through the backend"
exec "$PY" examples/backend_run.py
