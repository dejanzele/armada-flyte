#!/usr/bin/env bash
# Run a real-Python example on Armada via local execution, in one command. Sets up the blob store
# (a host MinIO) and the task image the pods need, then runs the example.
#
#   ./examples/run_local.sh                              # default: examples/function.py
#   ./examples/run_local.sh examples/fanout.py
#   ./examples/run_local.sh examples/ml_pipeline.py
#
# Prerequisite: a running Armada localdev stack with a real executor on a kind cluster
# (default cluster: armada-test). For backend execution (Flyte UI) see ./demo/run.sh instead.
set -euo pipefail

cd "$(dirname "$0")/.."
PY=./.venv/bin/python
EXAMPLE="${1:-examples/function.py}"
KIND_CLUSTER="${KIND_CLUSTER:-armada-test}"
IMAGE=armada-flyte-task:v1
BLOB_PORT=9100

echo "==> 1/3  MinIO blob store on the host (:${BLOB_PORT})"
if ! docker ps --format '{{.Names}}' | grep -qx minio; then
  docker run -d --name minio -p ${BLOB_PORT}:9000 \
    -e MINIO_ROOT_USER=minio -e MINIO_ROOT_PASSWORD=minio12345 \
    minio/minio:latest server /data >/dev/null
  sleep 5
fi
docker run --rm --network host --entrypoint sh minio/mc:latest \
  -c "mc alias set h http://localhost:${BLOB_PORT} minio minio12345 && mc mb -p h/flyte" \
  >/dev/null 2>&1 || true

echo "==> 2/3  Build the task image and load it into the Armada cluster (${KIND_CLUSTER})"
BUILD="$(mktemp -d)"
mkdir -p "${BUILD}/pkg"
cp -R pyproject.toml src "${BUILD}/pkg/"
cat > "${BUILD}/Dockerfile" <<'EOF'
FROM python:3.11-slim
RUN pip install --no-cache-dir "flyte==2.5.1"
COPY pkg /pkg
RUN pip install --no-cache-dir /pkg
EOF
docker build -t "${IMAGE}" "${BUILD}" >/dev/null
rm -rf "${BUILD}"
kind load docker-image "${IMAGE}" --name "${KIND_CLUSTER}" >/dev/null

echo "==> 3/3  Run ${EXAMPLE} (local execution)"
# The blob store both this process and the Armada pods use. The host LAN IP is the one address both
# reach. The example reads these (the in-process connector at import, the client at flyte.init).
HOST_IP="${HOST_IP:-$(ipconfig getifaddr en0 2>/dev/null || hostname -I | awk '{print $1}')}"
export FLYTE_BLOB_ENDPOINT="http://${HOST_IP}:${BLOB_PORT}"
export FLYTE_BLOB_ACCESS_KEY=minio
export FLYTE_BLOB_SECRET_KEY=minio12345
exec "${PY}" "${EXAMPLE}"
