#!/usr/bin/env bash
# Run a real-Python example on Armada via local execution, in one command. Sets up the blob store
# (a host MinIO) and the task image the pods need, then runs the example.
#
#   ./examples/run_local.sh                              # default: examples/python_pipeline.py
#   ./examples/run_local.sh examples/python_function.py
#   ./examples/run_local.sh examples/gang_dag.py
#
# Prerequisite: a running Armada localdev stack with a real executor on a kind cluster
# (default cluster: armada-test). For backend execution (Flyte UI) see ./demo/run.sh instead.
set -euo pipefail

cd "$(dirname "$0")/.."
PY=./.venv/bin/python
EXAMPLE="${1:-examples/python_pipeline.py}"
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
exec "${PY}" "${EXAMPLE}"
