#!/usr/bin/env bash
# Run a real multi-stage Python pipeline (examples/python_pipeline.py) on Armada, end to end,
# with one command. Each @env.task runs its actual Python body in an Armada pod.
#
# Prerequisite: a running Armada localdev stack with a real executor on a kind cluster
# (default cluster name: armada-test). This script handles everything else: a MinIO blob store,
# the task image, loading it into the cluster, and running the pipeline.
#
#   ./examples/run_python_pipeline.sh
#
# Override the kind cluster name with KIND_CLUSTER=... if yours differs.
set -euo pipefail

cd "$(dirname "$0")/.."
PY=./.venv/bin/python
KIND_CLUSTER="${KIND_CLUSTER:-armada-test}"
IMAGE=armada-flyte-task:latest
BLOB_PORT=9100

echo "==> 1/4  MinIO blob store on the host (:${BLOB_PORT})"
if ! docker ps --format '{{.Names}}' | grep -qx minio; then
  docker run -d --name minio -p ${BLOB_PORT}:9000 \
    -e MINIO_ROOT_USER=minio -e MINIO_ROOT_PASSWORD=minio12345 \
    minio/minio:latest server /data >/dev/null
  sleep 5
fi
docker run --rm --network host --entrypoint sh minio/mc:latest \
  -c "mc alias set h http://localhost:${BLOB_PORT} minio minio12345 && mc mb -p h/flyte" \
  >/dev/null 2>&1 || true

echo "==> 2/4  Build the task image (flyte + armada_flyte)"
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

echo "==> 3/4  Load the image into the kind cluster (${KIND_CLUSTER})"
kind load docker-image "${IMAGE}" --name "${KIND_CLUSTER}" >/dev/null

echo "==> 4/4  Run the pipeline (driver runs locally; stages run as Armada pods)"
exec "${PY}" examples/python_pipeline.py
