"""A real Python @env.task that runs its body inside an Armada pod.

Unlike the other examples (which run placeholder workloads), this runs actual Python: `square`
executes in the Armada-scheduled pod, reading its input and writing its output through a shared
blob store. The ergonomics are stock Flyte 2: set plugin_config on the TaskEnvironment and every
@env.task in it routes to Armada.

Requires (beyond an Armada cluster) a blob store reachable from BOTH this process and the Armada
pods. This example points at a MinIO on the host (bound to 0.0.0.0:9100). It discovers the host's
LAN IP at runtime, which is the one address both the host and the in-cluster pods can reach. Run:

    ./.venv/bin/python examples/python_function.py
"""

from __future__ import annotations

import os
import socket


def _host_ip() -> str:
    """The host's primary LAN IP, reachable from both this process and the cluster pods."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()

# Set the blob config BEFORE importing armada_flyte, so the connector (created at import) picks it
# up and injects it into the Armada pods.
os.environ.setdefault("FLYTE_BLOB_ENDPOINT", f"http://{_host_ip()}:9100")
os.environ.setdefault("FLYTE_BLOB_ACCESS_KEY", "minio")
os.environ.setdefault("FLYTE_BLOB_SECRET_KEY", "minio12345")

import flyte
import flyte.storage

from armada_flyte import ArmadaConfig

image = os.environ.get("ARMADA_TASK_IMAGE", "armada-flyte-task:latest")

env = flyte.TaskEnvironment(
    name="ml",
    image=image,
    plugin_config=ArmadaConfig(queue="flyte", cpu="500m", memory="512Mi"),
)


@env.task
async def square(x: int) -> int:
    return x * x


if __name__ == "__main__":
    flyte.init(
        storage=flyte.storage.S3(
            endpoint=os.environ["FLYTE_BLOB_ENDPOINT"],
            access_key_id=os.environ["FLYTE_BLOB_ACCESS_KEY"],
            secret_access_key=os.environ["FLYTE_BLOB_SECRET_KEY"],
            region="us-east-1",
            addressing_style="path",
        ),
    )
    # raw_data_path must be remote (s3://) so the code bundle and inputs go to the blob store the
    # Armada pod reads from, not a local temp dir.
    run = flyte.with_runcontext(mode="local", raw_data_path="s3://flyte/raw").run(square, x=7)
    print("\n=== result (square(7) computed inside an Armada pod) ===")
    print(run.outputs())
