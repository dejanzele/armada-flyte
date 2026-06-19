"""A real Python @env.task whose body runs inside an Armada pod.

Runs actual Python: `square` executes in the Armada-scheduled pod, reading its input and writing
its output through a shared blob store. The ergonomics are stock Flyte 2: set plugin_config on the
TaskEnvironment and every @env.task in it routes to Armada.

Two execution modes, selected by the same single block at the bottom:

  local (default)    `python examples/python_function.py`
                     Runs the submit-and-poll loop in this process. Prints the result. Needs a
                     blob store reachable from here and the pods (this discovers a host MinIO on
                     :9100). Use examples/run_local.sh, which sets that up.

  backend            `python examples/python_function.py --backend`  (or BACKEND=1)
                     Registers the task with a Flyte backend (flyte.run), so it shows up in the
                     Flyte UI. Use ./demo/run.sh, which wires the connector to the backend.
"""

from __future__ import annotations

import os
import socket
import sys


def _host_ip() -> str:
    """The host's primary LAN IP, reachable from both this process and the cluster pods."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


# Set the blob config BEFORE importing armada_flyte, so the local in-process connector picks it up.
# (For --backend the connector runs as a separate service, wired by demo/run.sh; this is harmless.)
os.environ.setdefault("FLYTE_BLOB_ENDPOINT", f"http://{_host_ip()}:9100")
os.environ.setdefault("FLYTE_BLOB_ACCESS_KEY", "minio")
os.environ.setdefault("FLYTE_BLOB_SECRET_KEY", "minio12345")

import flyte
import flyte.remote
import flyte.storage

from armada_flyte import ArmadaConfig

# A non-latest tag so the backend driver pod defaults to imagePullPolicy IfNotPresent and uses the
# locally loaded image. The runners (examples/run_local.sh, demo/run.sh) build and load this tag.
IMAGE = os.environ.get("ARMADA_TASK_IMAGE", "armada-flyte-task:v1")

env = flyte.TaskEnvironment(
    name="ml",
    image=IMAGE,
    plugin_config=ArmadaConfig(queue="flyte", cpu="500m", memory="512Mi"),
)


@env.task
async def square(x: int) -> int:
    return x * x


def _run_local() -> None:
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
    print(f"\nsquare(7) = {run.outputs()[0]}  (real Python, computed in an Armada pod)")


def _run_backend() -> None:
    flyte.init(endpoint="localhost:30080", insecure=True, project="flytesnacks", domain="development")
    run = flyte.run(square, x=7)
    print(f"\nsubmitted run {run.name}\n  UI: {run.url}")
    run.wait()
    result = flyte.remote.Run.get(run.name).outputs()[0]
    print(f"\nsquare(7) = {result}  (real Python, in an Armada pod, via the Flyte backend)")


if __name__ == "__main__":
    if "--backend" in sys.argv or os.environ.get("BACKEND"):
        _run_backend()
    else:
        _run_local()
