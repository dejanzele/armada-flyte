"""The simplest possible Flyte 2 to Armada example: ONE task, no DAG.

A single ``ArmadaTask`` is submitted to Armada and polled to completion by the Armada
connector. There is no driver function and no dependency graph: ``flyte local run`` executes
the one task directly. Its execution is a *real* Armada job (a placeholder ``echo`` workload),
and the connector synthesises the task's ``result`` output from ``output_template`` + inputs.

Run (with the Armada localdev stack up and reachable at $ARMADA_URL, default localhost:50051):

    cd armada-flyte
    ./.venv/bin/python examples/single_task.py
"""

from __future__ import annotations

import os
import tempfile

import grpc
import flyte

# Import armada_flyte BEFORE armada_client: its __init__ runs the proto-compat shim that
# prevents a duplicate google/api/http.proto registration, and registers the connector.
from armada_flyte import ArmadaConfig, ArmadaTask
from armada_client.client import ArmadaClient

ARMADA_URL = os.environ.get("ARMADA_URL", "localhost:50051")
QUEUE = os.environ.get("ARMADA_QUEUE", "flyte")

greet = ArmadaTask(
    name="greet",
    plugin_config=ArmadaConfig(
        queue=QUEUE,
        command=["sh", "-c", "echo 'hello from armada'; sleep 1"],
        output_template="Hello, {name}! (ran as armada job {job_id})",
    ),
    inputs={"name": str},
    outputs={"result": str},
)


def ensure_queue(name: str) -> None:
    client = ArmadaClient(grpc.insecure_channel(ARMADA_URL))
    try:
        client.create_queue(client.create_queue_request(name=name, priority_factor=1))
        print(f"[setup] created Armada queue '{name}'")
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.ALREADY_EXISTS:
            print(f"[setup] Armada queue '{name}' already exists")
        else:
            raise


if __name__ == "__main__":
    ensure_queue(QUEUE)
    flyte.init()
    run = flyte.with_runcontext(
        mode="local",
        raw_data_path=tempfile.mkdtemp(prefix="armada-flyte-"),
    ).run(greet, name="world")

    # ActionOutputs is a tuple subclass; [0] is the task's return value.
    result = run.outputs()[0]
    print("\n=== task result (ran as 1 real Armada job) ===")
    print(result)
