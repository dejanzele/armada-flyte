"""Hello-world Flyte 2 DAG scheduled onto Armada.

Two tasks, run as a DAG (``shout`` depends on ``hello``). Each node is an ``ArmadaTask``:
its execution is a *real* Armada job, submitted and polled to completion by the Armada
connector. The DAG structure and data flow (hello's output into shout's input) are real Flyte 2.

Run (with the Armada localdev stack up and reachable at $ARMADA_URL, default localhost:50051):

    cd flyte-armada-connector
    PYTHONPATH=src ./.venv/bin/python examples/hello_world_dag.py
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

hello = ArmadaTask(
    name="hello",
    plugin_config=ArmadaConfig(
        queue=QUEUE,
        command=["sh", "-c", "echo 'hello from armada'; sleep 1"],
        output_template="Hello, {name}! (ran as armada job {job_id})",
    ),
    inputs={"name": str},
    outputs={"result": str},
)

shout = ArmadaTask(
    name="shout",
    plugin_config=ArmadaConfig(
        queue=QUEUE,
        command=["sh", "-c", "echo 'shouting'; sleep 1"],
        output_template="SHOUT<{text}> (ran as armada job {job_id})",
    ),
    inputs={"text": str},
    outputs={"result": str},
)

# Connector tasks (image=None) live in their own environment.
armada_env = flyte.TaskEnvironment.from_task("armada-tasks", hello, shout)

# The driver environment runs the DAG logic and depends on the Armada tasks.
driver_env = flyte.TaskEnvironment(name="hello-armada", depends_on=[armada_env])


@driver_env.task
async def dag(name: str = "world") -> str:
    greeting = await hello(name=name)
    loud = await shout(text=greeting)
    return loud


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
        raw_data_path=tempfile.mkdtemp(prefix="flyte-armada-"),
    ).run(dag, name="world")

    # ActionOutputs is a tuple subclass; [0] is the DAG's return value.
    result = run.outputs()[0]
    print("\n=== DAG result (flowed through 2 real Armada jobs) ===")
    print(result)
