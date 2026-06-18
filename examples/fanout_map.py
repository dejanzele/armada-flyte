"""Fan-out / fan-in Flyte 2 DAG scheduled onto Armada.

A driver ``@env.task`` launches N ``ArmadaTask`` workers concurrently with ``asyncio.gather``
(the fan-out), then feeds their outputs into a single ``reduce`` ``ArmadaTask`` (the fan-in).
Every node - each worker and the reduce step - is a *real* Armada job, submitted and polled to
completion by the Armada connector. The DAG structure and data flow are real Flyte 2.

Run (with the Armada localdev stack up and reachable at $ARMADA_URL, default localhost:50051):

    cd armada-flyte
    ./.venv/bin/python examples/fanout_map.py
"""

from __future__ import annotations

import asyncio
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
FANOUT = int(os.environ.get("FANOUT", "4"))

# One worker task definition; the driver invokes it FANOUT times concurrently.
worker = ArmadaTask(
    name="worker",
    plugin_config=ArmadaConfig(
        queue=QUEUE,
        command=["sh", "-c", "echo 'worker running'; sleep 1"],
        output_template="worker[{shard}]=done (armada job {job_id})",
    ),
    inputs={"shard": str},
    outputs={"result": str},
)

# Fan-in: a single task that consumes the joined worker outputs.
reduce = ArmadaTask(
    name="reduce",
    plugin_config=ArmadaConfig(
        queue=QUEUE,
        command=["sh", "-c", "echo 'reducing'; sleep 1"],
        output_template="reduced {count} workers (armada job {job_id})",
    ),
    inputs={"count": str, "joined": str},
    outputs={"result": str},
)

# Connector tasks (image=None) live in their own environment.
armada_env = flyte.TaskEnvironment.from_task("armada-tasks", worker, reduce)

# The driver environment runs the DAG logic and depends on the Armada tasks.
driver_env = flyte.TaskEnvironment(name="fanout-armada", depends_on=[armada_env])


@driver_env.task
async def fanout(n: int = FANOUT) -> str:
    # Fan-out: launch n worker Armada jobs concurrently.
    results = await asyncio.gather(*(worker(shard=str(i)) for i in range(n)))
    # Fan-in: a single reduce Armada job over the joined worker outputs.
    final = await reduce(count=str(n), joined="; ".join(results))
    return final


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
    ).run(fanout, n=FANOUT)

    # ActionOutputs is a tuple subclass; [0] is the DAG's return value.
    result = run.outputs()[0]
    print(f"\n=== DAG result (fanned out across {FANOUT + 1} real Armada jobs) ===")
    print(result)
