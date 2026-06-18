"""A Flyte 2 to Armada pipeline with data sharing and a gang of workers.

Topology:

    generate              (job 1) produces a dataset descriptor
        |
     +--+--+
     |  |  |
    w0 w1 w2              (jobs 2, 3, 4) a GANG of 3 workers, one shard each
     |  |  |
     +--+--+
        |
    aggregate             (job 5) combines the three workers' outputs

What is real here: the DAG topology, the data threaded between nodes (generate's output is fed
to every worker, and the workers' outputs are fed to aggregate), and Armada gang scheduling.
The three worker jobs share an Armada gangId and are scheduled all-or-nothing together.

What is a placeholder here: each node runs a real Armada job, but the workload is a stand-in
(echo). The node's output is synthesised by the connector from its output_template and inputs,
not computed inside the pod. For real in-pod compute, see pipeline.py.

Run (Armada cluster reachable at $ARMADA_URL, default localhost:50051):

    ./.venv/bin/python examples/gang_pipeline.py
"""

from __future__ import annotations

import asyncio
import os
import tempfile

import grpc
import flyte

# Import armada_flyte before armada_client (proto-compat shim + connector registration).
from armada_flyte import ArmadaConfig, ArmadaTask
from armada_client.client import ArmadaClient

ARMADA_URL = os.environ.get("ARMADA_URL", "localhost:50051")
QUEUE = os.environ.get("ARMADA_QUEUE", "flyte")
SHARDS = 3

# job 1: generate the dataset.
generate = ArmadaTask(
    name="generate",
    plugin_config=ArmadaConfig(
        queue=QUEUE,
        command=["sh", "-c", "echo 'generating dataset'; sleep 1"],
        output_template="dataset-{job_id}[{count} records]",
    ),
    inputs={"count": str},
    outputs={"result": str},
)

# jobs 2, 3, 4: a gang of workers. Every invocation shares gang_id + gang_cardinality, so the
# three jobs form one Armada gang and are scheduled together. Each receives the same dataset
# (fan-out of generate's output) plus its own shard index.
worker = ArmadaTask(
    name="worker",
    plugin_config=ArmadaConfig(
        queue=QUEUE,
        command=["sh", "-c", "echo 'processing shard'; sleep 1"],
        gang_id="pipeline-workers",
        gang_cardinality=SHARDS,
        output_template="shard {shard} of {dataset} done by {job_id}",
    ),
    inputs={"shard": str, "dataset": str},
    outputs={"result": str},
)

# job 5: aggregate the workers' outputs (fan-in).
aggregate = ArmadaTask(
    name="aggregate",
    plugin_config=ArmadaConfig(
        queue=QUEUE,
        command=["sh", "-c", "echo 'aggregating'; sleep 1"],
        output_template="aggregated {n} shards by {job_id}: {parts}",
    ),
    inputs={"n": str, "parts": str},
    outputs={"result": str},
)

# Connector tasks (image=None) share one environment; the driver depends on it.
armada_env = flyte.TaskEnvironment.from_task("armada-tasks", generate, worker, aggregate)
driver_env = flyte.TaskEnvironment(name="gang-pipeline", depends_on=[armada_env])


@driver_env.task
async def pipeline(records: int = 9) -> str:
    dataset = await generate(count=str(records))

    # Fan out to the gang: all workers run concurrently and each sees the same dataset.
    parts = await asyncio.gather(
        *(worker(shard=str(i), dataset=dataset) for i in range(SHARDS))
    )

    return await aggregate(n=str(SHARDS), parts=" | ".join(parts))


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
    ).run(pipeline, records=9)

    print(f"\n=== pipeline result (5 real Armada jobs, workers scheduled as a gang of {SHARDS}) ===")
    print(run.outputs()[0])
