"""A real distributed sum: generate then a gang of workers then aggregate.

Same shape as gang_pipeline.py, but the pods do actual work instead of a placeholder. Each
node's inputs are passed into its pod as environment variables, the pod computes a result with
real shell arithmetic and prints ``ARMADA_RESULT:<value>``, and the connector reads that line
back from the pod's logs (set on the task with ``capture_result=True``).

    generate           prints the numbers 1..N as a CSV
        |
     +--+--+
     w0 w1 w2          gang of 3 workers; worker i sums numbers at positions i, i+3, i+6, ... (0-based)
     +--+--+
        |
    aggregate          sums the three partial sums

For N=9 the workers compute partial sums of {1,4,7}, {2,5,8}, {3,6,9} and aggregate returns 45.
The result is produced by the pods, not synthesised, so the printed total is real.

Run (Armada cluster up at $ARMADA_URL, default localhost:50051):

    ./.venv/bin/python examples/pipeline.py
"""

from __future__ import annotations

import asyncio
import os
import tempfile

import grpc
import flyte

from armada_flyte import ArmadaConfig, ArmadaTask
from armada_client.client import ArmadaClient

ARMADA_URL = os.environ.get("ARMADA_URL", "localhost:50051")
QUEUE = os.environ.get("ARMADA_QUEUE", "flyte")
SHARDS = 3

# generate: emit 1..COUNT as a CSV. seq is available in busybox.
generate = ArmadaTask(
    name="generate",
    plugin_config=ArmadaConfig(
        queue=QUEUE,
        command=["sh", "-c", "echo ARMADA_RESULT:$(seq -s, 1 $COUNT)"],
        capture_result=True,
    ),
    inputs={"count": str},
    outputs={"result": str},
)

# worker: sum the shard of NUMBERS at positions SHARD, SHARD+SHARDS, ... (busybox awk).
worker = ArmadaTask(
    name="worker",
    plugin_config=ArmadaConfig(
        queue=QUEUE,
        command=["sh", "-c",
                 "echo ARMADA_RESULT:$(echo $NUMBERS | tr ',' '\\n' | "
                 "awk -v s=$SHARD -v n=$SHARDS '(NR-1)%n==s{t+=$1} END{print t+0}')"],
        gang_id="real-workers",
        gang_cardinality=SHARDS,
        capture_result=True,
    ),
    inputs={"numbers": str, "shard": str, "shards": str},
    outputs={"result": str},
)

# aggregate: sum the comma-separated partial sums.
aggregate = ArmadaTask(
    name="aggregate",
    plugin_config=ArmadaConfig(
        queue=QUEUE,
        command=["sh", "-c",
                 "echo ARMADA_RESULT:$(echo $PARTS | tr ',' '\\n' | awk '{t+=$1} END{print t+0}')"],
        capture_result=True,
    ),
    inputs={"parts": str},
    outputs={"result": str},
)

armada_env = flyte.TaskEnvironment.from_task("armada-tasks", generate, worker, aggregate)
driver_env = flyte.TaskEnvironment(name="real-pipeline", depends_on=[armada_env])


@driver_env.task
async def pipeline(count: int = 9) -> str:
    numbers = await generate(count=str(count))

    # Shard ids are 0..SHARDS-1; the worker matches positions where (NR-1) % SHARDS == shard.
    partials = await asyncio.gather(
        *(worker(numbers=numbers, shard=str(i), shards=str(SHARDS)) for i in range(SHARDS))
    )

    total = await aggregate(parts=",".join(partials))
    return f"sum(1..{count}) computed on Armada = {total}  (partial sums: {partials})"


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
    ).run(pipeline, count=9)

    print("\n=== real pipeline result (computed by the pods, read from their logs) ===")
    print(run.outputs()[0])
