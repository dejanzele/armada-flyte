"""Generate, fan out to a gang of workers, aggregate. Every node runs real Python in an Armada pod.

Topology:

    generate                (job 1) produces the dataset
        |
     +--+--+
     |  |  |
    w0 w1 w2                (jobs 2, 3, 4) a GANG of 3 workers, one shard each
     |  |  |
     +--+--+
        |
    aggregate               (job 5) sums the three partial results

The three workers are a real Armada gang: they share a gangId and are scheduled all-or-nothing
together. That comes from putting them in their own TaskEnvironment whose plugin_config carries
gang_id and gang_cardinality, so each of the three calls submits as one member of the gang. The
generate and aggregate nodes are in a separate (non-gang) environment.

Data flows through the blob store like any Flyte run, so this needs the same setup as the other
real-Python examples (a shared blob store and the task image in the cluster). Run:

    ./.venv/bin/python examples/gang_dag.py
"""

from __future__ import annotations

import asyncio
import os
import socket


def _host_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


# Blob config must be set before importing armada_flyte (the connector reads it at import).
os.environ.setdefault("FLYTE_BLOB_ENDPOINT", f"http://{_host_ip()}:9100")
os.environ.setdefault("FLYTE_BLOB_ACCESS_KEY", "minio")
os.environ.setdefault("FLYTE_BLOB_SECRET_KEY", "minio12345")

import flyte
import flyte.storage

from armada_flyte import ArmadaConfig

IMAGE = os.environ.get("ARMADA_TASK_IMAGE", "armada-flyte-task:latest")
GANG = 3

# generate and aggregate: ordinary Armada tasks.
io_env = flyte.TaskEnvironment(
    name="io",
    image=IMAGE,
    plugin_config=ArmadaConfig(queue="flyte", cpu="500m", memory="512Mi"),
)

# the three workers: a gang. Sharing gang_id + gang_cardinality makes the three submissions one
# Armada gang, scheduled all-or-nothing together.
gang_env = flyte.TaskEnvironment(
    name="calc",
    image=IMAGE,
    plugin_config=ArmadaConfig(
        queue="flyte",
        cpu="500m",
        memory="512Mi",
        gang_id="calc-gang",
        gang_cardinality=GANG,
    ),
)

driver = flyte.TaskEnvironment(name="driver", depends_on=[io_env, gang_env])


@io_env.task
async def generate(n: int, seed: int) -> list[int]:
    import random

    rng = random.Random(seed)
    return [rng.randint(1, 100) for _ in range(n)]


@gang_env.task
async def partial_sum(shard: list[int]) -> int:
    return sum(shard)


@io_env.task
async def total(parts: list[int]) -> int:
    return sum(parts)


@driver.task
async def pipeline(n: int = 90) -> int:
    data = await generate(n=n, seed=1)
    shards = [data[i::GANG] for i in range(GANG)]
    parts = await asyncio.gather(*(partial_sum(shard=s) for s in shards))
    return await total(parts=list(parts))


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
    run = flyte.with_runcontext(mode="local", raw_data_path="s3://flyte/raw").run(pipeline, n=90)
    print(f"\n=== sum of 90 numbers, computed by a gang of {GANG} Armada pods ===")
    print("total =", run.outputs()[0])
