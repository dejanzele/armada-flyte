"""A real multi-stage Python pipeline whose every stage runs inside an Armada pod.

This is stock Flyte 2: ordinary `@env.task` functions, real Python bodies, typed data (lists and
dataclasses) flowing between stages. The only Armada-specific thing is `plugin_config=ArmadaConfig`
on the environment, which routes each task to Armada. Flyte handles code shipping and the
inputs/outputs (through the blob store); the connector just runs the rendered container as an
Armada job.

    generate(n)          -> n pseudo-random integers
    split into S shards
    shard_stats(chunk)   -> per-shard Stats, computed in S parallel Armada pods (fan-out)
    merge(parts)         -> combined Stats (fan-in)

Run it with one command: examples/run_local.sh
"""

from __future__ import annotations

import asyncio
import os
import socket
from dataclasses import dataclass


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

# Worker tasks run on Armada; the driver runs locally and orchestrates them.
work = flyte.TaskEnvironment(
    name="stats",
    image=IMAGE,
    plugin_config=ArmadaConfig(queue="flyte", cpu="500m", memory="512Mi"),
)
driver = flyte.TaskEnvironment(name="driver", depends_on=[work])


@dataclass
class Stats:
    count: int
    total: int
    lo: int
    hi: int


@work.task
async def generate(n: int, seed: int) -> list[int]:
    import random

    rng = random.Random(seed)
    return [rng.randint(1, 1000) for _ in range(n)]


@work.task
async def shard_stats(chunk: list[int]) -> Stats:
    return Stats(count=len(chunk), total=sum(chunk), lo=min(chunk), hi=max(chunk))


@work.task
async def merge(parts: list[Stats]) -> Stats:
    return Stats(
        count=sum(p.count for p in parts),
        total=sum(p.total for p in parts),
        lo=min(p.lo for p in parts),
        hi=max(p.hi for p in parts),
    )


@driver.task
async def pipeline(n: int = 2000, shards: int = 4) -> Stats:
    data = await generate(n=n, seed=42)
    chunks = [data[i::shards] for i in range(shards)]
    parts = await asyncio.gather(*(shard_stats(chunk=c) for c in chunks))
    return await merge(parts=list(parts))


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
    run = flyte.with_runcontext(mode="local", raw_data_path="s3://flyte/raw").run(
        pipeline, n=2000, shards=4
    )
    result: Stats = run.outputs()[0]
    print("\n=== pipeline result (computed across parallel Armada pods) ===")
    print(result)
    print(f"mean = {result.total / result.count:.2f}")
