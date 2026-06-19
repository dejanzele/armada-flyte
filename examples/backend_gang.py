"""The gang DAG, run through a Flyte backend so it shows up in the Flyte UI.

Same topology as gang_dag.py (generate, a gang of 3 workers, aggregate), but registered with
FlyteAdmin via flyte.run() instead of local execution. The driver task runs in the backend
cluster and orchestrates; the generate/worker/aggregate tasks are routed to Armada.

Run it with one command:  ./demo/run.sh examples/backend_gang.py
"""

from __future__ import annotations

import asyncio

import flyte
import flyte.remote

from armada_flyte import ArmadaConfig

IMAGE = "armada-flyte-task:v1"
GANG = 3

io_env = flyte.TaskEnvironment(
    name="io",
    image=IMAGE,
    plugin_config=ArmadaConfig(queue="flyte", cpu="500m", memory="512Mi"),
)
gang_env = flyte.TaskEnvironment(
    name="calc",
    image=IMAGE,
    plugin_config=ArmadaConfig(
        queue="flyte", cpu="500m", memory="512Mi", gang_id="calc-gang", gang_cardinality=GANG
    ),
)
# The driver orchestrates; in backend execution it runs as a pod in the backend cluster.
driver = flyte.TaskEnvironment(name="driver", image=IMAGE, depends_on=[io_env, gang_env])


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
    flyte.init(endpoint="localhost:30080", insecure=True, project="flytesnacks", domain="development")
    run = flyte.run(pipeline, n=90)
    print(f"\nsubmitted run {run.name}")
    print(f"  UI: {run.url}")
    run.wait()
    print(f"\ntotal = {flyte.remote.Run.get(run.name).outputs()[0]}  (gang of {GANG} on Armada, via the backend)")
