"""Hello world: the smallest Armada task. One @env.task, one line of Armada config.

    ./demo/run.sh examples/hello.py                  # the default: runs on Armada, shows in the Flyte UI
    ./examples/run_local.sh examples/hello.py        # also available: a local run for fast iteration
"""

from __future__ import annotations

import os

import flyte
from armada_flyte import ArmadaConfig

IMAGE = os.environ.get("ARMADA_TASK_IMAGE", "armada-flyte-task:v1")

env = flyte.TaskEnvironment(
    name="hello",
    image=IMAGE,
    resources=flyte.Resources(cpu=1, memory="512Mi"),
    plugin_config=ArmadaConfig(queue="flyte"),   # the one Armada-specific line
)


@env.task
async def greet(name: str) -> str:
    return f"hello {name}, from an Armada pod"


if __name__ == "__main__":
    from _runner import run

    print("\n" + run(greet, name="armada"))
