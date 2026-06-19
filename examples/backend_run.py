"""Run a real Python @env.task through a deployed Flyte backend (not local execution).

This is the "proper" path: the task is registered with FlyteAdmin and shows up in the Flyte UI.
FlytePropeller routes the `armada` task to the connector service, which submits it to Armada; the
pod runs the function and writes its real typed output, which the backend records.

Prerequisites (beyond an Armada cluster):
  - a Flyte 2 backend whose executor routes `armada` tasks to the connector (the connector plugin
    must be registered in the executor),
  - the connector running as a service (`c0`), pointed at a blob store the Armada pods can reach,
    via FLYTE_BLOB_ENDPOINT / FLYTE_BLOB_ACCESS_KEY / FLYTE_BLOB_SECRET_KEY (this must be the same
    store the backend uses, reachable from the Armada cluster),
  - the task image available on the Armada cluster.

Run:  ./.venv/bin/python examples/backend_run.py
"""

from __future__ import annotations

import flyte

from armada_flyte import ArmadaConfig

env = flyte.TaskEnvironment(
    name="ml",
    image="armada-flyte-task:latest",
    plugin_config=ArmadaConfig(queue="flyte", cpu="500m", memory="512Mi"),
)


@env.task
async def square(x: int) -> int:
    return x * x


if __name__ == "__main__":
    flyte.init(endpoint="localhost:30080", insecure=True, project="flytesnacks", domain="development")
    run = flyte.run(square, x=7)
    print("run:", run.name)
    print("url:", run.url)
    run.wait()
    print("done; see the result in the Flyte UI")
