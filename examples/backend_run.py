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

Run it with one command (which also wires the connector to the backend's blob store):

    ./demo/run.sh
"""

from __future__ import annotations

import flyte
import flyte.remote

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
    print(f"\nsubmitted run {run.name}")
    print(f"  UI: {run.url}")
    run.wait()
    result = flyte.remote.Run.get(run.name).outputs()[0]
    print(f"\nsquare(7) = {result}  (real Python, computed in an Armada pod, via the Flyte backend)")
