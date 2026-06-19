# armada-flyte

A Flyte 2 connector that runs each Flyte task as an [Armada](https://github.com/armadaproject/armada) job.

You author a DAG in pure-Python [Flyte 2](https://github.com/flyteorg/flyte). Each node is then
submitted to an Armada queue, scheduled by Armada, and polled to completion.

## Use it in your own Flyte 2 workflows

### 1. Install

Install it straight from the repository:

```bash
pip install "armada-flyte @ git+https://github.com/armadaproject/armada-flyte.git"
```

> On Apple Silicon, use an arm64 Python.
> An x86_64 interpreter cannot load Flyte's native `obstore` wheel (see [docs/gotchas.md](docs/gotchas.md)).

### 2. Point it at an Armada

You need a running Armada cluster, reachable over its gRPC API.

- Set `ARMADA_URL` (default `localhost:50051`).
- Make sure the queue you submit to exists.

To stand up a local Armada with a real executor on Kind, run these from a checkout of the
[armada](https://github.com/armadaproject/armada) repo:

```bash
mage Kind            # create the Kind cluster
mage dev:up no-auth  # start Armada with a real executor
```

Full walkthrough: [docs/getting-started.md](docs/getting-started.md).

### 3. Write a workflow

Write ordinary Flyte 2 tasks. Set `plugin_config=ArmadaConfig(...)` on the environment, and every
`@env.task` in it runs its real Python body inside an Armada pod:

```python
import flyte
from armada_flyte import ArmadaConfig  # importing this registers the connector

env = flyte.TaskEnvironment(
    name="ml",
    image="armada-flyte-task:latest",          # an image with flyte + your deps, on the cluster
    plugin_config=ArmadaConfig(queue="my-queue"),
)

@env.task
async def square(x: int) -> int:
    return x * x                                # runs in the Armada pod

if __name__ == "__main__":
    flyte.init(storage=...)                     # a blob store you and the pods both reach
    print(flyte.with_runcontext(mode="local").run(square, x=7).outputs())
```

That is the whole integration: a stock `@env.task`, plus one `plugin_config` line. Fan out with
`asyncio.gather`, pass typed data (dataclasses, lists) between tasks, and it all runs on Armada.

This needs a shared blob store and the task image in the cluster.
[`examples/run_python_pipeline.sh`](examples/run_python_pipeline.sh) sets both up and runs a
multi-stage map-reduce in **one command**; see
[docs/architecture.md](docs/architecture.md#real-python-tasks) for how it works.

> Prefer no blob store? A lower-level `ArmadaTask` runs shell workloads directly (with optional
> gang scheduling and a `capture_result` mode). See the [examples](examples/).

### 4. Run it

The examples run through Flyte local execution (`mode="local"`). That drives the submit-and-poll
loop in your process, so there is no backend to stand up.

To run on a **real Flyte backend** (the task registers with FlyteAdmin and shows up in the Flyte
UI), use the one-command demo:

```bash
./demo/run.sh
```

It runs an ordinary `@env.task` on Armada through the backend and prints the result and UI link.
See [demo/](demo/) for what it does and the one-time prerequisites.

## What works today

The full Flyte 2 authoring experience, running on Armada:

- **Real Python tasks** (shown above): each `@env.task` runs its body in an Armada pod, with typed
  inputs and outputs shipped through the blob store. Fan-out, dataclasses, and multi-stage
  pipelines all work, since each task is a self-contained Flyte container the connector just runs.
- **Real Armada scheduling**, including gang jobs: give tasks a shared `gang_id` and they are
  scheduled all-or-nothing together.
- **Two execution modes**: Flyte local execution (no backend to stand up), or as a gRPC service a
  deployed Flyte backend routes to (see [deploy/](deploy/)).

Don't want to run a blob store? The lower-level `ArmadaTask` runs shell workloads directly. Its
output is synthesised from an `output_template` by default; set `capture_result=True` to have the
pod print `ARMADA_RESULT:<value>` and the connector read it back from the pod's logs.
`examples/pipeline.py` runs a distributed sum this way.

## Documentation

- [docs/architecture.md](docs/architecture.md) how the connector works, the state mapping, gang
  scheduling, and the current limits.
- [docs/getting-started.md](docs/getting-started.md) stand up a local Armada and run an example.
- [docs/gotchas.md](docs/gotchas.md) the non-obvious environment and proto issues.
- [deploy/](deploy/) run the connector as a gRPC service, or deploy it to a Flyte backend.

## License

Apache-2.0. See [LICENSE](LICENSE).
