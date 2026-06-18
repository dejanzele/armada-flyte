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

Two rules:

- Import `armada_flyte` before `armada_client`. This registers the connector and applies the proto shim.
- Keep the connector tasks (which have `image=None`) in their own `TaskEnvironment`, and have the driver environment `depends_on` it.

```python
import flyte
from armada_flyte import ArmadaConfig, ArmadaTask  # importing this registers the connector

step = ArmadaTask(
    name="step",
    plugin_config=ArmadaConfig(queue="my-queue", command=["sh", "-c", "echo hi"]),
    inputs={"x": str},
    outputs={"result": str},
)

armada_env = flyte.TaskEnvironment.from_task("armada", step)
wf_env = flyte.TaskEnvironment("wf", depends_on=[armada_env])

@wf_env.task
async def wf(x: str = "hi") -> str:
    return await step(x=x)

if __name__ == "__main__":
    flyte.init()
    run = flyte.with_runcontext(mode="local").run(wf, x="hi")
    print(run.outputs()[0])
```

`ArmadaConfig` controls each job: `queue`, `image`, `command`, `cpu`, `memory`, `priority`, and an
`output_template`.

To build a DAG:

- Call the same `ArmadaTask` several times to fan out.
- Give those calls a shared `gang_id` and `gang_cardinality` to schedule them as one Armada gang.

The [examples](examples/) cover linear, fan-out, and gang pipelines.

### 4. Run it

The examples run through Flyte local execution (`mode="local"`). That drives the submit-and-poll
loop in your process, so there is no backend to stand up.

The connector can also run as a gRPC service that a deployed Flyte backend routes to. See
[deploy/](deploy/).

## What works today, and what does not

Each node submits a real Armada job that is genuinely scheduled, run as a pod, and polled. You get:

- real Flyte 2 DAG topology and data flow between nodes,
- real Armada scheduling, including gang jobs,
- real per-node compute, when you opt in (see below).

By default a node's output is synthesised from its `output_template`. This keeps the basic
examples simple.

For real compute, set `capture_result=True`. The pod then:

- receives its inputs as environment variables,
- does the work and prints `ARMADA_RESULT:<value>`,
- has that value read back by the connector from the pod's logs.

`examples/pipeline.py` uses this to run a distributed sum across a gang.

You can also run **real Python functions**. Set `plugin_config=ArmadaConfig(...)` on a
`TaskEnvironment`, and every `@env.task` in it runs its body inside an Armada pod:

```python
env = flyte.TaskEnvironment("ml", image=img, plugin_config=ArmadaConfig(queue="compute"))

@env.task
async def square(x: int) -> int:
    return x * x      # runs in the Armada pod
```

Flyte ships the task's code and moves its inputs and outputs through a blob store that both your
process and the Armada pods can reach. See `examples/python_function.py` and
[docs/architecture.md](docs/architecture.md#real-python-tasks).

## Documentation

- [docs/architecture.md](docs/architecture.md) how the connector works, the state mapping, gang
  scheduling, and the current limits.
- [docs/getting-started.md](docs/getting-started.md) stand up a local Armada and run an example.
- [docs/gotchas.md](docs/gotchas.md) the non-obvious environment and proto issues.
- [deploy/](deploy/) run the connector as a gRPC service, or deploy it to a Flyte backend.

## License

Apache-2.0. See [LICENSE](LICENSE).
