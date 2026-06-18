# armada-flyte

A Flyte 2 connector that runs each Flyte task as an [Armada](https://github.com/armadaproject/armada)
job. You author a DAG in pure-Python [Flyte 2](https://github.com/flyteorg/flyte); each node is
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

You need a running Armada cluster. The connector submits jobs and polls their status over
Armada's gRPC API.

- Set `ARMADA_URL` (default `localhost:50051`).
- Make sure the queue you submit to exists.

To stand up a local Armada with a real executor on Kind, from a checkout of the
[armada](https://github.com/armadaproject/armada) repo run `mage Kind` then `mage dev:up no-auth`.
Full walkthrough in [docs/getting-started.md](docs/getting-started.md).

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

To fan out, call the same `ArmadaTask` several times in a DAG. To schedule those calls as one
Armada gang, give them a shared `gang_id` and `gang_cardinality`.

The [examples](examples/) cover linear, fan-out, and gang pipelines.

### 4. Run it

The examples run through Flyte local execution (`mode="local"`), which drives the submit-and-poll
loop in your process, so there is no backend to stand up.

The connector can also run as a gRPC service that a deployed Flyte backend routes to, instead of
local execution. See [deploy/](deploy/).

## What works today, and what does not

Each node submits a real Armada job and is genuinely scheduled, run as a pod, and polled. You get:

- real Flyte 2 DAG topology and data flow between nodes,
- real Armada scheduling, including gang jobs,
- real per-node compute, when you opt in: with `capture_result=True` the pod does actual work
  (its inputs arrive as env vars), prints `ARMADA_RESULT:<value>`, and the connector reads that
  back from the pod's logs. See `examples/pipeline.py` (a distributed sum across a gang).

By default a node's output is synthesised from `output_template` rather than the workload. This
keeps the basic examples simple.

One thing is not supported yet: running an arbitrary Python function as the body of a node, which
would mean shipping the task's code and moving large inputs and outputs through a blob store. See
the [limitations and next steps](docs/architecture.md#limitations-and-next-steps).

## Documentation

- [docs/architecture.md](docs/architecture.md) how the connector works, the state mapping, gang
  scheduling, and the current limits.
- [docs/getting-started.md](docs/getting-started.md) stand up a local Armada and run an example.
- [docs/gotchas.md](docs/gotchas.md) the non-obvious environment and proto issues.
- [deploy/](deploy/) run the connector as a gRPC service, or deploy it to a Flyte backend.

## License

Apache-2.0. See [LICENSE](LICENSE).
