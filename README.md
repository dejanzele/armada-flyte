# armada-flyte

A Flyte 2 connector that runs each Flyte task as an [Armada](https://github.com/armadaproject/armada)
job. You author a DAG in pure-Python [Flyte 2](https://github.com/flyteorg/flyte); each node is
submitted to an Armada queue, scheduled by Armada, and polled to completion.

## Use it in your own Flyte 2 workflows

### 1. Install

This is a private repo, so install it as a git dependency (with your SSH key or a token):

```bash
pip install "armada-flyte @ git+ssh://git@github.com/<org>/armada-flyte.git"
```

On Apple Silicon, use an arm64 Python. An x86_64 interpreter cannot load Flyte's native
`obstore` wheel (see [docs/gotchas.md](docs/gotchas.md)).

### 2. Point it at an Armada

You need a running Armada cluster. The connector submits jobs and polls their status over
Armada's gRPC API.

- Set `ARMADA_URL` (default `localhost:50051`).
- Make sure the queue you submit to exists.

To stand up a local Armada with a real executor on Kind, from a checkout of the
[armada](https://github.com/armadaproject/armada) repo run `mage Kind` then `mage dev:up no-auth`.
Full walkthrough in [docs/getting-started.md](docs/getting-started.md).

### 3. Write a workflow

Two rules: import `armada_flyte` before `armada_client`, and keep the connector tasks (which
have `image=None`) in their own `TaskEnvironment`, with the driver environment depending on it.

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

`ArmadaConfig` controls each job: `queue`, `image`, `command`, `cpu`, `memory`, `priority`, an
`output_template`, and gang scheduling via `gang_id` / `gang_cardinality`. Call the same
`ArmadaTask` several times in a DAG to fan out; give the workers a shared `gang_id` to schedule
them as an Armada gang. The [examples](examples/) cover linear, fan-out, and gang pipelines.

### 4. Run it

Everything runs through Flyte local execution (`mode="local"`), which drives the submit-and-poll
loop in your process. There is no Flyte backend to deploy.

## What works today, and what does not

This is the M1 shape. Each node submits a real Armada job and is genuinely scheduled and polled,
but the workload is a placeholder (for example `echo`). The node's output is synthesised by the
connector from `output_template` and inputs, not produced by your code. So today you get:

- real Flyte 2 DAG topology and data flow between nodes,
- real Armada scheduling, including gang jobs,
- string data threaded through the graph.

What you cannot do yet is run an arbitrary Python function as the body of a node on Armada. That
is the next milestone (M2), described in [docs/architecture.md](docs/architecture.md#roadmap).

## Documentation

- [docs/architecture.md](docs/architecture.md) how the connector works, the state mapping, gang
  scheduling, and the M1/M2/M4 roadmap.
- [docs/getting-started.md](docs/getting-started.md) stand up a local Armada and run an example.
- [docs/gotchas.md](docs/gotchas.md) the non-obvious environment and proto issues.

## License

Apache-2.0. See [LICENSE](LICENSE).
