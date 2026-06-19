# Examples

Write ordinary Flyte 2 Python. Each `@env.task` runs in an Armada-scheduled pod. The only
Armada-specific line is `plugin_config=ArmadaConfig(queue=...)`; everything else (resources,
chaining, fan-out, typed data) is stock Flyte.

Three examples, in order:

| File | Shows | Expected output |
| --- | --- | --- |
| [`function.py`](function.py) | **Simple.** One task: a Black-Scholes option price. | `call price = 10.4506` |
| [`fanout.py`](fanout.py) | **Parallel.** A typed dataclass through a fan-out / fan-in (independent jobs via `asyncio.gather`). | `Stats(...)  mean = 506.47` |
| [`ml_pipeline.py`](ml_pipeline.py) | **Complex.** An end-to-end ML run: make data, k-fold cross-validation in parallel, pick the best model, evaluate. | `best alpha = 1.0  fit y ~ 3.00 x + 2.00` |

## Run one

Two one-command runners do all the infra (build the task image, wire the blob store):

```bash
./examples/run_local.sh examples/function.py     # local execution, prints the result
./demo/run.sh           examples/function.py     # through a Flyte backend, shows in the Flyte UI
```

Pass any example as the argument. Prerequisite: a running Armada cluster (see
[../docs/getting-started.md](../docs/getting-started.md)); the backend path additionally needs a
Flyte 2 backend (see [../demo/](../demo/)).

## What you write

```python
import flyte
from armada_flyte import ArmadaConfig

env = flyte.TaskEnvironment(
    name="quant",
    image="armada-flyte-task:v1",
    resources=flyte.Resources(cpu=1, memory="512Mi"),   # required; declared the stock-Flyte way
    plugin_config=ArmadaConfig(queue="flyte"),           # the one Armada-specific line
)

@env.task
async def price(spot: float, strike: float) -> float:
    ...
```

Resources are required (Armada rejects a job without them). Declare them with `flyte.Resources`
on the environment, or per task with `@env.task(resources=...)`. Need a GPU? `flyte.Resources(gpu=1)`.
`_runner.py` is the shared helper the examples call to run; it is not an example itself.
