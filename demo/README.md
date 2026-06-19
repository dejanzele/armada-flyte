# Demo: real Python on Armada, through the Flyte backend

`./demo/run.sh` runs an ordinary Flyte 2 task on Armada via a real Flyte backend. The task is
registered with FlyteAdmin, scheduled by Armada as a pod, and shows up in the Flyte UI with its
real result:

```
submitted run rf6zwrmnpzpwdgnfzffn
  UI: http://localhost:30080/v2/.../runs/rf6zwrmnpzpwdgnfzffn
square(7) = 49  (real Python, computed in an Armada pod, via the Flyte backend)
```

## What you write

Stock Flyte 2. The only Armada-specific line is `plugin_config`:

```python
env = flyte.TaskEnvironment("ml", image="armada-flyte-task:latest",
                            plugin_config=ArmadaConfig(queue="flyte"))

@env.task
async def square(x: int) -> int:
    return x * x

flyte.run(square, x=7)
```

## Run it

```bash
./demo/run.sh
```

The script builds the task image, loads it into the Armada cluster, points the connector at the
backend's blob store, and runs the task. It prints the UI link and the result.

Run a different example by passing it, for example the gang DAG (`generate`, a gang of 3 workers,
`aggregate`), which runs end to end through the backend:

```bash
./demo/run.sh examples/backend_gang.py
```

## One-time prerequisites

The script does the wiring; it assumes two things are already running:

1. **An Armada cluster** with a real executor on a kind cluster. From a checkout of the
   [armada](https://github.com/armadaproject/armada) repo:

   ```bash
   mage Kind            # kind cluster (default name: armada-test)
   mage dev:up no-auth  # Armada with a real executor
   ```

2. **A Flyte 2 backend** whose executor routes `armada` tasks to the connector. This needs the
   connector backend plugin registered in the executor (stock Flyte 2 does not register it). Run
   the backend as a devbox (`flyte start devbox`) using an executor build that includes the plugin.

Override defaults via env vars: `KIND_CLUSTER`, `DEVBOX`, `ARMADA_URL`, `HOST_IP`.

## What the script hides

The two things backend execution needs beyond local execution, both handled for you:

- **One shared blob store.** The backend, the client, and the Armada pods all use the backend's
  bucket. The pods reach it through its host-published port; the script reads the endpoint and
  credentials and passes them to the connector.
- **Runtime arguments.** For a backend run FlytePropeller normally fills the task's runtime args
  (`--run-base-dir`, org/project/domain, run and action names). The connector fills them from the
  task execution metadata, so the function runs and its real typed output is recorded.

See [../docs/architecture.md](../docs/architecture.md#real-python-tasks) and
[../deploy/README.md](../deploy/README.md) for the full picture.
