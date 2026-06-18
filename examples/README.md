# Examples

Runnable Flyte 2 examples that schedule each task onto Armada via the `armada_flyte`
connector. Every DAG node is a *real* Armada job: the connector submits it, polls it to
completion, and synthesises the node's output from `ArmadaConfig.output_template` + inputs
(milestone M1).

## Prerequisites

- The Armada localdev stack running and reachable at `$ARMADA_URL` (default `localhost:50051`).
  The connector's `GetJobStatus` reads the Lookout DB, so `lookoutingester` must be running or
  job status stays `UNKNOWN`.
- An editable install of this package (`pip install -e ".[dev]"`), so `armada_flyte` and
  `armada_client` resolve without setting `PYTHONPATH`.

Each example calls `ensure_queue()` on startup, so the Armada queue (default `flyte`,
override with `$ARMADA_QUEUE`) is created automatically if it does not already exist.

## Examples

| File | What it shows | Armada jobs |
|------|---------------|-------------|
| `single_task.py` | The simplest case: ONE `ArmadaTask` run directly (no DAG). | 1 |
| `hello_world_dag.py` | A two-node DAG where `shout` depends on `hello` (linear data flow). | 2 |
| `fanout_map.py` | Fan-out / fan-in: N workers run concurrently via `asyncio.gather`, then one reduce node. | N + 1 |

## Running

After the editable install, no `PYTHONPATH` is needed:

```bash
./.venv/bin/python examples/single_task.py
./.venv/bin/python examples/hello_world_dag.py
./.venv/bin/python examples/fanout_map.py
```

`fanout_map.py` honours `FANOUT` (default `4`) to set the number of concurrent worker jobs:

```bash
FANOUT=8 ./.venv/bin/python examples/fanout_map.py
```
