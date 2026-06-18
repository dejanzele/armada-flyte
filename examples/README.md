# Examples

Runnable Flyte 2 examples that schedule each task onto Armada via the `armada_flyte`
connector. Every DAG node is a *real* Armada job: the connector submits it, polls it to
completion, and synthesises the node's output from `ArmadaConfig.output_template` + inputs
(milestone M1).

## Prerequisites

- A running Armada cluster reachable at `$ARMADA_URL` (default `localhost:50051`). See
  [../docs/getting-started.md](../docs/getting-started.md) to stand one up locally.
- An editable install of this package (`pip install -e ".[dev]"`), so `armada_flyte` and
  `armada_client` resolve without setting `PYTHONPATH`.

Each example calls `ensure_queue()` on startup, so the Armada queue (default `flyte`,
override with `$ARMADA_QUEUE`) is created automatically if it does not already exist.

## Examples

| File                 | What it shows                                                                                                                                                                                  | Armada jobs |
|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `single_task.py`     | The simplest case: ONE `ArmadaTask` run directly (no DAG).                                                                                                                                     | 1           |
| `hello_world_dag.py` | A two-node DAG where `shout` depends on `hello` (linear data flow).                                                                                                                            | 2           |
| `fanout_map.py`      | Fan-out / fan-in: N workers run concurrently via `asyncio.gather`, then one reduce node.                                                                                                       | N + 1       |
| `gang_pipeline.py`   | `generate` then a 3-worker gang then `aggregate`. Generate's data is shared to every worker, the workers form an Armada gang (scheduled all-or-nothing), and aggregate combines their outputs. | 5           |
| `pipeline.py`   | Same shape as `gang_pipeline.py`, but the pods do real work: a distributed sum of 1..N, sharded across the gang. Inputs are passed to each pod as env vars and the result is read back from the pod's stdout. Prints `sum(1..9) = 45`. | 5           |

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

`gang_pipeline.py` is the gang scheduling example, and `pipeline.py` is the same shape with real
in-pod compute (a distributed sum):

```bash
./.venv/bin/python examples/gang_pipeline.py
./.venv/bin/python examples/pipeline.py
```
