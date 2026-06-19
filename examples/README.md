# Examples

Runnable Flyte 2 examples that schedule each task onto Armada via the `armada_flyte`
connector. Every DAG node is a real Armada job: the connector submits it and polls it to
completion. By default the node's output is synthesised from `ArmadaConfig.output_template` and
inputs; `pipeline.py` shows real in-pod compute via `capture_result`.

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
| `python_function.py` | A real Python `@env.task` (`square(x)`) whose body runs inside an Armada pod, with inputs/outputs through a blob store. Needs a MinIO reachable from the host and the cluster, and a task image loaded into the cluster (see the file header and docs/architecture.md). Prints `49`. | 1           |
| `python_pipeline.py` | A real multi-stage map-reduce: `generate` then 4 parallel `shard_stats` then `merge`, each stage's actual Python running in an Armada pod with typed data (a `Stats` dataclass) flowing between them. Run it with one command via `run_python_pipeline.sh`. | 6           |
| `gang_dag.py`        | The real-Python version of `gang_pipeline.py`: `generate` then a 3-worker GANG (`partial_sum`) then `total`. The three workers run actual Python and are scheduled as one Armada gang (shared `gang_id`, cardinality 3). Prints `total = 4789`. | 5           |

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

The two real-Python examples (`python_function.py`, `python_pipeline.py`) run your actual function
body in the pod, so they also need a blob store and a task image in the cluster. `run_python_pipeline.sh`
sets all of that up and runs the map-reduce in one command:

```bash
./examples/run_python_pipeline.sh
```
