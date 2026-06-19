# Examples

A learning ladder: each rung adds one concept. The first tier needs only an Armada cluster; the
later tiers run real Python in the pods, which needs a blob store and a task image (the runner
scripts set that up for you).

Prerequisite for all of them: a running Armada cluster (see
[../docs/getting-started.md](../docs/getting-started.md)) and an editable install
(`pip install -e ".[dev]"`).

## Tier 1: shell workloads, no blob store

The lower-level `ArmadaTask`: every node is a real Armada job running a shell command. Nothing to
set up beyond the cluster.

| File | What it adds | Armada jobs |
| --- | --- | --- |
| `single_task.py` | One `ArmadaTask`, no DAG. Output synthesised from `output_template`. | 1 |
| `hello_world_dag.py` | A 2-node linear DAG (`shout` depends on `hello`). | 2 |
| `fanout_map.py` | Fan-out / fan-in: N workers via `asyncio.gather`, then a reduce node (`FANOUT`, default 4). | N + 1 |
| `pipeline.py` | Gang scheduling **and** real in-pod compute via `capture_result`: a distributed `sum(1..9) = 45`, the gang of 3 reading results from pod logs. | 5 |

```bash
./.venv/bin/python examples/single_task.py
./.venv/bin/python examples/hello_world_dag.py
FANOUT=8 ./.venv/bin/python examples/fanout_map.py
./.venv/bin/python examples/pipeline.py
```

## Tier 2: real Python `@env.task`, local execution

A stock `@env.task` whose actual Python body runs in the pod, with typed inputs and outputs flowing
through a blob store. Requires a blob store reachable from the host and the pods, plus the task
image in the cluster. `run_local.sh` sets all of that up and runs the example in one command:

| File | What it adds | Armada jobs |
| --- | --- | --- |
| `python_function.py` | The first real Python body (`square`) in an Armada pod. | 1 |
| `python_pipeline.py` | Typed data: a `@dataclass` flowing through a 4-way map-reduce. | 6 |
| `gang_dag.py` | Gang scheduling on the real-Python surface (a gang of 3 `partial_sum`). | 5 |

```bash
./examples/run_local.sh examples/python_function.py   # square(7) = 49
./examples/run_local.sh examples/python_pipeline.py   # the map-reduce
./examples/run_local.sh examples/gang_dag.py          # total = 4789
```

## Tier 3: through a Flyte backend (in the Flyte UI)

The same Tier 2 files take `--backend`, registering with FlyteAdmin via `flyte.run` so the run
appears in the Flyte UI. `./demo/run.sh` does the backend wiring (see [../demo/](../demo/)):

```bash
./demo/run.sh                          # python_function.py --backend -> square(7) = 49, in the UI
./demo/run.sh examples/gang_dag.py     # the gang DAG, in the UI -> total = 4789
```
