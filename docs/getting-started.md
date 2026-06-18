# Getting started

Run a Flyte 2 DAG whose nodes execute as Armada jobs, end to end, on a local stack.

## 1. Bring up Armada

From a checkout of the [armada](https://github.com/armadaproject/armada) repo, start its
dependencies and a minimal stack. You need the server, scheduler, scheduler ingester, event
ingester, an executor, and the lookout ingester. A fake executor is enough, so no Kubernetes
cluster is required:

```
docker compose -f _local/docker-compose-deps.yaml up -d   # postgres, redis, pulsar
bash _local/scripts/init.sh                                # create DBs, run migrations
goreman -f _local/procfiles/fake-executor.Procfile start   # or your own minimal profile
```

The lookout ingester is not optional here. Armada's `GetJobStatus` reads the Lookout database,
so without it a submitted job reports `UNKNOWN` forever and the connector never sees it finish.

Wait until the executor logs `Reporting current free resource` before submitting.

## 2. Install this package

On Apple Silicon use an arm64 Python. An x86_64 interpreter cannot load Flyte's native
`obstore` wheel.

```
python3.11 -m venv .venv
./.venv/bin/pip install -e ".[dev]"
```

## 3. Run the example

```
./.venv/bin/python examples/hello_world_dag.py
```

You should see two jobs submitted (`hello`, then `shout`, which depends on it) and a final
line built from both:

```
[setup] created Armada queue 'flyte'
submitted job 01k... to queue=flyte job_set=flyte-dag
submitted job 01k... to queue=flyte job_set=flyte-dag

=== DAG result (flowed through 2 real Armada jobs) ===
SHOUT<Hello, world! (ran as armada job 01k...)> (ran as armada job 01k...)
```

The second job is submitted only after the first reaches `SUCCEEDED`, which is the data
dependency in the DAG driving real Armada scheduling.

## Configuration

Both are read from the environment:

- `ARMADA_URL` (default `localhost:50051`): the Armada submit/status gRPC endpoint.
- `ARMADA_QUEUE` (default `flyte`): the queue jobs are submitted to. Each example creates it
  if it does not exist.

If something does not behave, check [gotchas.md](gotchas.md) first. Most setup problems are
listed there.
