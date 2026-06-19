# Getting started

Run a Flyte 2 DAG whose nodes execute as Armada jobs, end to end, on a local stack.

## 1. Bring up Armada

The connector needs a running Armada cluster. From a checkout of the
[armada](https://github.com/armadaproject/armada) repo, create a local Kubernetes cluster and
start the stack with a real executor:

```
mage Kind            # create the Kind cluster and select its kube context
mage dev:up no-auth  # dependencies, migrations, and the full Armada stack with a real executor
```

`mage Kind` selects the `kind-armada-test` context, which the executor uses to create pods. Tear
the cluster down again with `mage KindTeardown`, and stop the dependencies with `mage dev:down`.

Wait until the executor logs `Reporting current free resource` before submitting.

## 2. Install this package

On Apple Silicon use an arm64 Python. An x86_64 interpreter cannot load Flyte's native
`obstore` wheel.

```
python3.11 -m venv .venv
./.venv/bin/pip install -e ".[dev]"
```

To use the connector in your own project instead of this repo, install it directly:

```
pip install "armada-flyte @ git+https://github.com/armadaproject/armada-flyte.git"
```

## 3. Run an example

The runner builds the task image, wires a blob store, and runs the example in one command:

```
./examples/run_local.sh examples/function.py
```

It runs a Black-Scholes price as a real `@env.task` in an Armada pod and prints:

```
call price = 10.4506  (expected ~10.4506, computed in an Armada pod)
```

Then try `examples/fanout.py` (a parallel fan-out) and `examples/ml_pipeline.py` (a full ML
pipeline). To run on a Flyte backend and see it in the Flyte UI, use `./demo/run.sh <example>`
(see [../demo/](../demo/)). The example surface is described in [../examples/](../examples/).

## Configuration

- `ARMADA_URL` (default `localhost:50051`): the Armada submit/status gRPC endpoint, read by the
  connector.
- `ARMADA_TASK_IMAGE` (default `armada-flyte-task:v1`): the task image the runner builds and loads.

If something does not behave, check [gotchas.md](gotchas.md) first. Most setup problems are
listed there.
