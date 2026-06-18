# Running the connector as a service

By default the examples use Flyte local execution, which runs the connector in your own process.
A connector can also run as a long-running gRPC service that a deployed Flyte backend
(FlytePropeller) routes `armada` tasks to. This is the same connector code, hosted differently.

## Run the service locally

```bash
c0 --modules armada_flyte.connector        # serves the connector on :8000
```

`armada-flyte` declares a `flyte.connectors` entry point, so once it is installed the connector
also loads with a bare `c0` (no `--modules`). On startup the service prints its registered task
types:

```
Connector Name     Support Task Types
Armada Connector   armada (0)
```

Point it at Armada with `ARMADA_URL` (default `localhost:50051`) and `BINOCULARS_URL` (default
`localhost:50053`).

## Drive it like a backend would

`deploy/call_service.py` calls `CreateTask` and polls `GetTask` over gRPC, exactly as a Flyte
backend does. With an Armada cluster and the service both up:

```bash
./.venv/bin/python deploy/call_service.py
```

It submits a real Armada job through the service and prints the job's captured output, for example:

```
terminal: SUCCEEDED (armada job 01k... state=SUCCEEDED)
  output result = 'hello-from-deployed-connector'
```

## Deploy it to a Flyte backend

`deploy/app.py` defines the connector as a `flyte.app.ConnectorEnvironment`. Against a Flyte
backend (`flyte.init_from_config()` pointed at one), deploy it with:

```bash
python deploy/app.py        # calls flyte.deploy(connector)
```

This builds the image and creates the connector deployment. After that, a task whose
`task_type` is `armada` is routed to it automatically, so workflows run without local execution.
A deployed Flyte backend is the one prerequisite this repo does not stand up for you.
