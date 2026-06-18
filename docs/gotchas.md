# Gotchas

The non-obvious things that cost time when wiring Flyte 2 to Armada. Each was hit and fixed
while building this.

## Job status comes from the Lookout database

Armada's `GetJobStatus` gRPC does not read the scheduler database. It reads the Lookout
database (`internal/server/queryapi/query_api.go`). If the lookout ingester is not running, a
job can submit and even succeed while `GetJobStatus` keeps returning `UNKNOWN`. The connector
polls that API, so the lookout ingester has to be up.

## armada_client and Flyte both ship google/api protos

`armada_client` vendors its own `armada_client.google.api`, and Flyte's `flyteidl2` registers
the standard `google.api`. Both describe `google/api/http.proto`, and the second registration
fails with:

```
TypeError: Couldn't build proto file into descriptor pool: duplicate file name google/api/http.proto
```

`_proto_compat.py` fixes this by aliasing the vendored modules to the standard ones in
`sys.modules` before `armada_client` is imported. It is imported first in the package
`__init__`, so importing `armada_flyte` before `armada_client` is enough.

## Use an arm64 Python on Apple Silicon

Flyte depends on `obstore`, which has a native extension. An x86_64 interpreter (for example a
Rosetta build under `/usr/local`) fails to load the arm64 wheel:

```
incompatible architecture (have 'arm64', need 'x86_64')
```

Use a native arm64 interpreter, such as Homebrew's `python3.11`.

## k8s proto fields are camelCase

`armada_client`'s Kubernetes protos keep the original field names, so it is `restartPolicy` and
`terminationGracePeriodSeconds`, not the snake_case you might expect from generated Python
protos.

## Connector tasks and the driver cannot share an environment

A connector task is built with `image=None`, while a function task (the driver) defaults to
`image="auto"`. `TaskEnvironment.from_task` requires every task in an environment to share an
image, so the two cannot live together. Put the `ArmadaTask` nodes in their own environment and
give the driver environment a `depends_on` to it, as the examples do.
