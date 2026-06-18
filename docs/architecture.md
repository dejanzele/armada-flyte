# Architecture

Flyte owns the DAG and the data flow between nodes. Armada owns scheduling and execution. The
bridge between them is a single Flyte 2 connector.

## The connector contract

Flyte 2 connectors implement three async methods. This connector maps each onto one Armada gRPC
call:

| Flyte method | Armada call         | Purpose                                         |
|--------------|---------------------|-------------------------------------------------|
| `create`     | `Submit.SubmitJobs` | Submit one Armada job, return a job handle       |
| `get`        | `Jobs.GetJobStatus` | Poll status, map the job state to a Flyte phase  |
| `delete`     | `Submit.CancelJobs` | Cancel the job                                   |

A task is routed to the connector by its `task_type`. `ArmadaTask` sets `task_type = "armada"`,
which matches `ArmadaConnector.task_type_name`. The task's `ArmadaConfig` is serialised into the
task template's `custom` field, and the connector reads it back in `create` to build the job.

## Execution model

In local execution, Flyte's `AsyncConnectorExecutorMixin` drives the loop in-process: it calls
`create` once, then polls `get` every 3 seconds until the task reaches a terminal phase
(`SUCCEEDED`, `FAILED`, or `ABORTED`). The connector itself holds no state between calls. The
job handle it needs (`job_id`, `job_set_id`, `queue`, and the data to render the output) lives
in `ArmadaJobMetadata`, which Flyte persists between `create` and `get`/`delete`.

```mermaid
flowchart TD
    dag["Flyte 2 DAG (ArmadaTask nodes)"]
    mixin["AsyncConnectorExecutorMixin: create once, then poll get every 3s"]
    conn["ArmadaConnector (task_type 'armada')"]
    armada["Armada (localhost:50051)"]
    pod["Kubernetes pod"]

    dag --> mixin --> conn
    conn -- "SubmitJobs / GetJobStatus / CancelJobs" --> armada
    armada --> pod
    armada -. "job state" .-> conn
```

## State mapping

The connector maps Armada `JobState` onto Flyte's `TaskExecution.Phase`:

| Armada JobState                 | Flyte phase        | Note                                     |
|---------------------------------|--------------------|------------------------------------------|
| `QUEUED`, `SUBMITTED`, `LEASED` | `QUEUED`           |                                          |
| `PENDING`                       | `INITIALIZING`     |                                          |
| `RUNNING`, `UNKNOWN`            | `RUNNING`          | `UNKNOWN` is transient, keep polling     |
| `SUCCEEDED`                     | `SUCCEEDED`        |                                          |
| `FAILED`, `REJECTED`            | `FAILED`           |                                          |
| `CANCELLED`                     | `ABORTED`          |                                          |
| `PREEMPTED`                     | `RETRYABLE_FAILED` | preemption is expected, so Flyte retries |

Mapping `PREEMPTED` to `RETRYABLE_FAILED` is deliberate: Armada preempts jobs as part of normal
fair-share scheduling, so the node should retry rather than fail the run.

## Gang scheduling

`ArmadaConfig` exposes `gang_id`, `gang_cardinality`, and `gang_node_uniformity_label`. When a
task sets `gang_id` and a `gang_cardinality` of two or more, the connector attaches Armada's gang
annotations (`armadaproject.io/gangId`, `armadaproject.io/gangCardinality`) to the submission.
Jobs sharing a gang are scheduled all-or-nothing together. Call the same `ArmadaTask` N times
inside the DAG to submit N gang members. See `examples/gang_pipeline.py`.

## Output: synthesised, or read from the pod

By default the connector does not run the user's Python inside the pod. Each node submits a real
Armada job with a placeholder workload, and on success the connector synthesises the node's
`result` by rendering `ArmadaConfig.output_template` against the job id and inputs. Data flowing
between nodes is real Flyte 2 dataflow, but the per-node computation is symbolic.

Setting `capture_result=True` makes the compute real. The connector passes each input into the
pod as an upper-cased env var (input `numbers` becomes `$NUMBERS`), the workload computes a value
and prints a line `ARMADA_RESULT:<value>`, and on success the connector reads that line back from
the pod's logs (via binoculars) and returns it as the node's output. If no such line is found it
falls back to the template. `examples/pipeline.py` uses this to run a distributed sum across
a gang of workers.

## Real Python tasks

A normal `@env.task` function can run its body inside an Armada pod. Register `ArmadaConfig` as a
`TaskEnvironment` plugin (`ArmadaFunctionTask`, wired via `TaskPluginRegistry`), then:

```python
env = flyte.TaskEnvironment("ml", image=img, plugin_config=ArmadaConfig(queue="compute"))

@env.task
async def square(x: int) -> int:
    return x * x
```

How it works: Flyte renders the function into a container whose entrypoint is `a0` (it loads the
code bundle, reads inputs from blob storage, runs the function, writes outputs back). For these
tasks the connector ignores the placeholder fields and instead wraps that rendered container into
the Armada pod, adding blob-store credentials so the pod can reach storage. The function executes
in the pod and its return value flows back as the task output. See `examples/python_function.py`.

This needs two things in place:

- **A blob store** (S3, GCS, or MinIO) reachable at one address by both your process (to upload
  the code bundle and inputs) and the Armada pods (to read them and write outputs). Set it with
  `flyte.init(storage=...)` and tell the connector via `FLYTE_BLOB_ENDPOINT` /
  `FLYTE_BLOB_ACCESS_KEY` / `FLYTE_BLOB_SECRET_KEY`, and use a remote `raw_data_path` (`s3://...`).
- **A task image** carrying `flyte` plus the workflow's own dependencies (including `armada_flyte`),
  available on the Armada cluster (for a local kind cluster, `kind load docker-image ...`).

## Execution modes

The connector runs in two ways with the same code:

- **Local execution** (`mode="local"`), where `AsyncConnectorExecutorMixin` drives the
  create/poll loop in your process. This is what the examples use.
- **As a gRPC service**, where a deployed Flyte backend (FlytePropeller) calls `CreateTask` and
  `GetTask` on the connector over gRPC. Run it with `c0 --modules armada_flyte.connector` or
  deploy it as a `ConnectorEnvironment`. See [../deploy/](../deploy/).

## Limitations and next steps

What works today: real Armada submission, status polling, gang scheduling, DAG dataflow, in-pod
compute for shell workloads (via `capture_result`), real Python `@env.task` functions running in
the pod (see above), and both execution modes above.

One prerequisite is outside this repo:

- **A deployed Flyte backend.** The connector is ready to deploy as a `ConnectorEnvironment` and
  serves the connector gRPC API, but automatic task routing needs a running Flyte backend
  (FlytePropeller), which this repo does not stand up.
