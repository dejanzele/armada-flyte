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

## Output synthesis (the M1 boundary)

This connector does not run the user's Python inside the Armada pod. Each node submits a real
Armada job, but with a placeholder workload (for example `echo`). On success, the connector
synthesises the node's `result` output by rendering `ArmadaConfig.output_template` against the
job id and the node's inputs. Data flowing between nodes is therefore real Flyte 2 dataflow, but
the per-node computation is symbolic.

## Roadmap

- **M1 (this repo).** Real Armada submission, status polling, gang scheduling, and DAG dataflow,
  with placeholder per-node compute and synthesised outputs. Runs through Flyte local execution.
- **M2.** Run the user's actual Python in the Armada pod. This means shipping the task's code
  bundle and threading inputs and outputs through a shared blob store (S3, GCS, or MinIO) using
  Flyte's `a0` entrypoint, then having the connector wrap that container into the Armada pod spec.
- **M4.** Deploy the connector as a Flyte `ConnectorEnvironment` so a real Flyte backend
  (FlytePropeller) routes `armada` tasks to it, instead of only local execution.
