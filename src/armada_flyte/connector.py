"""A Flyte 2 AsyncConnector that submits each task as an Armada job.

The connector implements the three-method Flyte v2 connector contract:

    create   submits an Armada job (Submit.SubmitJobs) and returns a job handle
    get      polls job status (Jobs.GetJobStatus) and maps it to a Flyte phase
    delete   cancels the job (Submit.CancelJobs)

In local execution, ``flyte.connectors.AsyncConnectorExecutorMixin`` drives this loop
in-process: it calls ``create`` once, then polls ``get`` every 3s until a terminal phase.

Each DAG node runs a real Armada job. By default the workload is a placeholder (e.g. ``echo``)
and the node's output is synthesised from the task's ``output_template`` and inputs. With
``capture_result=True`` the pod does real work and the connector reads its result from the pod
logs. Running an arbitrary Python function inside the pod (shipping code and moving inputs and
outputs through a blob store) is not supported yet.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import grpc

# Must run before any armada_client import (aliases vendored google.api to standard).
import armada_flyte._proto_compat  # noqa: F401
from armada_client.armada import submit_pb2
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import generated_pb2 as api_resource
from armada_client.log_client import JobLogClient
from flyte import logger
from flyte.connectors import AsyncConnector, ConnectorRegistry, Resource, ResourceMeta
from flyteidl2.core.execution_pb2 import TaskExecution
from google.protobuf import json_format

# A task that wants its real output captured prints a line beginning with this marker. The
# connector reads it back from the job's pod logs once the job succeeds.
RESULT_MARKER = "ARMADA_RESULT:"

# Armada JobState (pkg/api/submit.proto:97) -> Flyte TaskExecution.Phase.
# PREEMPTED -> RETRYABLE_FAILED so Flyte retries jobs Armada preempted (expected, not a failure).
_ARMADA_STATE_TO_PHASE: Dict[int, "TaskExecution.Phase"] = {
    submit_pb2.QUEUED: TaskExecution.QUEUED,
    submit_pb2.SUBMITTED: TaskExecution.QUEUED,
    submit_pb2.LEASED: TaskExecution.QUEUED,
    submit_pb2.PENDING: TaskExecution.INITIALIZING,
    submit_pb2.RUNNING: TaskExecution.RUNNING,
    submit_pb2.UNKNOWN: TaskExecution.RUNNING,  # transient; keep polling
    submit_pb2.SUCCEEDED: TaskExecution.SUCCEEDED,
    submit_pb2.FAILED: TaskExecution.FAILED,
    submit_pb2.REJECTED: TaskExecution.FAILED,
    submit_pb2.CANCELLED: TaskExecution.ABORTED,
    submit_pb2.PREEMPTED: TaskExecution.RETRYABLE_FAILED,
}

# Armada gang scheduling (internal/common/constants/constants.go). Jobs sharing a gangId, all
# declaring the same gangCardinality, are scheduled all-or-nothing together.
_GANG_ID_ANNOTATION = "armadaproject.io/gangId"
_GANG_CARDINALITY_ANNOTATION = "armadaproject.io/gangCardinality"
_GANG_NODE_UNIFORMITY_ANNOTATION = "armadaproject.io/gangNodeUniformityLabel"


def _gang_annotations(cfg: Dict[str, Any]) -> Dict[str, str]:
    """Translate gang_* config into Armada gang annotations, or {} for a non-gang job."""
    gang_id = cfg.get("gang_id")
    cardinality = int(cfg.get("gang_cardinality") or 0)
    if not gang_id or cardinality < 2:
        return {}
    annotations = {
        _GANG_ID_ANNOTATION: gang_id,
        _GANG_CARDINALITY_ANNOTATION: str(cardinality),
    }
    label = cfg.get("gang_node_uniformity_label")
    if label:
        annotations[_GANG_NODE_UNIFORMITY_ANNOTATION] = label
    return annotations


@dataclass
class ArmadaJobMetadata(ResourceMeta):
    """Handle Flyte persists between create() and get()/delete()."""

    job_id: str
    job_set_id: str
    queue: str
    output_template: str = ""
    inputs: Dict[str, Any] = field(default_factory=dict)
    capture_result: bool = False


def _quantity(value: str) -> api_resource.Quantity:
    return api_resource.Quantity(string=value)


class ArmadaConnector(AsyncConnector):
    name = "Armada Connector"
    task_type_name = "armada"
    metadata_type = ArmadaJobMetadata

    def __init__(self, armada_url: Optional[str] = None, binoculars_url: Optional[str] = None):
        self._url = armada_url or os.environ.get("ARMADA_URL", "localhost:50051")
        self._binoculars_url = binoculars_url or os.environ.get("BINOCULARS_URL", "localhost:50053")
        # Blob store the Armada pods use for function-task inputs/outputs (must match the store
        # the Flyte client uploaded to). Empty endpoint means placeholder tasks only.
        self._blob_endpoint = os.environ.get("FLYTE_BLOB_ENDPOINT", "")
        self._blob_key = os.environ.get("FLYTE_BLOB_ACCESS_KEY", "")
        self._blob_secret = os.environ.get("FLYTE_BLOB_SECRET_KEY", "")
        self._blob_region = os.environ.get("FLYTE_BLOB_REGION", "us-east-1")
        self._client: Optional[ArmadaClient] = None

    @property
    def client(self) -> ArmadaClient:
        # Lazily build the channel so importing the module never opens a socket.
        if self._client is None:
            self._client = ArmadaClient(grpc.insecure_channel(self._url))
        return self._client

    def _build_pod_spec(self, cfg: Dict[str, Any], inputs: Dict[str, Any]) -> core_v1.PodSpec:
        cpu = _quantity(cfg.get("cpu", "100m"))
        memory = _quantity(cfg.get("memory", "128Mi"))
        requests = {"cpu": cpu, "memory": memory}
        # Expose each input to the workload as an upper-cased env var, so the pod can do real
        # work on its inputs (e.g. input "dataset" becomes $DATASET).
        env = [core_v1.EnvVar(name=k.upper(), value=str(v)) for k, v in inputs.items()]
        container = core_v1.Container(
            name=cfg.get("container_name", "armada-task"),
            image=cfg.get("image", "busybox:latest"),
            command=list(cfg.get("command", ["sh", "-c", "echo hello from armada"])),
            args=list(cfg.get("args", [])),
            env=env,
            # Requests == limits so the task gets a guaranteed QoS pod.
            resources=core_v1.ResourceRequirements(requests=requests, limits=dict(requests)),
        )
        # armada_client's k8s protos use camelCase field names.
        return core_v1.PodSpec(
            terminationGracePeriodSeconds=0,
            restartPolicy="Never",
            containers=[container],
        )

    def _storage_env(self) -> list:
        """Env vars so a function-task pod can reach the blob store (S3/MinIO over plain HTTP)."""
        if not self._blob_endpoint:
            return []
        return [
            core_v1.EnvVar(name="AWS_ACCESS_KEY_ID", value=self._blob_key),
            core_v1.EnvVar(name="AWS_SECRET_ACCESS_KEY", value=self._blob_secret),
            core_v1.EnvVar(name="AWS_DEFAULT_REGION", value=self._blob_region),
            core_v1.EnvVar(name="AWS_REGION", value=self._blob_region),
            core_v1.EnvVar(name="AWS_ENDPOINT_URL", value=self._blob_endpoint),
            core_v1.EnvVar(name="AWS_ENDPOINT_URL_S3", value=self._blob_endpoint),
            core_v1.EnvVar(name="AWS_ALLOW_HTTP", value="true"),
        ]

    def _pod_from_flyte_container(self, c, cfg: Dict[str, Any]) -> core_v1.PodSpec:
        """Wrap Flyte's rendered task container (the a0 entrypoint) into an Armada pod, so the
        user's real function runs in the pod with inputs/outputs via the blob store."""
        cpu = _quantity(cfg.get("cpu", "1"))
        memory = _quantity(cfg.get("memory", "500Mi"))
        requests = {"cpu": cpu, "memory": memory}
        env = []
        for e in c.env:
            name = getattr(e, "name", None) or getattr(e, "key", "")
            env.append(core_v1.EnvVar(name=name, value=e.value))
        env.extend(self._storage_env())
        container = core_v1.Container(
            name="armada-task",
            image=c.image,
            command=list(c.command),
            args=list(c.args),
            env=env,
            # Use the image already loaded into the cluster (e.g. via `kind load`).
            imagePullPolicy="IfNotPresent",
            resources=core_v1.ResourceRequirements(requests=requests, limits=dict(requests)),
        )
        return core_v1.PodSpec(
            terminationGracePeriodSeconds=0,
            restartPolicy="Never",
            containers=[container],
        )

    def _result_from_logs(self, job_id: str) -> Optional[str]:
        """Read the pod's stdout via binoculars and return the last RESULT_MARKER line's value."""
        try:
            lines = JobLogClient(self._binoculars_url, job_id, disable_ssl=True).logs()
        except grpc.RpcError as e:
            logger.warning(f"[armada] could not fetch logs for {job_id}: {e.code().name}")
            return None
        for line in reversed(lines):
            text = getattr(line, "line", str(line))
            if text.startswith(RESULT_MARKER):
                return text[len(RESULT_MARKER):].strip()
        return None

    async def create(
        self,
        task_template,
        output_prefix: str = "",
        inputs: Optional[Dict[str, Any]] = None,
        task_execution_metadata=None,
        **kwargs,
    ) -> ArmadaJobMetadata:
        cfg = json_format.MessageToDict(task_template.custom) if task_template.custom else {}
        queue = cfg.get("queue", "flyte")
        job_set_id = cfg.get("job_set_id", "flyte-dag")
        inputs = inputs or {}

        container = getattr(task_template, "container", None)
        if container and container.image:
            # Real function task: run Flyte's rendered container (the a0 entrypoint) in the pod.
            pod = self._pod_from_flyte_container(container, cfg)
        else:
            # Placeholder task (ArmadaTask): build the pod from ArmadaConfig.
            pod = self._build_pod_spec(cfg, inputs)
        annotations = _gang_annotations(cfg)
        item = self.client.create_job_request_item(
            priority=float(cfg.get("priority", 1)),
            namespace=cfg.get("namespace", "default"),
            pod_spec=pod,
            labels={"flyte.org/connector": "armada"},
            annotations=annotations,
        )
        resp = self.client.submit_jobs(queue=queue, job_set_id=job_set_id, job_request_items=[item])
        first = resp.job_response_items[0]
        if first.error:
            raise RuntimeError(f"Armada rejected job submission: {first.error}")

        gang = f" gang={annotations[_GANG_ID_ANNOTATION]}" if annotations else ""
        logger.info(f"[armada] submitted job {first.job_id} to queue={queue} job_set={job_set_id}{gang}")
        return ArmadaJobMetadata(
            job_id=first.job_id,
            job_set_id=job_set_id,
            queue=queue,
            output_template=cfg.get("output_template", "armada job {job_id} succeeded"),
            inputs={k: str(v) for k, v in inputs.items()},
            capture_result=bool(cfg.get("capture_result", False)),
        )

    async def get(self, resource_meta: ArmadaJobMetadata, **kwargs) -> Resource:
        states = self.client.get_job_status([resource_meta.job_id]).job_states
        state = states.get(resource_meta.job_id, submit_pb2.UNKNOWN)
        phase = _ARMADA_STATE_TO_PHASE.get(state, TaskExecution.RUNNING)

        outputs = None
        if phase == TaskExecution.SUCCEEDED:
            # If the task asked for real output, read what the pod actually printed; otherwise
            # (and as a fallback) synthesise the output from the template and inputs.
            result = None
            if resource_meta.capture_result:
                result = self._result_from_logs(resource_meta.job_id)
            if result is None:
                result = resource_meta.output_template.format(
                    job_id=resource_meta.job_id, **resource_meta.inputs
                )
            outputs = {"result": result}

        return Resource(
            phase=phase,
            message=f"armada job {resource_meta.job_id} state={submit_pb2.JobState.Name(state)}",
            outputs=outputs,
        )

    async def delete(self, resource_meta: ArmadaJobMetadata, **kwargs):
        self.client.cancel_jobs(
            queue=resource_meta.queue,
            job_set_id=resource_meta.job_set_id,
            job_id=resource_meta.job_id,
        )
        logger.info(f"[armada] cancelled job {resource_meta.job_id}")


# Register a default connector instance so local execution can find it by task_type.
ConnectorRegistry.register(ArmadaConnector())
