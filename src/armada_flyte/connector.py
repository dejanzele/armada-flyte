"""A Flyte 2 AsyncConnector that submits each task as an Armada job.

The connector implements the three-method Flyte v2 connector contract:

    create   submits an Armada job (Submit.SubmitJobs) and returns a job handle
    get      polls job status (Jobs.GetJobStatus) and maps it to a Flyte phase
    delete   cancels the job (Submit.CancelJobs)

In local execution, ``flyte.connectors.AsyncConnectorExecutorMixin`` drives this loop
in-process: it calls ``create`` once, then polls ``get`` every 3s until a terminal phase.

Each DAG node runs a real Armada job. For a placeholder ``ArmadaTask`` the workload is a shell
command (e.g. ``echo``) and the node's output is synthesised from the task's ``output_template``,
or read from the pod logs when ``capture_result=True``. For a real ``@env.task`` function the
connector wraps Flyte's rendered container (the ``a0`` entrypoint) into the pod, so the function
body runs with its inputs and outputs flowing through the configured blob store.
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
    # True for real @env.task functions: a0 writes the real (typed) output to the output location,
    # so get() must not synthesise one (which would overwrite it on the backend path).
    is_function_task: bool = False


def _quantity(value: str) -> api_resource.Quantity:
    return api_resource.Quantity(string=value)


def _resources(cpu: str, memory: str) -> core_v1.ResourceRequirements:
    # Requests == limits so the task gets a guaranteed QoS pod.
    requests = {"cpu": _quantity(cpu), "memory": _quantity(memory)}
    return core_v1.ResourceRequirements(requests=requests, limits=dict(requests))


def _pod_spec(container: core_v1.Container) -> core_v1.PodSpec:
    # armada_client's k8s protos use camelCase field names.
    return core_v1.PodSpec(
        terminationGracePeriodSeconds=0,
        restartPolicy="Never",
        containers=[container],
    )


class ArmadaConnector(AsyncConnector):
    name = "Armada Connector"
    task_type_name = "armada"
    metadata_type = ArmadaJobMetadata

    def __init__(self, armada_url: Optional[str] = None, binoculars_url: Optional[str] = None):
        self._url = armada_url or os.environ.get("ARMADA_URL", "localhost:50051")
        self._binoculars_url = binoculars_url or os.environ.get("BINOCULARS_URL", "localhost:50053")
        # Storage config the Armada pods get (as FLYTE_AWS_*), pointing at the same store the Flyte
        # client uploaded to. Empty endpoint means placeholder tasks only (no blob store needed).
        self._blob_endpoint = os.environ.get("FLYTE_BLOB_ENDPOINT", "")
        self._blob_key = os.environ.get("FLYTE_BLOB_ACCESS_KEY", "")
        self._blob_secret = os.environ.get("FLYTE_BLOB_SECRET_KEY", "")
        self._client: Optional[ArmadaClient] = None

    @property
    def client(self) -> ArmadaClient:
        # Lazily build the channel so importing the module never opens a socket.
        if self._client is None:
            self._client = ArmadaClient(grpc.insecure_channel(self._url))
        return self._client

    def _build_pod_spec(self, cfg: Dict[str, Any], inputs: Dict[str, Any]) -> core_v1.PodSpec:
        # Expose each input to the workload as an upper-cased env var, so the pod can do real
        # work on its inputs (e.g. input "dataset" becomes $DATASET).
        env = [core_v1.EnvVar(name=k.upper(), value=str(v)) for k, v in inputs.items()]
        container = core_v1.Container(
            name=cfg.get("container_name", "armada-task"),
            image=cfg.get("image", "busybox:latest"),
            command=list(cfg.get("command", ["sh", "-c", "echo hello from armada"])),
            args=list(cfg.get("args", [])),
            env=env,
            resources=_resources(cfg.get("cpu", "100m"), cfg.get("memory", "128Mi")),
        )
        return _pod_spec(container)

    def _storage_env(self) -> list:
        """Stamp the platform storage config onto the pod using Flyte's own FLYTE_AWS_* convention,
        exactly as FlytePropeller does for in-cluster task pods. The a0 runtime reads these via
        flyte.storage.S3.auto(). This is a self-hosted S3 store (MinIO/RustFS), not AWS cloud: the
        endpoint differs from the backend's because the Armada pod is on another cluster, but the
        credentials are the platform's. obstore allows plain HTTP by default, so no extra flags."""
        if not self._blob_endpoint:
            return []
        return [
            core_v1.EnvVar(name="FLYTE_AWS_ENDPOINT", value=self._blob_endpoint),
            core_v1.EnvVar(name="FLYTE_AWS_ACCESS_KEY_ID", value=self._blob_key),
            core_v1.EnvVar(name="FLYTE_AWS_SECRET_ACCESS_KEY", value=self._blob_secret),
        ]

    @staticmethod
    def _run_name(meta) -> str:
        """The Flyte run name (shared by every action in a run), or "" if unavailable."""
        try:
            return meta.task_execution_id.node_execution_id.execution_id.name
        except AttributeError:
            return ""

    @staticmethod
    def _runtime_args(args: list, output_prefix: str, meta) -> list:
        """Fill in the runtime arguments the backend leaves for the executor to substitute.

        In local execution the SDK renders these; in backend execution FlytePropeller normally
        does, but the connector (webapi) plugin does not, so the a0 args arrive with unfilled
        ``{{.runName}}``/``{{.actionName}}`` placeholders and no ``--run-base-dir`` (which a0
        requires). We fill them from the task execution metadata. Both are conditional, so this is
        a no-op on the already-complete local args.
        """
        run_name = action_name = project = domain = org = ""
        try:
            tid = meta.task_execution_id.task_id
            project, domain = tid.project, tid.domain
            ne = meta.task_execution_id.node_execution_id
            run_name = ne.execution_id.name
            action_name = ne.node_id
        except AttributeError:
            pass
        # labels is a protobuf map<string,string>; organization carries the org.
        try:
            org = meta.labels.get("organization", "")
        except (AttributeError, TypeError):
            pass
        subs = {"{{.runName}}": run_name, "{{.actionName}}": action_name}
        args = [subs.get(a, a) for a in args]
        # output_prefix ends in /<run>/<action>/<attempt>; the run base dir drops action+attempt.
        if "--run-base-dir" not in args and output_prefix:
            args = args + ["--run-base-dir", output_prefix.rsplit("/", 2)[0]]
        # a0 requires org (and project/domain) which the backend leaves for the executor to fill.
        for flag, val in (("--org", org), ("--project", project), ("--domain", domain)):
            if flag not in args and val:
                args = args + [flag, val]
        return args

    def _pod_from_flyte_container(self, c, cfg: Dict[str, Any], output_prefix: str = "", meta=None) -> core_v1.PodSpec:
        """Wrap Flyte's rendered task container (the a0 entrypoint) into an Armada pod, so the
        user's real function runs in the pod with inputs/outputs via the blob store."""
        # Build env from the rendered container, then let our blob-store env override any matching
        # keys (e.g. a backend may bake an in-cluster storage endpoint the Armada pods can't reach).
        env_by_name: Dict[str, str] = {}
        for e in c.env:
            env_by_name[getattr(e, "name", None) or getattr(e, "key", "")] = e.value
        for ev in self._storage_env():
            env_by_name[ev.name] = ev.value
        env = [core_v1.EnvVar(name=n, value=v) for n, v in env_by_name.items()]
        container = core_v1.Container(
            name="armada-task",
            image=c.image,
            command=list(c.command),
            args=self._runtime_args(list(c.args), output_prefix, meta),
            env=env,
            # Use the image already loaded into the cluster (e.g. via `kind load`).
            imagePullPolicy="IfNotPresent",
            resources=_resources(cfg.get("cpu", "1"), cfg.get("memory", "500Mi")),
        )
        return _pod_spec(container)

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
        is_function_task = bool(container and container.image)
        if is_function_task:
            # Real function task: run Flyte's rendered container (the a0 entrypoint) in the pod.
            pod = self._pod_from_flyte_container(container, cfg, output_prefix, task_execution_metadata)
        else:
            # Placeholder task (ArmadaTask): build the pod from ArmadaConfig.
            pod = self._build_pod_spec(cfg, inputs)
        # Scope the gang_id to this run so each run forms its own gang (and concurrent runs do not
        # collide), while every gang member in the run still shares the same id.
        run_name = self._run_name(task_execution_metadata)
        if run_name and cfg.get("gang_id"):
            cfg["gang_id"] = f"{cfg['gang_id']}-{run_name}"
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
            is_function_task=is_function_task,
        )

    async def get(self, resource_meta: ArmadaJobMetadata, **kwargs) -> Resource:
        states = self.client.get_job_status([resource_meta.job_id]).job_states
        state = states.get(resource_meta.job_id, submit_pb2.UNKNOWN)
        phase = _ARMADA_STATE_TO_PHASE.get(state, TaskExecution.RUNNING)

        outputs = None
        if phase == TaskExecution.SUCCEEDED and not resource_meta.is_function_task:
            # Placeholder task: if it asked for real output, read what the pod printed; otherwise
            # (and as a fallback) synthesise the output from the template and inputs. Function tasks
            # are skipped here: a0 already wrote their real typed output to the output location.
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
