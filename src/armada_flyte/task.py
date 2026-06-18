"""Authoring surface: an Armada-backed Flyte 2 task.

``ArmadaTask`` is a ``TaskTemplate`` whose ``task_type == "armada"``, which routes its
execution to :class:`armada_flyte.connector.ArmadaConnector`. Configure it with
:class:`ArmadaConfig` (queue, image, command, resources, and an ``output_template`` that the
connector renders into the task's ``result`` output once the Armada job succeeds).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Type

from flyte.connectors import AsyncConnectorExecutorMixin
from flyte.extend import AsyncFunctionTaskTemplate, TaskPluginRegistry, TaskTemplate
from flyte.models import NativeInterface, SerializationContext


@dataclass
class ArmadaConfig:
    """Per-task Armada submission config (serialised into the task template's ``custom``)."""

    queue: str = "flyte"
    job_set_id: str = "flyte-dag"
    image: str = "busybox:latest"
    command: List[str] = field(default_factory=lambda: ["sh", "-c", "echo hello from armada"])
    args: List[str] = field(default_factory=list)
    cpu: str = "100m"
    memory: str = "128Mi"
    namespace: str = "default"
    priority: int = 1
    # Rendered by the connector on success: ``.format(job_id=..., **inputs)``.
    output_template: str = "armada job {job_id} succeeded"
    # When True, the connector reads the task's result from the pod's stdout (the last line
    # beginning with ``ARMADA_RESULT:``) instead of rendering output_template. The pod's inputs
    # are exposed to the workload as upper-cased env vars (input ``dataset`` becomes ``$DATASET``).
    capture_result: bool = False
    # Gang scheduling: tasks sharing a gang_id (all declaring the same gang_cardinality) are
    # scheduled all-or-nothing together. Leave gang_id None for an ordinary job.
    gang_id: Optional[str] = None
    gang_cardinality: int = 0
    gang_node_uniformity_label: Optional[str] = None


class ArmadaTask(AsyncConnectorExecutorMixin, TaskTemplate):
    _TASK_TYPE = "armada"

    def __init__(
        self,
        name: str,
        plugin_config: ArmadaConfig,
        inputs: Optional[Dict[str, Type]] = None,
        outputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            interface=NativeInterface(
                {k: (v, None) for k, v in (inputs or {}).items()},
                outputs or {"result": str},
            ),
            task_type=self._TASK_TYPE,
            image=None,
            **kwargs,
        )
        self.plugin_config = plugin_config

    def custom_config(self, sctx: SerializationContext) -> Dict[str, Any]:
        # The serialised key set is exactly ArmadaConfig's field names, which the connector reads
        # back. asdict() deep-copies, so command/args are fresh lists the caller cannot mutate.
        return asdict(self.plugin_config)


class ArmadaFunctionTask(AsyncConnectorExecutorMixin, AsyncFunctionTaskTemplate):
    """A real Python ``@env.task`` that runs its function body inside an Armada pod.

    Registered as the plugin for ``ArmadaConfig``, so a whole environment routes to Armada::

        env = flyte.TaskEnvironment("ml", image=img, plugin_config=ArmadaConfig(queue="compute"))

        @env.task
        async def square(x: int) -> int:
            return x * x

    Flyte renders each task into a container (its ``a0`` entrypoint, which loads the code bundle,
    reads inputs from the configured blob store, runs the function, and writes outputs back). The
    connector wraps that container into an Armada job, so the function body executes in the pod.
    For function tasks the ``ArmadaConfig`` image/command/output_template/capture_result fields are
    ignored; only queue, namespace, priority, resources, and gang settings apply.
    """

    _TASK_TYPE = "armada"

    def __post_init__(self):
        super().__post_init__()
        self.task_type = self._TASK_TYPE

    def custom_config(self, sctx: SerializationContext) -> Dict[str, Any]:
        return asdict(self.plugin_config) if self.plugin_config else {}


# Wire ArmadaConfig as a TaskEnvironment plugin_config: any env created with
# plugin_config=ArmadaConfig(...) builds its tasks as ArmadaFunctionTask.
TaskPluginRegistry.register(ArmadaConfig, ArmadaFunctionTask)
