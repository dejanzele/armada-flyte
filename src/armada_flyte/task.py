"""Authoring surface: an Armada-backed Flyte 2 task.

``ArmadaTask`` is a ``TaskTemplate`` whose ``task_type == "armada"``, which routes its
execution to :class:`armada_flyte.connector.ArmadaConnector`. Configure it with
:class:`ArmadaConfig` (queue, image, command, resources, and an ``output_template`` that the
connector renders into the task's ``result`` output once the Armada job succeeds).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type

from flyte.connectors import AsyncConnectorExecutorMixin
from flyte.extend import TaskTemplate
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
        config = self.plugin_config
        return {
            "queue": config.queue,
            "job_set_id": config.job_set_id,
            "image": config.image,
            "command": list(config.command),
            "args": list(config.args),
            "cpu": config.cpu,
            "memory": config.memory,
            "namespace": config.namespace,
            "priority": config.priority,
            "output_template": config.output_template,
        }
