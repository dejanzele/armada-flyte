"""Drive the connector gRPC service directly, the way a Flyte backend would.

This is what a deployed Flyte backend (FlytePropeller) does for an `armada` task: it calls
`CreateTask` and polls `GetTask` on the connector service over gRPC. Running it here proves the
deployed code path end to end against a real Armada, without needing a full Flyte backend.

Start the service first (in another terminal), then run this:

    c0 --modules armada_flyte.connector            # serves on :8000
    ./.venv/bin/python deploy/call_service.py

Set CONNECTOR_ADDR to point at the service (default localhost:8000).
"""

from __future__ import annotations

import os
import time

import armada_flyte._proto_compat  # noqa: F401  (proto shim before armada_client)
import grpc
from flyteidl2.connector import connector_pb2 as cpb
from flyteidl2.connector.service_pb2_grpc import AsyncConnectorServiceStub
from flyteidl2.core import tasks_pb2
from flyteidl2.core.execution_pb2 import TaskExecution
from flyteidl2.task import common_pb2
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

CONNECTOR_ADDR = os.environ.get("CONNECTOR_ADDR", "localhost:8000")

# The task config a Flyte task would carry in its template's `custom` field.
CONFIG = {
    "queue": os.environ.get("ARMADA_QUEUE", "flyte"),
    "job_set_id": "deployed",
    "image": "busybox:latest",
    "command": ["sh", "-c", "echo ARMADA_RESULT:hello-from-deployed-connector"],
    "args": [],
    "cpu": "100m",
    "memory": "128Mi",
    "namespace": "default",
    "priority": 1,
    "output_template": "armada job {job_id} succeeded",
    "capture_result": True,
    "gang_id": None,
    "gang_cardinality": 0,
    "gang_node_uniformity_label": None,
}

TERMINAL = {TaskExecution.SUCCEEDED, TaskExecution.FAILED, TaskExecution.ABORTED}


def main() -> None:
    custom = Struct()
    json_format.ParseDict(CONFIG, custom)
    template = tasks_pb2.TaskTemplate(type="armada", task_type_version=0, custom=custom)

    stub = AsyncConnectorServiceStub(grpc.insecure_channel(CONNECTOR_ADDR))

    print(f"CreateTask on {CONNECTOR_ADDR} ...")
    created = stub.CreateTask(
        cpb.CreateTaskRequest(template=template, inputs=common_pb2.Inputs(), output_prefix="/tmp/deployed")
    )
    resource_meta = created.resource_meta

    category = cpb.TaskCategory(name="armada", version=0)
    while True:
        got = stub.GetTask(cpb.GetTaskRequest(task_category=category, resource_meta=resource_meta))
        phase = got.resource.phase
        print(f"  GetTask phase={TaskExecution.Phase.Name(phase)}")
        if phase in TERMINAL:
            print(f"\nterminal: {TaskExecution.Phase.Name(phase)} ({got.resource.message})")
            for named in got.resource.outputs.literals:
                print(f"  output {named.name} = {named.value.scalar.primitive.string_value!r}")
            return
        time.sleep(3)


if __name__ == "__main__":
    main()
