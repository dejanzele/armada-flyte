"""The Armada-to-Flyte phase mapping, and that get() applies it correctly."""

from __future__ import annotations

import pytest
from armada_client.armada import submit_pb2
from flyteidl2.core.execution_pb2 import TaskExecution

from armada_flyte.connector import _ARMADA_STATE_TO_PHASE, ArmadaJobMetadata

EXPECTED = {
    submit_pb2.QUEUED: TaskExecution.QUEUED,
    submit_pb2.SUBMITTED: TaskExecution.QUEUED,
    submit_pb2.LEASED: TaskExecution.QUEUED,
    submit_pb2.PENDING: TaskExecution.INITIALIZING,
    submit_pb2.RUNNING: TaskExecution.RUNNING,
    submit_pb2.UNKNOWN: TaskExecution.RUNNING,
    submit_pb2.SUCCEEDED: TaskExecution.SUCCEEDED,
    submit_pb2.FAILED: TaskExecution.FAILED,
    submit_pb2.REJECTED: TaskExecution.FAILED,
    submit_pb2.CANCELLED: TaskExecution.ABORTED,
    submit_pb2.PREEMPTED: TaskExecution.RETRYABLE_FAILED,
}


@pytest.mark.parametrize("state, phase", EXPECTED.items())
def test_mapping(state, phase):
    assert _ARMADA_STATE_TO_PHASE[state] == phase


def _meta() -> ArmadaJobMetadata:
    return ArmadaJobMetadata(
        job_id="01job",
        job_set_id="flyte-dag",
        queue="flyte",
        output_template="Hello, {name}! ({job_id})",
        inputs={"name": "world"},
    )


async def test_get_maps_state(connector, mock_client):
    mock_client.get_job_status.return_value.job_states = {"01job": submit_pb2.RUNNING}
    resource = await connector.get(_meta())
    assert resource.phase == TaskExecution.RUNNING
    assert resource.outputs is None


async def test_get_renders_output_on_success(connector, mock_client):
    mock_client.get_job_status.return_value.job_states = {"01job": submit_pb2.SUCCEEDED}
    resource = await connector.get(_meta())
    assert resource.phase == TaskExecution.SUCCEEDED
    assert resource.outputs == {"result": "Hello, world! (01job)"}


async def test_get_unknown_when_job_absent(connector, mock_client):
    # A job id the status map does not contain falls back to UNKNOWN, which maps to RUNNING.
    mock_client.get_job_status.return_value.job_states = {}
    resource = await connector.get(_meta())
    assert resource.phase == TaskExecution.RUNNING
