"""create() builds a job from the task config and returns a handle, without a live Armada."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock


async def test_create_submits_and_returns_handle(connector, mock_client, make_custom):
    mock_client.submit_jobs.return_value = SimpleNamespace(
        job_response_items=[SimpleNamespace(job_id="01created", error="")]
    )

    # A placeholder task declares cpu/memory (Armada requires resources); other fields default.
    task_template = SimpleNamespace(custom=make_custom())
    meta = await connector.create(task_template, inputs={"name": "world"})

    assert meta.job_id == "01created"
    assert meta.queue == "flyte"
    assert meta.job_set_id == "flyte-dag"
    assert meta.inputs == {"name": "world"}

    # A pod spec was built and submitted to the default queue.
    _, kwargs = mock_client.create_job_request_item.call_args
    assert kwargs["pod_spec"].containers[0].image == "busybox:latest"
    assert mock_client.submit_jobs.call_args.kwargs["queue"] == "flyte"


async def test_create_raises_on_armada_error(connector, mock_client, make_custom):
    mock_client.submit_jobs.return_value = SimpleNamespace(
        job_response_items=[SimpleNamespace(job_id="", error="queue does not exist")]
    )
    try:
        await connector.create(SimpleNamespace(custom=make_custom()), inputs={})
        assert False, "expected create() to raise on submission error"
    except RuntimeError as e:
        assert "queue does not exist" in str(e)


async def test_create_requires_resources(connector, mock_client):
    # Armada rejects a job with no resources; the connector fails fast with a clear error.
    try:
        await connector.create(SimpleNamespace(custom=None), inputs={})
        assert False, "expected create() to require resources"
    except ValueError as e:
        assert "resource" in str(e).lower()


async def test_delete_cancels_job(connector, mock_client):
    from armada_flyte.connector import ArmadaJobMetadata

    await connector.delete(ArmadaJobMetadata(job_id="01job", job_set_id="flyte-dag", queue="flyte"))
    mock_client.cancel_jobs.assert_called_once()
