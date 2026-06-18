"""capture_result: get() returns the pod's real output, with a template fallback."""

from __future__ import annotations

from armada_client.armada import submit_pb2

from armada_flyte.connector import ArmadaJobMetadata


def _meta(capture: bool) -> ArmadaJobMetadata:
    return ArmadaJobMetadata(
        job_id="01job", job_set_id="x", queue="flyte",
        output_template="fallback {job_id}", inputs={}, capture_result=capture,
    )


async def test_get_uses_captured_pod_output(connector, mock_client, monkeypatch):
    mock_client.get_job_status.return_value.job_states = {"01job": submit_pb2.SUCCEEDED}
    monkeypatch.setattr(connector, "_result_from_logs", lambda job_id: "45")
    resource = await connector.get(_meta(capture=True))
    assert resource.outputs == {"result": "45"}


async def test_get_falls_back_to_template_when_no_log_result(connector, mock_client, monkeypatch):
    mock_client.get_job_status.return_value.job_states = {"01job": submit_pb2.SUCCEEDED}
    monkeypatch.setattr(connector, "_result_from_logs", lambda job_id: None)
    resource = await connector.get(_meta(capture=True))
    assert resource.outputs == {"result": "fallback 01job"}
