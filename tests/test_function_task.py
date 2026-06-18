"""Real Python @env.task: plugin_config routing and connector container-wrapping."""

from __future__ import annotations

from types import SimpleNamespace

import flyte

from armada_flyte import ArmadaConfig, ArmadaFunctionTask


def test_plugin_config_builds_a_function_task():
    env = flyte.TaskEnvironment("e", plugin_config=ArmadaConfig(queue="q"))

    @env.task
    async def f(x: int) -> int:
        return x * x

    assert isinstance(f, ArmadaFunctionTask)
    assert f.task_type == "armada"


async def test_create_wraps_the_flyte_container(connector, mock_client):
    # A real function task carries a rendered container (the a0 entrypoint). The connector must
    # submit THAT, not the placeholder workload.
    mock_client.submit_jobs.return_value = SimpleNamespace(
        job_response_items=[SimpleNamespace(job_id="01job", error="")]
    )
    container = SimpleNamespace(
        image="task-img:latest", command=[], args=["a0", "--inputs", "s3://b/i.pb"], env=[]
    )
    task_template = SimpleNamespace(custom=None, container=container)

    await connector.create(task_template, inputs={})

    pod = mock_client.create_job_request_item.call_args.kwargs["pod_spec"]
    assert pod.containers[0].image == "task-img:latest"
    assert list(pod.containers[0].args) == ["a0", "--inputs", "s3://b/i.pb"]
