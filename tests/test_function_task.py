"""Real Python @env.task: plugin_config routing and connector container-wrapping."""

from __future__ import annotations

from types import SimpleNamespace

import flyte

from armada_flyte import ArmadaConfig, ArmadaFunctionTask
from armada_flyte.connector import ArmadaConnector


def _backend_meta():
    # Mirrors the TaskExecutionMetadata a deployed backend passes to create().
    return SimpleNamespace(
        task_execution_id=SimpleNamespace(
            task_id=SimpleNamespace(project="flytesnacks", domain="development"),
            node_execution_id=SimpleNamespace(
                node_id="a0", execution_id=SimpleNamespace(name="run123")
            ),
        ),
        labels={"organization": "acme"},
    )


def test_runtime_args_fills_backend_placeholders():
    # The backend leaves {{.runName}}/{{.actionName}} unfilled and omits --run-base-dir/--org.
    prefix = "s3://b/sh/flytesnacks/development/run123/a0/1"
    args = ["a0", "--run-name", "{{.runName}}", "--name", "{{.actionName}}"]
    out = ArmadaConnector._runtime_args(args, prefix, _backend_meta())
    assert "{{.runName}}" not in out and "run123" in out
    assert "{{.actionName}}" not in out and "a0" in out
    assert out[out.index("--run-base-dir") + 1] == "s3://b/sh/flytesnacks/development/run123"
    assert out[out.index("--org") + 1] == "acme"
    assert out[out.index("--project") + 1] == "flytesnacks"


def test_runtime_args_noop_when_already_complete():
    # Local execution renders complete args; nothing to add (no placeholders, run-base-dir present).
    args = ["a0", "--run-base-dir", "s3://b/base", "--org", "o", "--inputs", "s3://b/i.pb"]
    assert ArmadaConnector._runtime_args(list(args), "", None) == args


def test_run_name_extraction():
    # The run name (shared by every action in a run) scopes the gang id so runs do not collide.
    assert ArmadaConnector._run_name(_backend_meta()) == "run123"
    assert ArmadaConnector._run_name(None) == ""


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
