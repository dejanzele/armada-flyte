"""ArmadaConfig defaults and how ArmadaTask serialises them into the task template."""

from __future__ import annotations

from armada_flyte import ArmadaConfig, ArmadaTask


def test_config_defaults():
    config = ArmadaConfig()
    assert config.queue == "flyte"
    assert config.image == "busybox:latest"
    assert config.command[0] == "sh"
    assert config.priority == 1


def test_custom_config_keys():
    task = ArmadaTask(
        name="t",
        plugin_config=ArmadaConfig(queue="compute", priority=5),
        inputs={"name": str},
        outputs={"result": str},
    )
    # custom_config does not use the SerializationContext, so None is fine here.
    custom = task.custom_config(None)
    assert set(custom) == {
        "queue", "job_set_id", "image", "command", "args",
        "cpu", "memory", "namespace", "priority", "output_template",
        "gang_id", "gang_cardinality", "gang_node_uniformity_label",
    }
    assert custom["queue"] == "compute"
    assert custom["priority"] == 5


def test_gang_defaults_off():
    custom = ArmadaTask(name="t", plugin_config=ArmadaConfig()).custom_config(None)
    assert custom["gang_id"] is None
    assert custom["gang_cardinality"] == 0
