from __future__ import annotations

from collections.abc import Mapping

from kokoro_agent.infrastructure.model import make_local_fake_chat_model
from kokoro_agent.infrastructure.subagent import materialize_runtime_subagents


def _injected_agent_name(runnable: object) -> object:
    # 预编译子代理的注入键落在 runnable 自身 config.metadata（with_config 就地合并）。
    config = getattr(runnable, "config", None)
    metadata = config.get("metadata") if isinstance(config, Mapping) else None
    return metadata.get("agent_name") if isinstance(metadata, Mapping) else None


def test_materialized_subagents_carry_runnable() -> None:
    specs = materialize_runtime_subagents(make_local_fake_chat_model())
    assert specs, "expected at least the built-in researcher"
    for spec in specs:
        assert "runnable" in spec
        assert "system_prompt" not in spec


def test_runnable_binds_injected_agent_name_metadata() -> None:
    specs = materialize_runtime_subagents(make_local_fake_chat_model())
    for spec in specs:
        assert _injected_agent_name(spec["runnable"]) == spec["name"]
