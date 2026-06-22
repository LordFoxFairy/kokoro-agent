from __future__ import annotations

from collections.abc import Mapping
from typing import TypeGuard

from kokoro_agent.infrastructure.model import make_local_fake_chat_model
from kokoro_agent.infrastructure.subagent import materialize_runtime_subagents


def _is_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    # 把未类型化值收口到 Mapping[object, object]，避免 .get() 返回 Unknown
    return isinstance(value, Mapping)


def _injected_agent_name(spec: Mapping[str, object]) -> object:
    # 预编译子代理的注入键落在 runnable 自身 config.metadata（with_config 就地合并）。
    runnable = spec.get("runnable")
    config = getattr(runnable, "config", None)
    if not _is_mapping(config):
        return None
    metadata = config.get("metadata")
    if not _is_mapping(metadata):
        return None
    return metadata.get("agent_name")


def test_materialized_subagents_carry_runnable() -> None:
    specs = materialize_runtime_subagents(make_local_fake_chat_model())
    assert specs, "expected at least the built-in researcher"
    for spec in specs:
        assert "runnable" in spec
        assert "system_prompt" not in spec


def test_runnable_binds_injected_agent_name_metadata() -> None:
    specs = materialize_runtime_subagents(make_local_fake_chat_model())
    for spec in specs:
        assert _injected_agent_name(spec) == spec["name"]
