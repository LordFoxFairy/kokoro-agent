from __future__ import annotations

from typing import Any

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from kokoro_agent.infrastructure import stream_translator
from kokoro_agent.infrastructure.stream_translator import (
    build_runtime_custom_subagent_tool,
)
from kokoro_agent.infrastructure.subagent_registry import RuntimeSubagentRegistry


def test_runtime_registry_registers_runtime_custom_source() -> None:
    registry = RuntimeSubagentRegistry()
    spec = registry.register(
        "temp-reviewer",
        "临时审稿",
        "检查当前回答是否一致",
    )

    assert spec.source == "runtime-custom"
    assert registry.get("temp-reviewer") == spec


def test_runtime_registry_rejects_duplicate_runtime_name() -> None:
    registry = RuntimeSubagentRegistry()
    registry.register("temp-reviewer", "临时审稿", "检查一致性")

    with pytest.raises(ValueError):
        registry.register("temp-reviewer", "重复", "bad")


# --- agent_runtime 协程执行路径(StructuredTool.coroutine + .func)---


class _FakeRunner:
    def __init__(self, messages: list[Any]) -> None:
        self._messages = messages

    async def ainvoke(self, _input: dict[str, Any]) -> dict[str, Any]:
        return {"messages": self._messages}


def _patch_runner(
    monkeypatch: pytest.MonkeyPatch, messages: list[Any]
) -> list[str]:
    """替换 _make_runner，返回返回值受控的 fake；回收每次构建用的 system_prompt。"""
    seen_prompts: list[str] = []

    def fake_make_runner(_model: Any, system_prompt: str, _name: str) -> _FakeRunner:
        seen_prompts.append(system_prompt)
        return _FakeRunner(messages)

    monkeypatch.setattr(stream_translator, "_make_runner", fake_make_runner)
    return seen_prompts


async def test_agent_runtime_registers_new_name_and_returns_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = RuntimeSubagentRegistry()
    _patch_runner(monkeypatch, [HumanMessage(content="任务"), AIMessage(content="结论 A")])
    tool = build_runtime_custom_subagent_tool(model=object(), runtime_registry=registry)  # pyright: ignore[reportArgumentType]

    assert tool.coroutine is not None
    result = await tool.coroutine(
        name="reviewer", description="审稿", system_prompt="检查一致性", task="复查"
    )

    assert result == "结论 A"
    spec = registry.get("reviewer")
    assert spec is not None and spec.source == "runtime-custom"


async def test_agent_runtime_reuses_existing_spec_not_call_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = RuntimeSubagentRegistry()
    registry.register("reviewer", "原描述", "原始 system prompt")
    prompts = _patch_runner(monkeypatch, [AIMessage(content="结论 B")])
    tool = build_runtime_custom_subagent_tool(model=object(), runtime_registry=registry)  # pyright: ignore[reportArgumentType]

    assert tool.coroutine is not None
    await tool.coroutine(
        name="reviewer",
        description="新描述",
        system_prompt="篡改的 prompt",
        task="复查",
    )

    # 同名命中既有 spec：runner 用已注册的 prompt，不用本次调用参数。
    assert prompts == ["原始 system prompt"]
    assert len(registry.specs()) == 1


async def test_agent_runtime_returns_empty_string_when_no_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = RuntimeSubagentRegistry()
    _patch_runner(monkeypatch, [AIMessage(content="")])
    tool = build_runtime_custom_subagent_tool(model=object(), runtime_registry=registry)  # pyright: ignore[reportArgumentType]

    assert tool.coroutine is not None
    result = await tool.coroutine(
        name="reviewer", description="审稿", system_prompt="检查", task="复查"
    )

    assert result == ""


async def test_agent_runtime_returns_empty_string_when_no_ai_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = RuntimeSubagentRegistry()
    _patch_runner(monkeypatch, [HumanMessage(content="只有用户消息")])
    tool = build_runtime_custom_subagent_tool(model=object(), runtime_registry=registry)  # pyright: ignore[reportArgumentType]

    assert tool.coroutine is not None
    result = await tool.coroutine(
        name="reviewer", description="审稿", system_prompt="检查", task="复查"
    )

    assert result == ""


def test_agent_runtime_sync_path_raises_runtime_error() -> None:
    registry = RuntimeSubagentRegistry()
    tool = build_runtime_custom_subagent_tool(model=object(), runtime_registry=registry)  # pyright: ignore[reportArgumentType]

    assert tool.func is not None
    with pytest.raises(RuntimeError, match="requires async execution"):
        tool.func(
            name="reviewer", description="审稿", system_prompt="检查", task="复查"
        )
