from __future__ import annotations

import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from pydantic import ValidationError

from kokoro_agent.infrastructure.model import make_local_fake_chat_model
from kokoro_agent.infrastructure.tools import runtime_subagent
from kokoro_agent.infrastructure.tools.runtime_subagent import (
    RuntimeSubagentToolInput,
    build_runtime_custom_subagent_tool,
)
from kokoro_agent.infrastructure.subagent import CUSTOM_SUBAGENTS_ENV, RuntimeSubagentRegistry


def _valid_input_fields() -> dict[str, str]:
    return {
        "name": "reviewer",
        "description": "审稿",
        "system_prompt": "检查一致性",
        "task": "复查",
    }


def test_tool_input_rejects_unknown_field() -> None:
    # LLM 边界：未知字段必须被拒（extra='forbid'），不得静默吞下。
    payload = {**_valid_input_fields(), "injected": "x"}
    with pytest.raises(ValidationError):
        RuntimeSubagentToolInput.model_validate(payload)


def test_tool_input_rejects_non_string_value() -> None:
    # strict 模式：类型不符不得隐式转换（lax 会把 bytes 解码为 str）。
    payload = {**_valid_input_fields(), "name": b"reviewer"}
    with pytest.raises(ValidationError):
        RuntimeSubagentToolInput.model_validate(payload)


def test_tool_input_accepts_well_formed_payload() -> None:
    parsed = RuntimeSubagentToolInput.model_validate(_valid_input_fields())
    assert parsed.name == "reviewer"


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


class _FakeRunner:
    def __init__(self, result: object) -> None:
        self._result = result

    async def ainvoke(self, _input: dict[str, list[dict[str, str]]]) -> object:
        return self._result


def _message_result(*messages: BaseMessage) -> dict[str, object]:
    return {"messages": list(messages)}


def _patch_runner(
    monkeypatch: pytest.MonkeyPatch, result: object
) -> list[str]:
    seen_prompts: list[str] = []

    def fake_make_runner(_model: BaseChatModel, system_prompt: str, _name: str) -> _FakeRunner:
        seen_prompts.append(system_prompt)
        return _FakeRunner(result)

    monkeypatch.setattr(runtime_subagent, "_make_runner", fake_make_runner)
    return seen_prompts


async def test_agent_runtime_returns_empty_for_non_mapping_runner_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # runner 返回非 Mapping（不透明对象边界），工具须降级为空串而非崩溃。
    registry = RuntimeSubagentRegistry()
    _patch_runner(monkeypatch, ["not", "mapping"])
    tool = build_runtime_custom_subagent_tool(model=make_local_fake_chat_model(), runtime_registry=registry)

    assert tool.coroutine is not None
    result = await tool.coroutine(
        name="reviewer", description="审稿", system_prompt="检查", task="复查"
    )

    assert result == ""


async def test_agent_runtime_registers_new_name_and_returns_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = RuntimeSubagentRegistry()
    _patch_runner(
        monkeypatch,
        _message_result(HumanMessage(content="任务"), AIMessage(content="结论 A")),
    )
    tool = build_runtime_custom_subagent_tool(model=make_local_fake_chat_model(), runtime_registry=registry)

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
    prompts = _patch_runner(monkeypatch, _message_result(AIMessage(content="结论 B")))
    tool = build_runtime_custom_subagent_tool(model=make_local_fake_chat_model(), runtime_registry=registry)

    assert tool.coroutine is not None
    await tool.coroutine(
        name="reviewer",
        description="原描述",
        system_prompt="原始 system prompt",
        task="复查",
    )

    assert prompts == ["原始 system prompt"]
    assert len(registry.specs()) == 1


async def test_agent_runtime_normalizes_name_and_reuses_existing_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = RuntimeSubagentRegistry()
    registry.register("reviewer", "原描述", "原始 system prompt")
    prompts = _patch_runner(monkeypatch, _message_result(AIMessage(content="结论 C")))
    tool = build_runtime_custom_subagent_tool(model=make_local_fake_chat_model(), runtime_registry=registry)

    assert tool.coroutine is not None
    result = await tool.coroutine(
        name=" reviewer ",
        description=" 原描述 ",
        system_prompt=" 原始 system prompt ",
        task=" 复查 ",
    )

    assert result == "结论 C"
    assert prompts == ["原始 system prompt"]
    assert len(registry.specs()) == 1


async def test_agent_runtime_rejects_conflicting_definition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = RuntimeSubagentRegistry()
    registry.register("reviewer", "原描述", "原始 system prompt")
    _patch_runner(monkeypatch, _message_result(AIMessage(content="结论 B")))
    tool = build_runtime_custom_subagent_tool(model=make_local_fake_chat_model(), runtime_registry=registry)

    assert tool.coroutine is not None
    with pytest.raises(ValueError, match="conflicting runtime subagent definition"):
        await tool.coroutine(
            name="reviewer",
            description="新描述",
            system_prompt="篡改的 prompt",
            task="复查",
        )


async def test_agent_runtime_rejects_env_duplicate_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        CUSTOM_SUBAGENTS_ENV,
        '[{"name":"reviewer","description":"审稿","system_prompt":"检查内容质量"}]',
    )
    registry = RuntimeSubagentRegistry()
    _patch_runner(monkeypatch, _message_result(AIMessage(content="结论 B")))
    tool = build_runtime_custom_subagent_tool(model=make_local_fake_chat_model(), runtime_registry=registry)

    assert tool.coroutine is not None
    with pytest.raises(ValueError, match="duplicate or reserved subagent name"):
        await tool.coroutine(
            name="reviewer",
            description="新描述",
            system_prompt="新 prompt",
            task="复查",
        )


async def test_agent_runtime_returns_empty_string_when_no_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = RuntimeSubagentRegistry()
    _patch_runner(monkeypatch, _message_result(AIMessage(content="")))
    tool = build_runtime_custom_subagent_tool(model=make_local_fake_chat_model(), runtime_registry=registry)

    assert tool.coroutine is not None
    result = await tool.coroutine(
        name="reviewer", description="审稿", system_prompt="检查", task="复查"
    )

    assert result == ""


async def test_agent_runtime_returns_empty_string_when_no_ai_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = RuntimeSubagentRegistry()
    _patch_runner(monkeypatch, _message_result(HumanMessage(content="只有用户消息")))
    tool = build_runtime_custom_subagent_tool(model=make_local_fake_chat_model(), runtime_registry=registry)

    assert tool.coroutine is not None
    result = await tool.coroutine(
        name="reviewer", description="审稿", system_prompt="检查", task="复查"
    )

    assert result == ""


def test_runtime_subagent_tool_is_async_only() -> None:
    registry = RuntimeSubagentRegistry()
    tool = build_runtime_custom_subagent_tool(model=make_local_fake_chat_model(), runtime_registry=registry)

    assert tool.func is None
    with pytest.raises(NotImplementedError):
        tool.run(
            {"name": "reviewer", "description": "审稿", "system_prompt": "检查", "task": "复查"}
        )
