from __future__ import annotations

import pytest

from langchain_core.messages import ToolCall, ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest, ToolRuntime

from kokoro_agent.tools.ask_user import ASK_USER_TOOL_NAME
from kokoro_agent.tools.middleware import ToolPolicyMiddleware
from kokoro_agent.tools.registry import (
    BUILT_IN_TOOLS,
    RESERVED_TOOL_NAMES,
    assert_tool_names_allowed,
)


def test_default_builtin_tools_include_ask_user() -> None:
    assert [tool.name for tool in BUILT_IN_TOOLS] == [ASK_USER_TOOL_NAME]


def test_ask_user_is_kokoro_owned_not_deepagents_reserved() -> None:
    assert ASK_USER_TOOL_NAME == "ask_user"
    assert ASK_USER_TOOL_NAME not in RESERVED_TOOL_NAMES


@pytest.mark.parametrize("name", ["write_todos", "task", "read_file", "execute"])
def test_reserved_tool_name_is_rejected(name: str) -> None:
    with pytest.raises(ValueError):
        assert_tool_names_allowed([name])


def test_agent_is_not_a_default_or_reserved_kokoro_tool_name() -> None:
    assert "agent" not in RESERVED_TOOL_NAMES
    assert_tool_names_allowed(["agent"])


def test_duplicate_tool_name_is_rejected() -> None:
    with pytest.raises(ValueError):
        assert_tool_names_allowed(["custom_tool", "custom_tool"])


def test_registry_names_pass_their_own_guard() -> None:
    assert_tool_names_allowed([tool.name for tool in BUILT_IN_TOOLS])
    assert RESERVED_TOOL_NAMES.isdisjoint({tool.name for tool in BUILT_IN_TOOLS})


def test_import_time_guard_is_non_vacuous_over_the_real_registry() -> None:
    names = [tool.name for tool in BUILT_IN_TOOLS] + ["task"]
    with pytest.raises(ValueError):
        assert_tool_names_allowed(names)


@pytest.mark.asyncio
async def test_tool_policy_middleware_forwards_request_unchanged() -> None:
    middleware = ToolPolicyMiddleware()
    request = _request("search", {"q": "kokoro"})
    seen: list[dict[str, object]] = []

    async def handler(inner: ToolCallRequest) -> ToolMessage:
        seen.append(dict(inner.tool_call["args"]))
        return ToolMessage(content="ok", tool_call_id=inner.tool_call["id"] or "")

    result = await middleware.awrap_tool_call(request, handler)

    assert isinstance(result, ToolMessage)
    assert seen == [{"q": "kokoro"}]


@pytest.mark.asyncio
async def test_tool_policy_middleware_can_override_tool_args() -> None:
    middleware = ToolPolicyMiddleware(
        normalizers={
            "schedule_task": lambda args: {
                **args,
                "time": "2026-07-03T15:00:00+08:00",
            }
        }
    )
    request = _request("schedule_task", {"time": "明天下午三点"})
    seen: list[dict[str, object]] = []

    async def handler(inner: ToolCallRequest) -> ToolMessage:
        seen.append(dict(inner.tool_call["args"]))
        return ToolMessage(content="ok", tool_call_id=inner.tool_call["id"] or "")

    result = await middleware.awrap_tool_call(request, handler)

    assert isinstance(result, ToolMessage)
    assert seen == [{"time": "2026-07-03T15:00:00+08:00"}]


def _request(name: str, args: dict[str, object]) -> ToolCallRequest:
    state: dict[str, object] = {}
    return ToolCallRequest(
        tool_call=ToolCall(name=name, args=args, id="tool_1", type="tool_call"),
        tool=None,
        state=state,
        runtime=ToolRuntime(
            state=state,
            context=None,
            config={},
            stream_writer=lambda _chunk: None,
            tool_call_id="tool_1",
            store=None,
        ),
    )
