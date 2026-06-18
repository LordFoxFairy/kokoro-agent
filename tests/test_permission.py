from __future__ import annotations

from collections.abc import Awaitable, Callable
from inspect import Parameter, signature
from typing import cast, get_type_hints

from langchain_core.tools import StructuredTool
from pydantic import BaseModel

from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.approval_policy import approval_policy
from kokoro_agent.infrastructure.permission import (
    blocked_tools,
    fs_permissions,
    gate_tools,
    gate_tools_interactive,
    tool_allowed,
)
from kokoro_agent.infrastructure.json_types import JsonValue
from kokoro_agent.infrastructure.transport import MemoryStreamPort


def test_fs_permissions_plan_read_only_else_unrestricted() -> None:
    assert fs_permissions("auto") == []
    assert fs_permissions("default") == []
    rules = fs_permissions("plan")
    assert len(rules) == 1
    rule = rules[0]
    assert rule.mode == "deny"
    assert "write" in rule.operations
    assert "read" not in rule.operations


def test_blocked_tools_driven_by_declarative_approval_policy() -> None:
    policy = approval_policy()
    assert "fetch_url" in policy.requires_approval_tools
    assert blocked_tools("auto") == frozenset()
    assert blocked_tools("default") == policy.requires_approval_tools
    assert blocked_tools("plan") >= policy.requires_approval_tools
    assert "agent" in blocked_tools("plan")


def test_run_request_defaults_permission_mode_auto() -> None:
    req = RunRequest(
        kind="run.request",
        run_id="run_1",
        session_id="ses_1",
        conversation_id="conv_1",
        input="hi",
    )
    assert req.permission_mode == "auto"


class _Args(BaseModel):
    x: str


def _make(name: str) -> StructuredTool:
    def _run(x: str) -> str:
        return f"ran {name} {x}"

    return StructuredTool(
        name=name,
        description=name,
        func=_run,
        args_schema=_Args,
    )


async def _make_async_only(name: str) -> StructuredTool:
    async def _run(x: str) -> str:
        return f"ran {name} {x}"

    def _sync(**_kwargs: JsonValue) -> str:
        raise RuntimeError("sync path should not run")

    return StructuredTool(
        name=name,
        description=name,
        func=_sync,
        coroutine=_run,
        args_schema=_Args,
    )


def test_tool_allowed_matrix() -> None:
    assert tool_allowed("auto", "fetch_url")
    assert tool_allowed("auto", "agent")
    assert not tool_allowed("default", "fetch_url")
    assert tool_allowed("default", "now")
    assert tool_allowed("default", "agent")
    assert not tool_allowed("plan", "fetch_url")
    assert not tool_allowed("plan", "agent")
    assert tool_allowed("plan", "now")


def test_gate_auto_passes_through_unchanged() -> None:
    tools = [_make("fetch_url"), _make("now")]
    gated = gate_tools(tools, "auto")
    assert all(a is b for a, b in zip(gated, tools, strict=True))


def test_gate_plan_wraps_blocked_keeps_allowed() -> None:
    fetch = _make("fetch_url")
    now = _make("now")
    gated = {t.name: t for t in gate_tools([fetch, now], "plan")}

    assert gated["now"] is now
    assert gated["fetch_url"] is not fetch

    blocked = gated["fetch_url"]
    assert blocked.func is not None
    result = blocked.func(x="http://example.com")
    assert "拦截" in result
    assert "plan" in result


def test_permission_gate_wrappers_expose_narrow_sync_signatures() -> None:
    blocked = gate_tools([_make("fetch_url")], "plan")[0]
    blocked_sync = blocked.func
    assert blocked_sync is not None
    params = signature(blocked_sync).parameters
    hints = get_type_hints(blocked_sync)
    assert set(params) == {"_args", "_kwargs"}
    assert params["_args"].kind is Parameter.VAR_POSITIONAL
    assert params["_kwargs"].kind is Parameter.VAR_KEYWORD
    assert "JsonValue" in str(hints["_args"])
    assert "JsonValue" in str(hints["_kwargs"])
    assert hints["return"] is str


async def test_interactive_gate_wrappers_expose_narrow_sync_signature() -> None:
    port = MemoryStreamPort()
    blocked = gate_tools_interactive([_make("fetch_url")], "plan", "run_1", port)[0]
    blocked_sync = blocked.func
    blocked_async = blocked.coroutine
    assert blocked_sync is not None
    assert blocked_async is not None
    params = signature(blocked_sync).parameters
    sync_hints = get_type_hints(blocked_sync)
    async_hints = get_type_hints(blocked_async)
    assert set(params) == {"_kwargs"}
    assert params["_kwargs"].kind is Parameter.VAR_KEYWORD
    assert "JsonValue" in str(sync_hints["_kwargs"])
    assert sync_hints["return"] is str
    assert "JsonValue" in str(async_hints["kwargs"])
    assert async_hints["return"] is str


async def test_gate_plan_blocks_async_only_tool() -> None:
    gated = gate_tools([await _make_async_only("agent")], "plan")
    blocked = gated[0]
    blocked_async = cast("Callable[..., Awaitable[str]] | None", blocked.coroutine)
    assert blocked_async is not None
    result = await blocked_async(x="http://example.com")
    assert "拦截" in result
    assert "plan" in result
