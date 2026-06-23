from __future__ import annotations

from inspect import Parameter, signature
from pathlib import Path
from typing import get_type_hints

import pytest
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ValidationError

from kokoro_agent.domain.run_request import PermissionMode, RunRequest
from kokoro_agent.infrastructure.permission import approval_policy
from kokoro_agent.infrastructure.permission import (
    blocked_tools,
    gate_tools,
    load_approval_policy,
    tool_allowed,
)
from kokoro_agent.infrastructure.json_types import JsonValue


def test_blocked_tools_driven_by_declarative_approval_policy() -> None:
    policy = approval_policy()
    assert "fetch_url" in policy.requires_approval_tools
    assert blocked_tools("auto") == frozenset()
    assert blocked_tools("default") == policy.requires_approval_tools


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


def _make(name: str, description: str | None = None) -> StructuredTool:
    def _run(x: str) -> str:
        return f"ran {name} {x}"

    return StructuredTool(
        name=name,
        description=name if description is None else description,
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


@pytest.mark.parametrize(
    ("mode", "tool_name", "allowed"),
    [
        # auto 永不拦：敏感工具与子代理工具都放行。
        ("auto", "fetch_url", True),
        ("auto", "agent", True),
        # default 拦需审批集，放行普通工具与子代理（开子代理无外部副作用，无需审批）。
        ("default", "fetch_url", False),
        ("default", "now", True),
        ("default", "agent", True),
    ],
)
def test_tool_allowed_matrix(
    mode: PermissionMode, tool_name: str, allowed: bool
) -> None:
    assert tool_allowed(mode, tool_name) is allowed


# 边界：策略按精确名做集合成员判定，未命中的奇异名（特殊字符/空串/unicode）一律放行，不崩。
@pytest.mark.parametrize("mode", ["auto", "default"])
@pytest.mark.parametrize(
    "tool_name",
    ["", "fetch_url; rm -rf /", "FETCH_URL", "fetch url", "工具😀", "  fetch_url  "],
)
def test_tool_allowed_unknown_exotic_names_pass(
    mode: PermissionMode, tool_name: str
) -> None:
    assert tool_allowed(mode, tool_name) is True


def test_gate_auto_passes_through_unchanged() -> None:
    tools = [_make("fetch_url"), _make("now")]
    gated = gate_tools(tools, "auto")
    assert all(a is b for a, b in zip(gated, tools, strict=True))


def test_gate_default_wraps_blocked_keeps_allowed() -> None:
    fetch = _make("fetch_url")
    now = _make("now")
    gated = {t.name: t for t in gate_tools([fetch, now], "default")}

    assert gated["now"] is now
    assert gated["fetch_url"] is not fetch

    blocked = gated["fetch_url"]
    assert blocked.func is not None
    result = blocked.func(x="http://example.com")
    assert "拦截" in result
    assert "default" in result


def test_permission_gate_wrappers_expose_narrow_sync_signatures() -> None:
    blocked = gate_tools([_make("fetch_url")], "default")[0]
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


async def test_gate_default_blocks_async_only_tool() -> None:
    gated = gate_tools([await _make_async_only("fetch_url")], "default")
    blocked = gated[0]
    blocked_async = blocked.coroutine
    assert blocked_async is not None
    result = await blocked_async(x="http://example.com")
    assert "拦截" in result
    assert "default" in result


# 边界：被拦工具描述为空串时仍能被包装并产出拦截桩（StructuredTool 不接受 None 描述）。
@pytest.mark.parametrize("description", ["", "fetch a url"])
def test_gate_blocked_tool_preserves_description(description: str) -> None:
    blocked = gate_tools([_make("fetch_url", description=description)], "default")[0]
    assert blocked.description == description
    assert blocked.func is not None
    result = blocked.func(x="http://example.com")
    assert "拦截" in result
    assert "default" in result


# 边界：畸形 policy 必须被 Pydantic strict/forbid 拦下，绝不静默放过脏配置。
@pytest.mark.parametrize(
    "yaml_text",
    [
        "ignored: true\n",  # 缺 requires_approval_tools
        "requires_approval_tools: [a]\nextra: 1\n",  # 多余字段
        "requires_approval_tools: fetch_url\n",  # 类型错（非列表）
        'requires_approval_tools: [""]\n',  # 空工具名违反 min_length
        "- a\n- b\n",  # 根非映射
    ],
)
def test_load_approval_policy_rejects_malformed(
    tmp_path: Path, yaml_text: str
) -> None:
    path = tmp_path / "approval_policy.yaml"
    path.write_text(yaml_text, encoding="utf-8")
    with pytest.raises(ValidationError):
        load_approval_policy(path)
