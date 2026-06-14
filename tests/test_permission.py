from __future__ import annotations

from langchain_core.tools import StructuredTool
from pydantic import BaseModel

from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.permission import gate_tools, tool_allowed


def test_run_request_defaults_permission_mode_auto() -> None:
    # 默认 auto：不传即全放行，保持现有行为不破。
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

    return StructuredTool.from_function(  # pyright: ignore[reportUnknownMemberType]
        name=name,
        description=name,
        func=_run,
        args_schema=_Args,
        infer_schema=False,
    )


def test_tool_allowed_matrix() -> None:
    assert tool_allowed("auto", "fetch_url")
    assert tool_allowed("auto", "agent")
    # default 拦外部副作用 fetch_url，放行 now / 子代理
    assert not tool_allowed("default", "fetch_url")
    assert tool_allowed("default", "now")
    assert tool_allowed("default", "agent")
    # plan 只读：拦 fetch_url + runtime 子代理 agent，放行 now
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

    assert gated["now"] is now  # 放行的原样保留
    assert gated["fetch_url"] is not fetch  # 被拦的被包装

    # 被拦工具执行返回拦截结果（模型据此调整），不真正执行原逻辑。
    blocked = gated["fetch_url"]
    assert blocked.func is not None
    result = blocked.func(x="http://example.com")
    assert "拦截" in result
    assert "plan" in result
