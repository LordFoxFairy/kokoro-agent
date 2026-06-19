from __future__ import annotations

from langchain_core.tools import StructuredTool
from pydantic import BaseModel

from kokoro_agent.infrastructure.control import (
    DecisionCursor,
    await_decision,
    control_stream,
)
from kokoro_agent.infrastructure.permission import gate_tools_interactive
from kokoro_agent.infrastructure.transport import MemoryStream


class _Args(BaseModel):
    x: str


def _tool(name: str = "fetch_url") -> StructuredTool:
    def _sync(x: str) -> str:
        return f"ran {name} {x}"

    async def _run(x: str) -> str:
        return f"ran {name} {x}"

    return StructuredTool.from_function(  # pyright: ignore[reportUnknownMemberType]
        name=name,
        description=name,
        func=_sync,
        coroutine=_run,
        args_schema=_Args,
        infer_schema=False,
    )


async def test_await_decision_approve() -> None:
    bus = MemoryStream()
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "approve"})
    assert await await_decision(bus, "run_1") == "approve"


async def test_await_decision_reject() -> None:
    bus = MemoryStream()
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "reject"})
    assert await await_decision(bus, "run_1") == "reject"


async def test_await_decision_skips_message_with_unexpected_fields() -> None:
    # 安全通道:approve 携带契约外字段(注入)不被采信;后随的合法 reject 才生效。
    bus = MemoryStream()
    await bus.publish(
        control_stream("run_1"), {"kind": "control", "decision": "approve", "injected": "x"}
    )
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "reject"})
    assert await await_decision(bus, "run_1") == "reject"


async def test_interactive_gate_approve_runs_real_tool() -> None:
    bus = MemoryStream()
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "approve"})
    gated = gate_tools_interactive([_tool("fetch_url")], "plan", "run_1", bus)
    result = await gated[0].ainvoke({"x": "http://example.com"})  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    assert result == "ran fetch_url http://example.com"


async def test_interactive_gate_reject_returns_rejection() -> None:
    bus = MemoryStream()
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "reject"})
    gated = gate_tools_interactive([_tool("fetch_url")], "plan", "run_1", bus)
    result = await gated[0].ainvoke({"x": "http://example.com"})  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    assert "拒绝" in result


async def test_await_decision_advances_cursor_across_tools() -> None:
    # 同 run 两个门控工具:第一个消费 approve(推进游标),第二个消费 reject——
    # 不再误读第一个的遗留决定(跨工具越权放行的修复)。
    bus = MemoryStream()
    cursor = DecisionCursor()
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "approve"})
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "reject"})
    assert await await_decision(bus, "run_1", cursor) == "approve"
    assert await await_decision(bus, "run_1", cursor) == "reject"


async def test_interactive_gate_auto_passes_through() -> None:
    bus = MemoryStream()
    tools = [_tool("fetch_url")]
    assert gate_tools_interactive(tools, "auto", "run_1", bus) == tools
