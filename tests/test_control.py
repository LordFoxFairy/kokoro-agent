from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping

import pytest
from langchain_core.tools import StructuredTool
from pydantic import BaseModel

from kokoro_agent.application.event_stream import StreamItem
from kokoro_agent.domain.control import ControlChannelClosed
from kokoro_agent.infrastructure.control import (
    DecisionCursor,
    await_decision,
    control_stream,
)
from kokoro_agent.infrastructure.json_types import JsonValue
from kokoro_agent.infrastructure.permission import gate_tools_interactive
from kokoro_agent.infrastructure.transport import MemoryStream


class _ClosedStream:
    """control 流意外终止的最小桩：subscribe 立即耗尽（连接断开），不阻塞等待。"""

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        raise NotImplementedError

    async def read_all(self, stream: str) -> list[StreamItem]:
        return []

    async def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]:
        return
        yield  # 空 async generator：标记类型但永不产出


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
    assert (await await_decision(bus, "run_1")).decision == "approve"


async def test_await_decision_reject() -> None:
    bus = MemoryStream()
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "reject"})
    assert (await await_decision(bus, "run_1")).decision == "reject"


async def test_await_decision_cancel_propagates_to_caller() -> None:
    # cancel 是一等决定:必须原样返回给调用方,而非被静默吞掉后阻塞等下一条。
    bus = MemoryStream()
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "cancel"})
    message = await asyncio.wait_for(await_decision(bus, "run_1"), timeout=1.0)
    assert message.decision == "cancel"


async def test_await_decision_cancel_advances_cursor() -> None:
    # cancel 也是一条被消费的决定:推进游标,下一个门控工具不重读它。
    bus = MemoryStream()
    cursor = DecisionCursor()
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "cancel"})
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "reject"})
    first = await asyncio.wait_for(await_decision(bus, "run_1", cursor), timeout=1.0)
    second = await asyncio.wait_for(await_decision(bus, "run_1", cursor), timeout=1.0)
    assert first.decision == "cancel"
    assert second.decision == "reject"


async def test_await_decision_raises_on_closed_channel() -> None:
    # H3:control 流意外终止(连接断开)绝不能伪造一条 reject 回灌模型——
    # 那会让被门控工具收到误导性的"用户拒绝"结果。改为 fail-loud 抛专用异常,由 run 级取消接管。
    with pytest.raises(ControlChannelClosed):
        await await_decision(_ClosedStream(), "run_1")


async def test_interactive_gate_closed_channel_aborts_via_cancellederror() -> None:
    # 流终止经门控应转成 CancelledError(让 run_task.cancel 接管),而非 rejection_result。
    gated = gate_tools_interactive([_tool("fetch_url")], "plan", "run_1", _ClosedStream())
    with pytest.raises(asyncio.CancelledError):
        await gated[0].ainvoke({"x": "http://example.com"})  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]


async def test_await_decision_skips_message_with_unexpected_fields() -> None:
    # 安全通道:approve 携带契约外字段(注入)不被采信;后随的合法 reject 才生效。
    bus = MemoryStream()
    await bus.publish(
        control_stream("run_1"), {"kind": "control", "decision": "approve", "injected": "x"}
    )
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "reject"})
    assert (await await_decision(bus, "run_1")).decision == "reject"


async def test_interactive_gate_approve_runs_real_tool() -> None:
    bus = MemoryStream()
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "approve"})
    gated = gate_tools_interactive([_tool("fetch_url")], "plan", "run_1", bus)
    result = await gated[0].ainvoke({"x": "http://example.com"})  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    assert result == "ran fetch_url http://example.com"


async def test_interactive_gate_approve_with_edited_args_runs_with_them() -> None:
    # HITL 暂停时用户编辑参数:approve 带 args → 工具用编辑后的参数执行,而非模型原参数。
    bus = MemoryStream()
    await bus.publish(
        control_stream("run_1"),
        {"kind": "control", "decision": "approve", "args": {"x": "edited://by-user"}},
    )
    gated = gate_tools_interactive([_tool("fetch_url")], "plan", "run_1", bus)
    result = await gated[0].ainvoke({"x": "http://original"})  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    assert result == "ran fetch_url edited://by-user"


async def test_interactive_gate_reject_returns_rejection() -> None:
    bus = MemoryStream()
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "reject"})
    gated = gate_tools_interactive([_tool("fetch_url")], "plan", "run_1", bus)
    result = await gated[0].ainvoke({"x": "http://example.com"})  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    assert "拒绝" in result


async def test_interactive_gate_cancel_aborts_via_cancellederror() -> None:
    # cancel 不是"工具被拒":run 级取消独占终止,门控抛 CancelledError 让 run_task.cancel 接管,
    # 而非回 rejection_result 在取消竞态里冒出误导性的工具拒绝结果。
    bus = MemoryStream()
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "cancel"})
    gated = gate_tools_interactive([_tool("fetch_url")], "plan", "run_1", bus)
    with pytest.raises(asyncio.CancelledError):
        await gated[0].ainvoke({"x": "http://example.com"})  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]


async def test_await_decision_advances_cursor_across_tools() -> None:
    # 同 run 两个门控工具:第一个消费 approve(推进游标),第二个消费 reject——
    # 不再误读第一个的遗留决定(跨工具越权放行的修复)。
    bus = MemoryStream()
    cursor = DecisionCursor()
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "approve"})
    await bus.publish(control_stream("run_1"), {"kind": "control", "decision": "reject"})
    assert (await await_decision(bus, "run_1", cursor)).decision == "approve"
    assert (await await_decision(bus, "run_1", cursor)).decision == "reject"


async def test_interactive_gate_auto_passes_through() -> None:
    bus = MemoryStream()
    tools = [_tool("fetch_url")]
    assert gate_tools_interactive(tools, "auto", "run_1", bus) == tools
