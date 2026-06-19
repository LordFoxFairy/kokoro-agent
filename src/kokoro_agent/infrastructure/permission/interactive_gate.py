"""交互门控：被拦工具运行时阻塞等待 control 流的人工审批决定。"""

from __future__ import annotations

from collections.abc import Sequence

from langchain_core.tools import StructuredTool

from kokoro_agent.domain.run_request import PermissionMode
from kokoro_agent.infrastructure.control import (
    DecisionCursor,
    await_decision,
    rejection_result,
)
from kokoro_agent.infrastructure.json_types import JsonValue
from kokoro_agent.infrastructure.permission.rules import tool_allowed
from kokoro_agent.application.event_stream import StreamProtocol


def gate_tools_interactive(
    tools: Sequence[StructuredTool],
    mode: PermissionMode,
    run_id: str,
    bus: StreamProtocol,
) -> list[StructuredTool]:
    """交互式门控：被门控工具调用时阻塞等审批（control 流），approve 跑真工具 / reject 回拒绝。
    translator 在 tool.invoked 后补 tool.awaiting_approval 让前端弹审批（见 drive_agent_events）。"""
    if mode == "auto":
        return list(tools)
    # 同一 run 的所有门控工具共享一个决定游标：决定按到达顺序逐个消费，互不串读。
    cursor = DecisionCursor()
    return [
        tool
        if tool_allowed(mode, tool.name)
        else _approval_gate(tool, run_id, bus, cursor)
        for tool in tools
    ]


def _approval_gate(
    tool: StructuredTool,
    run_id: str,
    bus: StreamProtocol,
    cursor: DecisionCursor,
) -> StructuredTool:
    async def gated_async(**kwargs: JsonValue) -> str:
        decision = await await_decision(bus, run_id, cursor)
        if decision != "approve":
            return rejection_result(tool.name)

        coroutine = tool.coroutine
        if coroutine is not None:
            return await coroutine(**kwargs)

        func = tool.func
        if func is None:
            msg = f"tool {tool.name} has no callable execution path"
            raise RuntimeError(msg)
        return func(**kwargs)

    # 纯异步包装：审批需阻塞等 control 流，无意义的 sync 路径交给 langchain 原生 NotImplementedError。
    return StructuredTool(
        name=tool.name,
        description=tool.description,
        args_schema=tool.args_schema,
        coroutine=gated_async,
    )
