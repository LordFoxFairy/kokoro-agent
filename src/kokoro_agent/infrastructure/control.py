from __future__ import annotations

from typing import Literal

from kokoro_agent.infrastructure.stream_port import StreamPort

ControlDecision = Literal["approve", "reject"]


def control_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:control"


def rejection_result(tool_name: str) -> str:
    """用户主动点击 reject 时回给模型的结果文案。"""
    return f"用户拒绝了工具 {tool_name} 的调用。"


class DecisionCursor:
    """同一 run 内顺序消费 control 决定：每读一条推进游标，下一个门控工具从其后等待，
    杜绝第二个工具误读第一个工具的遗留决定（跨工具越权放行）。"""

    def __init__(self) -> None:
        self.value: str | None = None


async def await_decision(
    port: StreamPort,
    run_id: str,
    cursor: DecisionCursor | None = None,
) -> ControlDecision:
    """阻塞读 control 流的下一条 approve/reject（从游标之后）。
    不做超时自动回退：审批工具就该一直等用户决定；用户放弃整轮时由 worker 的 cancel-watcher
    收到 cancel 并直接取消整个 run task（连带解阻塞所有待批门）。"""
    from_cursor = cursor.value if cursor is not None else None
    async for item in port.subscribe(control_stream(run_id), from_cursor):
        decision = item.event.get("decision")
        if decision == "approve":
            if cursor is not None:
                cursor.value = item.cursor
            return "approve"
        if decision == "reject":
            if cursor is not None:
                cursor.value = item.cursor
            return "reject"
    return "reject"


async def wait_for_cancel(port: StreamPort, run_id: str) -> None:
    """阻塞直到 control 流出现一条 cancel 决定（用户放弃该 run）。供 worker 取消 run task。"""
    async for item in port.subscribe(control_stream(run_id)):
        if item.event.get("decision") == "cancel":
            return
