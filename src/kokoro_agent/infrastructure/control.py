"""control 通道：人工审批/取消决定的解析、游标顺序消费与阻塞等待。"""

from __future__ import annotations

import logging
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError

from kokoro_agent.application.event_stream import StreamProtocol
from kokoro_agent.infrastructure.json_types import JsonObject

LOGGER = logging.getLogger(__name__)

ControlDecision = Literal["approve", "reject"]


class ControlMessage(BaseModel):
    """control 通道的人工审批消息契约；畸形载荷显式丢弃，不被误判为 reject 或非 cancel 决定。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["control"]
    decision: Literal["approve", "reject", "cancel"]


def control_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:control"


def rejection_result(tool_name: str) -> str:
    """用户主动点击 reject 时回给模型的结果文案。"""
    return f"用户拒绝了工具 {tool_name} 的调用。"


def _parse_control(event: JsonObject) -> ControlMessage | None:
    try:
        return ControlMessage.model_validate(event)
    except ValidationError:
        LOGGER.warning("dropping malformed control message: %s", event)
        return None


class DecisionCursor:
    """同一 run 内顺序消费 control 决定：每读一条推进游标，下一个门控工具从其后等待，避免后续工具误读前一工具的残留决定。"""

    def __init__(self) -> None:
        self.value: str | None = None


async def await_decision(
    bus: StreamProtocol,
    run_id: str,
    cursor: DecisionCursor | None = None,
) -> ControlDecision:
    """阻塞读取 control 流的下一条 approve/reject（从游标之后）。
    无超时自动回退：审批需持续等待用户决定；用户取消整轮时由 worker 的 cancel-watcher
    处理 cancel 并取消整个 run task，连带解除所有挂起审批的阻塞。"""
    from_cursor = cursor.value if cursor is not None else None
    async for item in bus.subscribe(control_stream(run_id), from_cursor):
        message = _parse_control(item.event)
        if message is None:
            continue
        decision = message.decision
        if decision == "cancel":
            continue
        if cursor is not None:
            cursor.value = item.cursor
        return decision
    # 流意外终止（连接断开）→ fail-closed：默认拒绝，不静默放行。
    return "reject"


async def wait_for_cancel(bus: StreamProtocol, run_id: str) -> None:
    """阻塞直到 control 流出现一条 cancel 决定（用户放弃该 run）。供 worker 取消 run task。"""
    async for item in bus.subscribe(control_stream(run_id)):
        message = _parse_control(item.event)
        if message is not None and message.decision == "cancel":
            return
