from __future__ import annotations

import logging
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError

from kokoro_agent.infrastructure.json_types import JsonObject
from kokoro_agent.infrastructure.transport import StreamPort

LOGGER = logging.getLogger(__name__)

ControlDecision = Literal["approve", "reject"]


class ControlMessage(BaseModel):
    """control 通道承载人工审批决定：每条消息都过这份严格契约，
    畸形载荷被显式丢弃，而非被静默当作 reject / 非 cancel 误判。"""

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
        message = _parse_control(item.event)
        if message is None:
            continue
        decision = message.decision
        if decision == "cancel":
            continue
        if cursor is not None:
            cursor.value = item.cursor
        return decision
    return "reject"


async def wait_for_cancel(port: StreamPort, run_id: str) -> None:
    """阻塞直到 control 流出现一条 cancel 决定（用户放弃该 run）。供 worker 取消 run task。"""
    async for item in port.subscribe(control_stream(run_id)):
        message = _parse_control(item.event)
        if message is not None and message.decision == "cancel":
            return
