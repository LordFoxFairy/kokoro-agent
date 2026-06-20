"""control 通道 IO 协调：流地址、游标顺序消费与阻塞等待（契约见 domain.control）。"""

from __future__ import annotations

import logging

from pydantic import ValidationError

from kokoro_agent.application.event_stream import StreamProtocol
from kokoro_agent.domain.control import ControlChannelClosed, ControlMessage
from kokoro_agent.domain.json_payload import JsonObject

LOGGER = logging.getLogger(__name__)


def control_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:control"


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
) -> ControlMessage:
    """阻塞读取 control 流的下一条决定（含可选编辑后 args，从游标之后）。
    无超时自动回退：审批需持续等待用户决定。cancel 原样返回给调用方，由其决定终止流程，
    不在此静默吞掉。流意外终止抛 ControlChannelClosed，绝不伪造 reject。"""
    from_cursor = cursor.value if cursor is not None else None
    async for item in bus.subscribe(control_stream(run_id), from_cursor):
        message = _parse_control(item.event)
        if message is None:
            continue
        if cursor is not None:
            cursor.value = item.cursor
        return message
    raise ControlChannelClosed(run_id)


async def wait_for_cancel(bus: StreamProtocol, run_id: str) -> None:
    """阻塞直到 control 流出现一条 cancel 决定（用户放弃该 run）。供 worker 取消 run task。"""
    async for item in bus.subscribe(control_stream(run_id)):
        message = _parse_control(item.event)
        if message is not None and message.decision == "cancel":
            return
