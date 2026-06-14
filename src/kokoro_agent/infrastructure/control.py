from __future__ import annotations

import asyncio
from typing import Literal

from kokoro_agent.infrastructure.stream_port import StreamPort

ControlDecision = Literal["approve", "reject"]

# 待批超时：无决定则回退 reject（安全默认），且在 astream 总超时之内。
APPROVAL_TIMEOUT_S = 90


def control_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:control"


class DecisionCursor:
    """同一 run 内顺序消费 control 决定：每读一条推进游标，下一个门控工具从其后等待，
    杜绝第二个工具误读第一个工具的遗留决定（跨工具越权放行）。"""

    def __init__(self) -> None:
        self.value: str | None = None


async def await_decision(
    port: StreamPort,
    run_id: str,
    cursor: DecisionCursor | None = None,
    timeout_s: float = APPROVAL_TIMEOUT_S,
) -> ControlDecision:
    """阻塞读 control 流的下一条决定（从游标之后）；超时回退 reject（绝不永久挂起）。"""
    from_cursor = cursor.value if cursor is not None else None
    try:
        async with asyncio.timeout(timeout_s):
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
    except TimeoutError:
        return "reject"
    return "reject"
