from __future__ import annotations

import asyncio
from typing import Literal

from kokoro_agent.infrastructure.stream_port import StreamPort

ControlDecision = Literal["approve", "reject"]

# 待批超时：无决定则回退 reject（安全默认），且在 astream 总超时（120s）之内。
APPROVAL_TIMEOUT_S = 90


def control_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:control"


async def await_decision(
    port: StreamPort, run_id: str, timeout_s: float = APPROVAL_TIMEOUT_S
) -> ControlDecision:
    """阻塞读 control 流首条决定；超时回退 reject（绝不永久挂起）。"""
    try:
        async with asyncio.timeout(timeout_s):
            async for item in port.subscribe(control_stream(run_id)):
                decision = item.event.get("decision")
                if decision == "approve":
                    return "approve"
                if decision == "reject":
                    return "reject"
    except TimeoutError:
        return "reject"
    return "reject"
