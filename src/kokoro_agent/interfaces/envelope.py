"""对外事件信封：消费端只见此统一外壳，绝不见 LangChain 原生流碎片。"""

from __future__ import annotations

import time
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

ExternalEvent = Literal[
    "agent_status",
    "text_chunk",
    "tool_call_start",
    "tool_call_end",
    "agent_done",
    "agent_error",
]


def _now_ms() -> int:
    return int(time.time() * 1000)


class AgentEvent(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    event: ExternalEvent
    request_id: str
    timestamp: int = Field(default_factory=_now_ms)
    data: dict[str, JsonValue]
