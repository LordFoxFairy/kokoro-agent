from __future__ import annotations

import json

from kokoro_agent.domain.events import SessionEvent


# SSE id 直接复用 cursor，方便 session 侧后续承接重放游标。
def format_sse(event: SessionEvent) -> bytes:
    payload = json.dumps(event, ensure_ascii=False)
    return (
        f"id: {event['cursor']}\n"
        f"event: {event['event']}\n"
        f"data: {payload}\n\n"
    ).encode("utf-8")
