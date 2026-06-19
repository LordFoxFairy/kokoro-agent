"""读取事件头：从未类型化 StreamEvent 提取 EventHeader。"""

from __future__ import annotations

from langchain_core.runnables.schema import StreamEvent

from kokoro_agent.infrastructure.stream_events.parsed_event import EventHeader


def read_header(event: StreamEvent) -> EventHeader:
    match event:
        case {
            "event": str() as kind,
            "name": str() as name,
            "run_id": str() as run_id,
            "metadata": {"lc_agent_name": str() as lc_agent_name},
        }:
            return EventHeader(kind, name, run_id, lc_agent_name)
        case {
            "event": str() as kind,
            "name": str() as name,
            "metadata": {"lc_agent_name": str() as lc_agent_name},
        }:
            return EventHeader(kind, name, "", lc_agent_name)
        case {"event": str() as kind, "name": str() as name, "run_id": str() as run_id}:
            return EventHeader(kind, name, run_id, "")
        case {"event": str() as kind, "name": str() as name}:
            return EventHeader(kind, name, "", "")
        case _:
            return EventHeader("", "", "", "")
