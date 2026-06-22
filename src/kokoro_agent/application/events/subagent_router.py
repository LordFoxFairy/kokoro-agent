"""应用层：子智能体路由——按 lc_agent_name 判定某文本事件是否属于当前活跃子智能体。"""

from __future__ import annotations

from langchain_core.runnables.schema import StreamEvent

from kokoro_agent.domain.stream_intent import SubagentStarted
from kokoro_agent.infrastructure.stream_events import read_header


class SubagentRouter:
    """当前活跃子智能体的归属判定：started 进入、finished 退出，route 命中返回其 id。"""

    def __init__(self) -> None:
        self._active: SubagentStarted | None = None

    def started(self, subagent: SubagentStarted) -> None:
        self._active = subagent

    def finished(self) -> None:
        self._active = None

    def route(self, event: StreamEvent) -> str | None:
        if self._active is None:
            return None
        agent_name = read_header(event).lc_agent_name
        return self._active.subagent_id if agent_name == self._active.name else None
