"""应用层：单次 run 的事件发射器——集中 seq 自增、segment 分配与 AgentEvent 构造。"""

from __future__ import annotations

from pydantic import JsonValue

from kokoro_agent.domain.agent_event import AgentEvent, AgentKind


class RunEmitter:
    """一次 run 的发射状态：seq 单调自增、segment 唯一分配，并构造对外 AgentEvent。

    segment() 在首段或上一段 complete 后开启新的全局唯一 segment id，使 tool 与 text 交替输出时不被并入同一段。
    """

    def __init__(self, run_id: str) -> None:
        self._run_id = run_id
        self._seq = 0
        self._segment_counter = 0
        self._segment: str | None = None
        self._segment_completed = False

    def emit(self, kind: AgentKind, payload: dict[str, JsonValue]) -> AgentEvent:
        self._seq += 1
        return AgentEvent(kind=kind, run_id=self._run_id, seq=self._seq, payload=payload)

    def segment(self) -> str:
        if self._segment is None or self._segment_completed:
            self._segment_counter += 1
            self._segment = f"{self._run_id}:seg_{self._segment_counter:04d}"
            self._segment_completed = False
        return self._segment

    def complete_segment(self) -> None:
        self._segment_completed = True
