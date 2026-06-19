"""事件驱动层：把 StreamIntent 流编排为对外的 AgentEvent 序列（分段/审批/超时收口）。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from langchain_core.runnables.schema import StreamEvent

from kokoro_agent.application.event_payloads import (
    subagent_finished_payload,
    subagent_started_payload,
    subagent_text_payload,
    text_payload,
    todo_payload,
    tool_invoked_payload,
    tool_returned_payload,
)
from kokoro_agent.domain.agent_event import AgentEvent
from kokoro_agent.infrastructure.stream_events import (
    SubagentFinished,
    SubagentStarted,
    TextFinal,
    TextStream,
    ThinkingDelta,
    TodoUpdated,
    ToolInvoked,
    ToolReturned,
    read_header,
    translate_stream_event,
)

ASTREAM_TIMEOUT_S = 120


class _Segmenter:
    """当前输出分段：首段内容或上一段结束后都会开一个全局唯一的新 segment id，
    使 tool→text→tool→text 不被并成一段。id 由 agent 分配，session 原样透传。"""

    def __init__(self, run_id: str) -> None:
        self._run_id = run_id
        self._counter = 0
        self._active: str | None = None
        self._completed = False

    def current(self) -> str:
        if self._active is None or self._completed:
            self._counter += 1
            self._active = f"{self._run_id}:seg_{self._counter:04d}"
            self._completed = False
        return self._active

    def complete(self) -> None:
        self._completed = True


async def drive_agent_events(
    run_id: str,
    raw_events: AsyncIterator[StreamEvent],
    awaiting_tools: frozenset[str] = frozenset(),
    timeout_s: float = ASTREAM_TIMEOUT_S,
) -> AsyncIterator[AgentEvent]:
    seq = 0

    def next_seq() -> int:
        nonlocal seq
        seq += 1
        return seq

    segment = _Segmenter(run_id)
    active_subagent: SubagentStarted | None = None
    streamed_text: str | None = None
    streamed_subagent_text: str | None = None

    def routed_subagent(event: StreamEvent) -> str | None:
        if active_subagent is None:
            return None
        current_agent_name = read_header(event).lc_agent_name
        return active_subagent.subagent_id if current_agent_name == active_subagent.name else None

    yield AgentEvent(kind="run.started", run_id=run_id, seq=next_seq(), payload={})
    try:
        async with asyncio.timeout(timeout_s):
            async for raw_event in raw_events:
                for intent in translate_stream_event(raw_event):
                    match intent:
                        case TextStream(text=text):
                            subagent_id = routed_subagent(raw_event)
                            if subagent_id is not None:
                                streamed_subagent_text = (streamed_subagent_text or "") + text
                                yield AgentEvent(
                                    kind="subagent.text.delta",
                                    run_id=run_id,
                                    seq=next_seq(),
                                    payload=subagent_text_payload(segment.current(), subagent_id, text),
                                )
                                continue
                            streamed_text = (streamed_text or "") + text
                            yield AgentEvent(
                                kind="text.delta",
                                run_id=run_id,
                                seq=next_seq(),
                                payload=text_payload(segment.current(), text),
                            )

                        case TextFinal(text=text):
                            subagent_id = routed_subagent(raw_event)
                            if subagent_id is not None:
                                segment_id = segment.current()
                                if streamed_subagent_text is not None:
                                    yield AgentEvent(
                                        kind="subagent.text.completed",
                                        run_id=run_id,
                                        seq=next_seq(),
                                        payload=subagent_text_payload(
                                            segment_id, subagent_id, streamed_subagent_text
                                        ),
                                    )
                                    streamed_subagent_text = None
                                    continue
                                payload = subagent_text_payload(segment_id, subagent_id, text)
                                yield AgentEvent(
                                    kind="subagent.text.delta",
                                    run_id=run_id,
                                    seq=next_seq(),
                                    payload=payload,
                                )
                                yield AgentEvent(
                                    kind="subagent.text.completed",
                                    run_id=run_id,
                                    seq=next_seq(),
                                    payload=payload,
                                )
                                continue

                            segment_id = segment.current()
                            if streamed_text is not None:
                                yield AgentEvent(
                                    kind="text.completed",
                                    run_id=run_id,
                                    seq=next_seq(),
                                    payload=text_payload(segment_id, streamed_text),
                                )
                                streamed_text = None
                                segment.complete()
                                continue
                            payload = text_payload(segment_id, text)
                            yield AgentEvent(
                                kind="text.delta",
                                run_id=run_id,
                                seq=next_seq(),
                                payload=payload,
                            )
                            yield AgentEvent(
                                kind="text.completed",
                                run_id=run_id,
                                seq=next_seq(),
                                payload=payload,
                            )
                            segment.complete()

                        case ThinkingDelta(text=text):
                            yield AgentEvent(
                                kind="thinking.delta",
                                run_id=run_id,
                                seq=next_seq(),
                                payload=text_payload(segment.current(), text),
                            )

                        case TodoUpdated(todos=todos):
                            yield AgentEvent(
                                kind="todo.updated",
                                run_id=run_id,
                                seq=next_seq(),
                                payload=todo_payload(todos),
                            )

                        case ToolInvoked() as tool:
                            payload = tool_invoked_payload(segment.current(), tool)
                            yield AgentEvent(
                                kind="tool.invoked",
                                run_id=run_id,
                                seq=next_seq(),
                                payload=payload,
                            )
                            if tool.name in awaiting_tools:
                                yield AgentEvent(
                                    kind="tool.awaiting_approval",
                                    run_id=run_id,
                                    seq=next_seq(),
                                    payload=payload,
                                )

                        case ToolReturned() as tool:
                            yield AgentEvent(
                                kind="tool.returned",
                                run_id=run_id,
                                seq=next_seq(),
                                payload=tool_returned_payload(segment.current(), tool),
                            )

                        case SubagentStarted() as subagent:
                            active_subagent = subagent
                            yield AgentEvent(
                                kind="subagent.started",
                                run_id=run_id,
                                seq=next_seq(),
                                payload=subagent_started_payload(segment.current(), subagent),
                            )

                        case SubagentFinished() as subagent:
                            active_subagent = None
                            yield AgentEvent(
                                kind="subagent.finished",
                                run_id=run_id,
                                seq=next_seq(),
                                payload=subagent_finished_payload(segment.current(), subagent),
                            )

                        case _:
                            continue
        yield AgentEvent(kind="run.completed", run_id=run_id, seq=next_seq(), payload={"status": "completed"})
    except Exception as error:  # noqa: BLE001 — 边界：任何失败都收口成 run.failed
        yield AgentEvent(
            kind="run.failed",
            run_id=run_id,
            seq=next_seq(),
            payload={"error_kind": type(error).__name__, "message": str(error)},
        )
