from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from langchain_core.runnables.schema import StreamEvent
from pydantic import JsonValue

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
    """The open output segment: a fresh, globally-unique segment id opens on first
    content or after the previous segment completed, so tool→text→tool→text stays
    unmerged into one. The agent assigns the id; session transmits it verbatim."""

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

    def nxt() -> int:
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

    yield AgentEvent(kind="run.started", run_id=run_id, seq=nxt(), payload={})
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
                                    seq=nxt(),
                                    payload={
                                        "segment_id": segment.current(),
                                        "subagent_id": subagent_id,
                                        "text": text,
                                    },
                                )
                                continue
                            streamed_text = (streamed_text or "") + text
                            yield AgentEvent(
                                kind="text.delta",
                                run_id=run_id,
                                seq=nxt(),
                                payload={"segment_id": segment.current(), "text": text},
                            )

                        case TextFinal(text=text):
                            subagent_id = routed_subagent(raw_event)
                            if subagent_id is not None:
                                segment_id = segment.current()
                                if streamed_subagent_text is not None:
                                    yield AgentEvent(
                                        kind="subagent.text.completed",
                                        run_id=run_id,
                                        seq=nxt(),
                                        payload={
                                            "segment_id": segment_id,
                                            "subagent_id": subagent_id,
                                            "text": streamed_subagent_text,
                                        },
                                    )
                                    streamed_subagent_text = None
                                    continue
                                payload: dict[str, JsonValue] = {
                                    "segment_id": segment_id,
                                    "subagent_id": subagent_id,
                                    "text": text,
                                }
                                yield AgentEvent(
                                    kind="subagent.text.delta",
                                    run_id=run_id,
                                    seq=nxt(),
                                    payload=payload,
                                )
                                yield AgentEvent(
                                    kind="subagent.text.completed",
                                    run_id=run_id,
                                    seq=nxt(),
                                    payload=payload,
                                )
                                continue

                            segment_id = segment.current()
                            if streamed_text is not None:
                                yield AgentEvent(
                                    kind="text.completed",
                                    run_id=run_id,
                                    seq=nxt(),
                                    payload={"segment_id": segment_id, "text": streamed_text},
                                )
                                streamed_text = None
                                segment.complete()
                                continue
                            payload: dict[str, JsonValue] = {"segment_id": segment_id, "text": text}
                            yield AgentEvent(
                                kind="text.delta",
                                run_id=run_id,
                                seq=nxt(),
                                payload=payload,
                            )
                            yield AgentEvent(
                                kind="text.completed",
                                run_id=run_id,
                                seq=nxt(),
                                payload=payload,
                            )
                            segment.complete()

                        case ThinkingDelta(text=text):
                            yield AgentEvent(
                                kind="thinking.delta",
                                run_id=run_id,
                                seq=nxt(),
                                payload={"segment_id": segment.current(), "text": text},
                            )

                        case TodoUpdated(todos=todos):
                            yield AgentEvent(
                                kind="todo.updated",
                                run_id=run_id,
                                seq=nxt(),
                                payload={
                                    "todos": [
                                        {"content": todo.content, "status": todo.status} for todo in todos
                                    ]
                                },
                            )

                        case ToolInvoked(tool_id=tool_id, name=name, args=args):
                            payload: dict[str, JsonValue] = {
                                "segment_id": segment.current(),
                                "tool_id": tool_id,
                                "name": name,
                                "args": dict(args),
                            }
                            yield AgentEvent(
                                kind="tool.invoked",
                                run_id=run_id,
                                seq=nxt(),
                                payload=payload,
                            )
                            if name in awaiting_tools:
                                yield AgentEvent(
                                    kind="tool.awaiting_approval",
                                    run_id=run_id,
                                    seq=nxt(),
                                    payload=payload,
                                )

                        case ToolReturned(tool_id=tool_id, name=name, result=result, is_error=is_error, rejected=rejected):
                            payload: dict[str, JsonValue] = {
                                "segment_id": segment.current(),
                                "tool_id": tool_id,
                                "name": name,
                                "result": result,
                                "is_error": is_error,
                            }
                            if rejected:
                                payload["rejected"] = True
                            yield AgentEvent(
                                kind="tool.returned",
                                run_id=run_id,
                                seq=nxt(),
                                payload=payload,
                            )

                        case SubagentStarted() as subagent:
                            active_subagent = subagent
                            yield AgentEvent(
                                kind="subagent.started",
                                run_id=run_id,
                                seq=nxt(),
                                payload={
                                    "segment_id": segment.current(),
                                    "subagent_id": subagent.subagent_id,
                                    "name": subagent.name,
                                    "description": subagent.description,
                                    "subagent_type": subagent.subagent_type,
                                    "source": subagent.source,
                                },
                            )

                        case SubagentFinished() as subagent:
                            active_subagent = None
                            yield AgentEvent(
                                kind="subagent.finished",
                                run_id=run_id,
                                seq=nxt(),
                                payload={
                                    "segment_id": segment.current(),
                                    "subagent_id": subagent.subagent_id,
                                    "name": subagent.name,
                                    "subagent_type": subagent.subagent_type,
                                    "source": subagent.source,
                                },
                            )

                        case _:
                            continue
        yield AgentEvent(kind="run.completed", run_id=run_id, seq=nxt(), payload={"status": "completed"})
    except Exception as error:  # noqa: BLE001 — boundary: any failure -> run.failed
        yield AgentEvent(
            kind="run.failed",
            run_id=run_id,
            seq=nxt(),
            payload={"error_kind": type(error).__name__, "message": str(error)},
        )
