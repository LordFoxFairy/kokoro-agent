"""事件驱动层：把 StreamIntent 流编排为对外的 AgentEvent 序列（分段/审批/终态收口）。"""

from __future__ import annotations

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
from kokoro_agent.application.run_emitter import RunEmitter
from kokoro_agent.domain.agent_event import AgentEvent
from kokoro_agent.domain.stream_intent import (
    SubagentFinished,
    SubagentStarted,
    TextFinal,
    TextStream,
    ThinkingDelta,
    TodoUpdated,
    ToolInvoked,
    ToolReturned,
)
from kokoro_agent.infrastructure.stream_events import read_header, translate_stream_event


async def drive_agent_events(
    run_id: str,
    raw_events: AsyncIterator[StreamEvent],
    awaiting_tools: frozenset[str] = frozenset(),
) -> AsyncIterator[AgentEvent]:
    """以 run.started 开头、必有终态（run.completed/failed）收口，保证对外事件流自洽。

    不设 run 级墙钟超时：HITL 审批需无限等待用户操作；放弃由用户 cancel 收口。
    """
    emitter = RunEmitter(run_id)
    active_subagent: SubagentStarted | None = None
    streamed_text: str | None = None
    streamed_subagent_text: str | None = None

    def routed_subagent(event: StreamEvent) -> str | None:
        if active_subagent is None:
            return None
        current_agent_name = read_header(event).lc_agent_name
        return active_subagent.subagent_id if current_agent_name == active_subagent.name else None

    yield emitter.emit("run.started", {})
    try:
        async for raw_event in raw_events:
            for intent in translate_stream_event(raw_event):
                match intent:
                    case TextStream(text=text):
                        subagent_id = routed_subagent(raw_event)
                        if subagent_id is not None:
                            streamed_subagent_text = (streamed_subagent_text or "") + text
                            yield emitter.emit(
                                "subagent.text.delta",
                                subagent_text_payload(emitter.segment(), subagent_id, text),
                            )
                            continue
                        streamed_text = (streamed_text or "") + text
                        yield emitter.emit("text.delta", text_payload(emitter.segment(), text))

                    case TextFinal(text=text):
                        subagent_id = routed_subagent(raw_event)
                        if subagent_id is not None:
                            segment_id = emitter.segment()
                            if streamed_subagent_text is not None:
                                yield emitter.emit(
                                    "subagent.text.completed",
                                    subagent_text_payload(
                                        segment_id, subagent_id, streamed_subagent_text
                                    ),
                                )
                                streamed_subagent_text = None
                                continue
                            payload = subagent_text_payload(segment_id, subagent_id, text)
                            yield emitter.emit("subagent.text.delta", payload)
                            yield emitter.emit("subagent.text.completed", payload)
                            continue

                        segment_id = emitter.segment()
                        if streamed_text is not None:
                            yield emitter.emit(
                                "text.completed", text_payload(segment_id, streamed_text)
                            )
                            streamed_text = None
                            emitter.complete_segment()
                            continue
                        payload = text_payload(segment_id, text)
                        yield emitter.emit("text.delta", payload)
                        yield emitter.emit("text.completed", payload)
                        emitter.complete_segment()

                    case ThinkingDelta(text=text):
                        yield emitter.emit("thinking.delta", text_payload(emitter.segment(), text))

                    case TodoUpdated(todos=todos):
                        yield emitter.emit("todo.updated", todo_payload(todos))

                    case ToolInvoked() as tool:
                        payload = tool_invoked_payload(emitter.segment(), tool)
                        yield emitter.emit("tool.invoked", payload)
                        if tool.name in awaiting_tools:
                            yield emitter.emit("tool.awaiting_approval", payload)

                    case ToolReturned() as tool:
                        yield emitter.emit(
                            "tool.returned", tool_returned_payload(emitter.segment(), tool)
                        )

                    case SubagentStarted() as subagent:
                        active_subagent = subagent
                        yield emitter.emit(
                            "subagent.started",
                            subagent_started_payload(emitter.segment(), subagent),
                        )

                    case SubagentFinished() as subagent:
                        active_subagent = None
                        yield emitter.emit(
                            "subagent.finished",
                            subagent_finished_payload(emitter.segment(), subagent),
                        )

                    case _:
                        continue
        yield emitter.emit("run.completed", {"status": "completed"})
    except TimeoutError:
        # 模型/IO 级超时显式以 timeout 状态收口，不混同用户拒绝或一般失败。
        yield emitter.emit("run.completed", {"status": "timeout"})
    except Exception as error:  # noqa: BLE001 — 顶层兜底：其余异常转为 run.failed
        yield emitter.emit(
            "run.failed",
            {"error_kind": type(error).__name__, "message": str(error)},
        )
