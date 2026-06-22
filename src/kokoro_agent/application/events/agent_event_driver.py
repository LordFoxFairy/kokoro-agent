"""事件驱动层：把 StreamIntent 流编排为对外的 AgentEvent 序列（分段/审批/终态收口）。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator

from langchain_core.runnables.schema import StreamEvent

from kokoro_agent.application.events.event_payloads import (
    subagent_finished_payload,
    subagent_started_payload,
    subagent_text_payload,
    text_payload,
    todo_payload,
    tool_invoked_payload,
    tool_returned_payload,
)
from kokoro_agent.application.events.run_emitter import RunEmitter
from kokoro_agent.application.events.subagent_router import SubagentRouter
from kokoro_agent.application.events.text_accumulator import TextAccumulator
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
from kokoro_agent.infrastructure.stream_events import translate_stream_event


def _terminal_event(emitter: RunEmitter, error: BaseException | None) -> AgentEvent:
    """终态收口：正常完成/超时归 run.completed，其余异常归 run.failed。"""
    if error is None:
        return emitter.emit("run.completed", {"status": "completed"})
    if isinstance(error, TimeoutError):
        # 模型/IO 级超时显式以 timeout 状态收口，不混同用户拒绝或一般失败。
        return emitter.emit("run.completed", {"status": "timeout"})
    return emitter.emit(
        "run.failed",
        {"error_kind": type(error).__name__, "message": str(error)},
    )


def _emit_subagent_text(
    emitter: RunEmitter, accumulator: TextAccumulator, subagent_id: str, final_text: str
) -> Iterator[AgentEvent]:
    """子智能体终答落定：流式累积过则只补 completed，否则合成 delta+completed。"""
    segment_id = emitter.segment()
    accumulated = accumulator.take()
    if accumulated is not None:
        yield emitter.emit(
            "subagent.text.completed",
            subagent_text_payload(segment_id, subagent_id, accumulated),
        )
        return
    payload = subagent_text_payload(segment_id, subagent_id, final_text)
    yield emitter.emit("subagent.text.delta", payload)
    yield emitter.emit("subagent.text.completed", payload)


def _emit_main_text(
    emitter: RunEmitter, accumulator: TextAccumulator, final_text: str
) -> Iterator[AgentEvent]:
    """主链路终答落定：与子智能体同形,额外在段尾 complete_segment 关闭当前段。"""
    segment_id = emitter.segment()
    accumulated = accumulator.take()
    if accumulated is not None:
        yield emitter.emit("text.completed", text_payload(segment_id, accumulated))
        emitter.complete_segment()
        return
    payload = text_payload(segment_id, final_text)
    yield emitter.emit("text.delta", payload)
    yield emitter.emit("text.completed", payload)
    emitter.complete_segment()


async def drive_agent_events(
    run_id: str,
    raw_events: AsyncIterator[StreamEvent],
    awaiting_tools: frozenset[str] = frozenset(),
) -> AsyncIterator[AgentEvent]:
    """以 run.started 开头、必有终态（run.completed/failed）收口，保证对外事件流自洽。

    不设 run 级墙钟超时：HITL 审批需无限等待用户操作；放弃由用户 cancel 收口。
    """
    emitter = RunEmitter(run_id)
    router = SubagentRouter()
    main_text = TextAccumulator()
    subagent_text = TextAccumulator()

    yield emitter.emit("run.started", {})
    try:
        async for raw_event in raw_events:
            for intent in translate_stream_event(raw_event):
                match intent:
                    case TextStream(text=text):
                        subagent_id = router.route(raw_event)
                        if subagent_id is not None:
                            yield emitter.emit(
                                "subagent.text.delta",
                                subagent_text_payload(
                                    emitter.segment(), subagent_id, subagent_text.append(text)
                                ),
                            )
                            continue
                        yield emitter.emit(
                            "text.delta", text_payload(emitter.segment(), main_text.append(text))
                        )

                    case TextFinal(text=text):
                        subagent_id = router.route(raw_event)
                        if subagent_id is not None:
                            for event in _emit_subagent_text(
                                emitter, subagent_text, subagent_id, text
                            ):
                                yield event
                            continue
                        for event in _emit_main_text(emitter, main_text, text):
                            yield event

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
                        router.started(subagent)
                        yield emitter.emit(
                            "subagent.started",
                            subagent_started_payload(emitter.segment(), subagent),
                        )

                    case SubagentFinished() as subagent:
                        router.finished()
                        yield emitter.emit(
                            "subagent.finished",
                            subagent_finished_payload(emitter.segment(), subagent),
                        )

                    case _:
                        continue
        yield _terminal_event(emitter, None)
    except Exception as error:  # noqa: BLE001 — 顶层兜底：超时/其余异常统一经 _terminal_event 收口
        yield _terminal_event(emitter, error)
