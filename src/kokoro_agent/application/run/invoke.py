"""一次 graph invoke：v3 typed projections 递归消费→保序 publish，遇 interrupt 暂停否则终态收口。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Mapping
from typing import Any

from langchain.agents.middleware.human_in_the_loop import ActionRequest
from langchain_core.messages import BaseMessage
from langchain_core.runnables.config import RunnableConfig
from langgraph.stream import CustomTransformer
from langgraph.types import Interrupt
from pydantic import JsonValue

from kokoro_agent.application.projection.awaiting import awaiting_approval_events
from kokoro_agent.application.projection.transformer import (
    SUBAGENT_LAUNCH_NAMES,
    custom_event,
    final_text_event,
    run_done_event,
    run_error_event,
    run_started_event,
    stream_text_event,
    subagent_finished_event,
    subagent_started_event,
    todo_event,
    tool_end_event,
    tool_start_event,
    usage_delta,
)
from kokoro_agent.application.protocols.agent import (
    AgentRunStream,
    InvokableAgent,
    ModelStream,
    SubagentRunStream,
    ToolCallView,
)
from kokoro_agent.application.protocols.stream import StreamProtocol
from kokoro_agent.infrastructure.constants import TODO_TOOL_NAME
from kokoro_agent.interfaces.envelope import AgentEvent

__all__ = ["InvokableAgent", "events_stream", "invoke_once"]

_EventQueue = asyncio.Queue["AgentEvent | None"]


def events_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:events"


async def invoke_once(
    bus: StreamProtocol,
    agent: InvokableAgent,
    run_id: str,
    conversation_id: str,
    payload: object,
    interrupt_on_names: frozenset[str] = frozenset(),
    trace: RunnableConfig | None = None,
) -> bool:
    """True=已发终态(completed/failed)；False=interrupt 暂停未发终态。"""
    stream = events_stream(run_id)
    config = _config(conversation_id, trace)
    # usage 按本次 invoke 段计量：HITL resume 是独立段，跨暂停累计待持久化后续接。
    usage_total: dict[str, JsonValue] = {}
    await _publish(bus, stream, run_started_event(run_id))
    try:
        run = await agent.astream_events(
            payload, version="v3", config=config, transformers=[CustomTransformer]
        )
        queue: _EventQueue = asyncio.Queue()
        async with run:
            drainer = asyncio.create_task(_drain(bus, stream, queue))
            await _consume_run(run, run_id, queue, usage_total, subagent_id=None)
            await queue.put(None)
            await drainer
            if await run.interrupted():
                snapshot = await agent.aget_state(config)
                for ev in awaiting_approval_events(
                    _messages(snapshot.values),
                    _action_requests(snapshot.interrupts),
                    interrupt_on_names,
                    request_id=run_id,
                ):
                    await _publish(bus, stream, ev)
                return False
        await _publish(bus, stream, run_done_event(usage_total, request_id=run_id))
        return True
    except Exception as error:  # noqa: BLE001 — 顶层兜底：任何异常统一收口为 agent_error
        await _publish(bus, stream, run_error_event(error, request_id=run_id))
        return True


async def _consume_run(
    run: AgentRunStream | SubagentRunStream,
    request_id: str,
    queue: _EventQueue,
    usage_total: dict[str, JsonValue],
    *,
    subagent_id: str | None,
) -> None:
    # 四投影并发消费，共享 single-flight pump 推进全图；各投影把 AgentEvent 推 queue 由 drainer 保序发。
    await asyncio.gather(
        _consume_messages(run.messages, request_id, queue, usage_total, subagent_id),
        _consume_tools(run.tool_calls, request_id, queue),
        _consume_subagents(run.subagents, request_id, queue, usage_total),
        _consume_custom(run.custom, request_id, queue),
    )


async def _consume_messages(
    messages: AsyncIterable[ModelStream],
    request_id: str,
    queue: _EventQueue,
    usage_total: dict[str, JsonValue],
    subagent_id: str | None,
) -> None:
    async for model in messages:
        segment_id = ""
        async for block in model:
            ident = block.get("id")
            if block.get("event") == "message-start" and isinstance(ident, str):
                segment_id = ident
            ev = stream_text_event(
                block, segment_id=segment_id, request_id=request_id, subagent_id=subagent_id
            )
            if ev is not None:
                await queue.put(ev)
        final = model.output_message
        if final is not None and final.id is not None:
            segment_id = final.id
        final_ev = final_text_event(
            final, segment_id=segment_id, request_id=request_id, subagent_id=subagent_id
        )
        if final_ev is not None:
            await queue.put(final_ev)
        for key, delta in usage_delta(final).items():
            prev = usage_total.get(key, 0)
            usage_total[key] = (prev if isinstance(prev, int) else 0) + delta


async def _consume_tools(
    tool_calls: AsyncIterable[ToolCallView], request_id: str, queue: _EventQueue
) -> None:
    async for tc in tool_calls:
        if tc.tool_name in SUBAGENT_LAUNCH_NAMES:
            # 子代理启动工具由 run.subagents 投影处理，避免与 tool_call_* 双发。
            await _drain_aiter(tc.output_deltas)
            continue
        if tc.tool_name == TODO_TOOL_NAME:
            await queue.put(todo_event(tc, request_id=request_id))
            await _drain_aiter(tc.output_deltas)
            continue
        await queue.put(tool_start_event(tc, request_id=request_id))
        await _drain_aiter(tc.output_deltas)
        await queue.put(tool_end_event(tc, request_id=request_id))


async def _consume_subagents(
    subagents: AsyncIterable[SubagentRunStream],
    request_id: str,
    queue: _EventQueue,
    usage_total: dict[str, JsonValue],
) -> None:
    async for sub in subagents:
        await queue.put(subagent_started_event(sub, request_id=request_id))
        await _consume_run(sub, request_id, queue, usage_total, subagent_id=sub.trigger_call_id)
        await queue.put(subagent_finished_event(sub, request_id=request_id))


async def _consume_custom(
    custom: AsyncIterable[object], request_id: str, queue: _EventQueue
) -> None:
    async for payload in custom:
        ev = custom_event(payload, request_id=request_id)
        if ev is not None:
            await queue.put(ev)


async def _drain(bus: StreamProtocol, stream: str, queue: _EventQueue) -> None:
    while True:
        ev = await queue.get()
        if ev is None:
            return
        await bus.publish(stream, ev.model_dump())


async def _drain_aiter(source: AsyncIterable[object]) -> None:
    async for _ in source:
        pass


def _config(conversation_id: str, trace: RunnableConfig | None) -> RunnableConfig:
    config: RunnableConfig = {"configurable": {"thread_id": conversation_id}}
    if trace is not None:
        callbacks = trace.get("callbacks")
        metadata = trace.get("metadata")
        if callbacks is not None:
            config["callbacks"] = callbacks
        if metadata is not None:
            config["metadata"] = metadata
    return config


async def _publish(bus: StreamProtocol, stream: str, ev: AgentEvent) -> None:
    await bus.publish(stream, ev.model_dump())


def _action_requests(interrupts: tuple[Interrupt, ...]) -> list[ActionRequest]:
    # HumanInTheLoopMiddleware 的 interrupt.value 即 typed HITLRequest，直接取 action_requests。
    requests: list[ActionRequest] = []
    for interrupt in interrupts:
        requests.extend(interrupt.value["action_requests"])
    return requests


def _messages(values: Mapping[str, Any]) -> list[BaseMessage]:
    # langgraph 图状态 values 为 Any；messages 在此唯一边界过滤为 BaseMessage 序列。
    raw: Any = values.get("messages") or []
    return [m for m in raw if isinstance(m, BaseMessage)]
