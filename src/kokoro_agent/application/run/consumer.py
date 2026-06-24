"""投影消费：langgraph 四路 typed projection 并发转 AgentEvent，推 queue 由 drain 保序发布。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Callable

from kokoro_agent.application.projection.transformer import (
    SUBAGENT_LAUNCH_NAMES,
    custom_event,
    reasoning_chunk_event,
    subagent_finished_event,
    subagent_started_event,
    text_chunk_event,
    todo_event,
    tool_end_event,
    tool_start_event,
)
from kokoro_agent.application.protocols.agent import (
    AgentRunStream,
    ModelStream,
    SubagentRunStream,
    ToolCallView,
)
from kokoro_agent.application.protocols.stream import StreamProtocol
from kokoro_agent.infrastructure.constants import TODO_TOOL_NAME
from kokoro_agent.interfaces.envelope import AgentEvent

__all__ = ["EventQueue", "consume_run", "drain"]

EventQueue = asyncio.Queue["AgentEvent | None"]


async def consume_run(
    run: AgentRunStream | SubagentRunStream,
    request_id: str,
    queue: EventQueue,
    *,
    subagent_id: str | None,
) -> None:
    # 四投影并发消费，共享 single-flight pump 推进全图；各投影把 AgentEvent 推 queue 由 drain 保序发。
    await asyncio.gather(
        _consume_messages(run.messages, request_id, queue, subagent_id),
        _consume_tools(run.tool_calls, request_id, queue),
        _consume_subagents(run.subagents, request_id, queue),
        _consume_custom(run.custom, request_id, queue),
    )


async def drain(bus: StreamProtocol, stream: str, queue: EventQueue) -> None:
    # 单一消费者把并发投影推入的事件按入队序发布；None 哨兵收束。
    while True:
        ev = await queue.get()
        if ev is None:
            return
        await bus.publish(stream, ev.model_dump())


async def _consume_messages(
    messages: AsyncIterable[ModelStream],
    request_id: str,
    queue: EventQueue,
    subagent_id: str | None,
) -> None:
    async for model in messages:
        # 原生 .text/.reasoning projection 并发消费（共享 pump、replay-buffer 安全），各自累积全文。
        segment_id = model.message_id or ""
        text_full, reasoning_full = await asyncio.gather(
            _pump(model.text, text_chunk_event, segment_id, request_id, queue, subagent_id),
            _pump(model.reasoning, reasoning_chunk_event, segment_id, request_id, queue, subagent_id),
        )
        final = model.output_message
        seg = final.id if (final is not None and final.id) else segment_id
        # 两通道对称发终态帧（web 以 final 覆盖累积）。text 用原生 message.text（排除 tool 块），
        # reasoning 无 message 访问器故用累积全文；空文本由 _chunk_event 吞掉。
        text_final = final.text if final is not None else text_full
        for builder, full in ((text_chunk_event, text_final), (reasoning_chunk_event, reasoning_full)):
            ev = builder(full, segment_id=seg, request_id=request_id, subagent_id=subagent_id, final=True)
            if ev is not None:
                await queue.put(ev)


async def _pump(
    deltas: AsyncIterable[str],
    builder: Callable[..., AgentEvent | None],
    segment_id: str,
    request_id: str,
    queue: EventQueue,
    subagent_id: str | None,
) -> str:
    # 通道增量逐 delta 发（final=False）并累积全文返回，供外层补对称的终态帧。
    acc = ""
    async for text in deltas:
        acc += text
        ev = builder(text, segment_id=segment_id, request_id=request_id, subagent_id=subagent_id, final=False)
        if ev is not None:
            await queue.put(ev)
    return acc


async def _consume_tools(
    tool_calls: AsyncIterable[ToolCallView], request_id: str, queue: EventQueue
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
    queue: EventQueue,
) -> None:
    async for sub in subagents:
        await queue.put(subagent_started_event(sub, request_id=request_id))
        await consume_run(sub, request_id, queue, subagent_id=sub.trigger_call_id)
        await queue.put(subagent_finished_event(sub, request_id=request_id))


async def _consume_custom(
    custom: AsyncIterable[object], request_id: str, queue: EventQueue
) -> None:
    async for payload in custom:
        await queue.put(custom_event(payload, request_id=request_id))


async def _drain_aiter(source: AsyncIterable[object]) -> None:
    async for _ in source:
        pass
