"""v3 四投影并发消费 + queue 合流单点发布：哨兵必达 drain 收束，防回压死锁。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable

from kokoro_agent.execution.protocols import (
    AgentRunStream,
    ModelStream,
    SubagentRunStream,
    ToolCallView,
)
from kokoro_agent.execution.events import (
    AgentEventPayload,
    RunEmitter,
    SourceResolver,
    delivery_created_payload,
    message_completed_payload,
    message_delta_payload,
    subagent_finished_payload,
    subagent_started_payload,
    subagent_text_completed_payload,
    subagent_tool_invoked_payload,
    subagent_tool_returned_payload,
    subagent_text_delta_payload,
    subagent_thinking_delta_payload,
    thinking_delta_payload,
    todo_payload,
    TOOL_RESULT_MAX_CHARS,
    output_delta_text,
    tool_invoked_payload,
    tool_output_delta_payload,
    tool_returned_payload,
)
from kokoro_agent.tools.registry import SUBAGENT_TOOL_NAME, TODO_TOOL_NAME

_EventQueue = asyncio.Queue[AgentEventPayload | None]


async def pump_run(emitter: RunEmitter, run: AgentRunStream, *, source_for: SourceResolver) -> None:
    """并发抽干 v3 四路 typed 投影 → 本地 queue 合流 → RunEmitter 单点发布。

    LangGraph v3 用 caller-driven single-flight pump 驱动全图，四路 typed 投影必须并发
    消费——任一通道缓冲满会回压整图直至死锁；queue 只为合流保序不为吞吐。
    正常/普通上游错误以 None 哨兵收束 drainer；外部取消或 drainer 致命错误则立即取消并
    await 两侧，既不继续发布缓冲帧，也不泄漏后台协程。
    """
    queue: _EventQueue = asyncio.Queue()
    drainer = asyncio.create_task(
        _drain(emitter, queue), name=f"kokoro-event-drainer:{emitter.run_id}"
    )
    producer = asyncio.create_task(
        _consume(run, queue, subagent_id=None, source_for=source_for),
        name=f"kokoro-projection-producer:{emitter.run_id}",
    )
    try:
        done, _pending = await asyncio.wait(
            {producer, drainer}, return_when=asyncio.FIRST_COMPLETED
        )
        if drainer in done:
            # The sentinel has not been sent yet, so an early drainer exit is an emitter
            # failure. Await the task directly to preserve the original exception type.
            await drainer
        try:
            await producer
        except Exception:
            # Preserve already-produced frames on an ordinary upstream failure.
            if not drainer.done():
                queue.put_nowait(None)
            await drainer
            raise
        if not drainer.done():
            queue.put_nowait(None)
        await drainer
    except asyncio.CancelledError:
        # External cancellation is an abort, not an orderly EOF: stop both sides
        # immediately and never continue draining buffered projections.
        await _cancel_and_settle(producer, drainer)
        raise
    except BaseException:
        await _cancel_and_settle(producer, drainer)
        raise


async def _cancel_and_settle(*tasks: asyncio.Task[None]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    # return_exceptions keeps concurrent cleanup faults from replacing the primary
    # cancellation or durable-output error already propagating from pump_run.
    await asyncio.gather(*tasks, return_exceptions=True)


async def _drain(emitter: RunEmitter, queue: _EventQueue) -> None:
    # 单一发布者：按入队序合流为一条有序 wire。RunEmitter 仅在已有 durable truth 时、
    # 且只在 bus.publish 调用边界隔离 live failure；其余错误必须原样冒泡并取消 producer。
    while True:
        payload = await queue.get()
        if payload is None:
            return
        await emitter.emit(payload)


async def _consume(
    run: AgentRunStream | SubagentRunStream,
    queue: _EventQueue,
    *,
    subagent_id: str | None,
    source_for: SourceResolver,
) -> None:
    await asyncio.gather(
        _consume_messages(run.messages, queue, subagent_id),
        _consume_tools(run.tool_calls, queue, subagent_id),
        _consume_subagents(run.subagents, queue, source_for),
        # custom 遥测无 wire kind：仍须抽干防回压，内容弃置。
        _drain_aiter(run.custom),
    )


async def _consume_messages(
    messages: AsyncIterable[ModelStream], queue: _EventQueue, subagent_id: str | None
) -> None:
    async for model in messages:
        if model.node != "model":
            # 非模型节点的消息投影（如 before_model 注入的 steer HumanMessage、
            # summarization 改写）：绝不冒充正文上 wire；仍抽干防回压。
            await asyncio.gather(_drain_aiter(model.text), _drain_aiter(model.reasoning))
            continue
        segment_id = model.message_id or ""
        # 原生 .text/.reasoning projection 并发消费（共享 pump、replay-buffer 安全）。
        text_full, _ = await asyncio.gather(
            _pump_text(model.text, queue, segment_id, subagent_id),
            _pump_reasoning(model.reasoning, queue, segment_id, subagent_id),
        )
        final = model.output_message
        seg = final.id if (final is not None and final.id) else segment_id
        # 终态帧全文覆盖累积。text 用原生 message.text（排除 tool 块）；
        # thinking 无 completed kind，故仅 text 收终态帧。
        text_final = final.text if final is not None else text_full
        completed = (
            subagent_text_completed_payload(text_final, segment_id=seg, subagent_id=subagent_id)
            if subagent_id is not None
            else message_completed_payload(text_final, segment_id=seg)
        )
        if completed is not None:
            await queue.put(completed)


async def _pump_text(
    deltas: AsyncIterable[str], queue: _EventQueue, segment_id: str, subagent_id: str | None
) -> str:
    acc = ""
    async for text in deltas:
        acc += text
        payload = (
            subagent_text_delta_payload(text, segment_id=segment_id, subagent_id=subagent_id)
            if subagent_id is not None
            else message_delta_payload(text, segment_id=segment_id)
        )
        if payload is not None:
            await queue.put(payload)
    return acc


async def _pump_reasoning(
    deltas: AsyncIterable[str], queue: _EventQueue, segment_id: str, subagent_id: str | None
) -> None:
    async for text in deltas:
        payload: AgentEventPayload | None
        if subagent_id is not None:
            payload = subagent_thinking_delta_payload(
                text, segment_id=segment_id, subagent_id=subagent_id
            )
        else:
            payload = thinking_delta_payload(text, segment_id=segment_id)
        if payload is not None:
            await queue.put(payload)


async def _pump_tool_output(tc: ToolCallView, queue: _EventQueue) -> None:
    # 长执行工具（如 execute）的增量输出上 wire；累计超 result 护栏后静默停发（终值走 returned）。
    sent = 0
    async for chunk in tc.output_deltas:
        if sent >= TOOL_RESULT_MAX_CHARS:
            continue  # 继续抽干防回压，只是不再上 wire
        text = output_delta_text(chunk)
        if not text:
            continue
        text = text[: TOOL_RESULT_MAX_CHARS - sent]
        sent += len(text)
        payload = tool_output_delta_payload(tc, text)
        if payload is not None:
            await queue.put(payload)


async def _consume_tools(
    tool_calls: AsyncIterable[ToolCallView], queue: _EventQueue, subagent_id: str | None
) -> None:
    async for tc in tool_calls:
        if tc.tool_name == SUBAGENT_TOOL_NAME:
            # 子代理启动工具由 subagents 投影处理，避免与 tool.* / subagent.tool.* 双发。
            await _drain_aiter(tc.output_deltas)
            continue
        if subagent_id is not None:
            # 子代理内工具（含其自有 todo，不得覆盖主面板）走 subagent.tool.* 可见性通道；
            # 无输出增量通道，抽干防回压，终值走 returned。
            await queue.put(subagent_tool_invoked_payload(tc, subagent_id=subagent_id))
            await _drain_aiter(tc.output_deltas)
            await queue.put(subagent_tool_returned_payload(tc, subagent_id=subagent_id))
            continue
        if tc.tool_name == TODO_TOOL_NAME:
            await queue.put(todo_payload(tc))
            await _drain_aiter(tc.output_deltas)
            continue
        await queue.put(tool_invoked_payload(tc))
        await _pump_tool_output(tc, queue)
        returned = tool_returned_payload(tc)
        if returned is None:
            # 工具执行中途 interrupt（如 request_input）：不发伪 returned，等 resume replay 出真值。
            continue
        await queue.put(returned)
        # deliver 成功归档时紧随 tool.returned 追发 delivery.created（同 emitter 统一序号）。
        delivery = delivery_created_payload(tc)
        if delivery is not None:
            await queue.put(delivery)


async def _consume_subagents(
    subagents: AsyncIterable[SubagentRunStream], queue: _EventQueue, source_for: SourceResolver
) -> None:
    async for sub in subagents:
        source = source_for(sub.name or "subagent")
        await queue.put(subagent_started_payload(sub, source=source))
        await _consume(sub, queue, subagent_id=sub.trigger_call_id, source_for=source_for)
        await queue.put(subagent_finished_payload(sub, source=source))


async def _drain_aiter(source: AsyncIterable[object]) -> None:
    async for _ in source:
        pass
