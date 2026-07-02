"""投影消费：langgraph 四路 typed projection 并发转 AgentEvent，推 queue 由 drain 保序发布。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterable, Callable

from kokoro_agent.subagents.types import SubagentSource
from kokoro_agent.execution.events import (
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
from kokoro_agent.execution.protocols import (
    AgentRunStream,
    ModelStream,
    SubagentRunStream,
    ToolCallView,
)
from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.tools.names import TODO_TOOL_NAME
from kokoro_agent.run.events import AgentEvent

__all__ = ["EventQueue", "consume_and_drain_run", "consume_run", "drain"]

LOGGER = logging.getLogger(__name__)

EventQueue = asyncio.Queue["AgentEvent | None"]
SubagentSourceResolver = Callable[[str], SubagentSource]


def _custom_source(_name: str) -> SubagentSource:
    return "config-custom"


async def consume_and_drain_run(
    bus: StreamProtocol,
    stream: str,
    run: AgentRunStream,
    request_id: str,
    subagent_source: SubagentSourceResolver = _custom_source,
) -> None:
    """微观本地消费层的一体化合流管道：并发抽干 v3 四路 typed 投影 → 本地有序合流 → 单点 publish。

    [双层队列·内部层] 与宏观分布式层（Redis Stream 总线 + claim_terminal 原子锁负责跨会话隔离/
    防多 Pod 重复终态）相对：此层在单进程内收口一次 run 的流式输出。LangGraph v3 用 caller-driven
    单点推进锁（single-flight pump）驱动全图，4 路 typed 投影必须并发消费——任一通道缓冲到 maxlen
    会回压（backpressure）整图直至死锁；本地 asyncio.Queue 再把并发投影按入队序合流为一条 publish 流。

    [异常防御] try/finally 保证 None 结束哨兵百分之百送达、drainer 必被 await 收束：无论上游投影或
    大模型流如何崩溃/中止，后台 drain 协程绝不永久阻塞在 queue.get() 而泄漏；异常照常向上传播由
    invoke_once 顶层收口为 agent_error。
    """
    queue: EventQueue = asyncio.Queue()
    drainer = asyncio.create_task(drain(bus, stream, queue))
    try:
        await consume_run(
            run, request_id, queue, subagent_id=None, subagent_source=subagent_source
        )
    finally:
        # 哨兵必达：consume_run 即使抛出，drain 也收 None 而非永久阻塞 → 杜绝后台协程泄漏。
        await queue.put(None)
        await drainer


async def consume_run(
    run: AgentRunStream | SubagentRunStream,
    request_id: str,
    queue: EventQueue,
    *,
    subagent_id: str | None,
    subagent_source: SubagentSourceResolver = _custom_source,
) -> None:
    # WHY 并发 + queue：messages/tool_calls/subagents/custom 是 langgraph v3 原生 typed 投影
    # (AsyncGraphRunStream)——非本仓发明。框架按 caller-driven 单 single-flight pump(asyncio.Lock)
    # 推进全图：并发迭代各通道即"驱动一次、分发各通道"，这才换来消费端零事件类型 isinstance 分支
    # (取代旧 v1/v2 扁平流的 type-dispatch)。4 个并发生产者再经 queue→单 drain re-serialize 成一条
    # 有序、单点 publish 的 wire(并发本就低、单 pump 已 pace；queue 只为合流保序不为吞吐)；None 收束。
    await asyncio.gather(
        _consume_messages(run.messages, request_id, queue, subagent_id),
        _consume_tools(run.tool_calls, request_id, queue, subagent_id),
        _consume_subagents(run.subagents, request_id, queue, subagent_source),
        _consume_custom(run.custom, request_id, queue),
    )


async def drain(bus: StreamProtocol, stream: str, queue: EventQueue) -> None:
    # 微观本地消费层的单一发布者：按入队序把并发投影合流为一条有序 wire；None 哨兵收束。
    # [局部容错] 单条事件写总线失败仅记日志并继续——一条坏事件不得中断整条 wire，更不得让 drainer
    # 异常退出、使 consume_and_drain_run 的 await drainer 在哨兵前抛出而漏发后续事件。
    while True:
        ev = await queue.get()
        if ev is None:
            return
        try:
            await bus.publish(stream, ev.model_dump())
        except Exception:  # noqa: BLE001 — 局部容错：单事件发布失败隔离，不毁整条流
            LOGGER.warning("dropping event on publish failure: event=%s", ev.event)


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
    tool_calls: AsyncIterable[ToolCallView],
    request_id: str,
    queue: EventQueue,
    subagent_id: str | None,
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
        await queue.put(tool_start_event(tc, request_id=request_id, subagent_id=subagent_id))
        await _drain_aiter(tc.output_deltas)
        await queue.put(tool_end_event(tc, request_id=request_id, subagent_id=subagent_id))


async def _consume_subagents(
    subagents: AsyncIterable[SubagentRunStream],
    request_id: str,
    queue: EventQueue,
    subagent_source: SubagentSourceResolver,
) -> None:
    async for sub in subagents:
        source = _source_for(sub.name or "subagent", subagent_source)
        await queue.put(subagent_started_event(sub, request_id=request_id, source=source))
        await consume_run(
            sub,
            request_id,
            queue,
            subagent_id=sub.trigger_call_id,
            subagent_source=subagent_source,
        )
        await queue.put(subagent_finished_event(sub, request_id=request_id, source=source))


def _source_for(name: str, resolve: SubagentSourceResolver) -> SubagentSource:
    try:
        return resolve(name)
    except ValueError:
        return "config-custom"


async def _consume_custom(
    custom: AsyncIterable[object], request_id: str, queue: EventQueue
) -> None:
    async for payload in custom:
        await queue.put(custom_event(payload, request_id=request_id))


async def _drain_aiter(source: AsyncIterable[object]) -> None:
    async for _ in source:
        pass
