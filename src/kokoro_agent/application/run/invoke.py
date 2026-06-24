"""一次 graph invoke 的生命周期编排：started → 投影消费 → interrupt 暂停 / 终态收口。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from langchain.agents.middleware.human_in_the_loop import ActionRequest
from langchain_core.callbacks import get_usage_metadata_callback
from langchain_core.messages import BaseMessage, UsageMetadata
from langchain_core.runnables.config import RunnableConfig
from langgraph.stream import CustomTransformer
from langgraph.types import Interrupt
from pydantic import JsonValue

from kokoro_agent.application.projection.awaiting import awaiting_approval_events
from kokoro_agent.application.projection.transformer import (
    run_done_event,
    run_error_event,
    run_started_event,
)
from kokoro_agent.application.protocols.agent import InvokableAgent
from kokoro_agent.application.protocols.stream import StreamProtocol
from kokoro_agent.application.run.consumer import EventQueue, consume_run, drain
from kokoro_agent.interfaces.envelope import AgentEvent

__all__ = ["InvokableAgent", "events_stream", "invoke_once"]


def events_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:events"


async def _always_claim() -> bool:
    # 默认认领：直接调 invoke_once（如测试）无共享存储时终态总归本次发。
    return True


async def invoke_once(
    bus: StreamProtocol,
    agent: InvokableAgent,
    run_id: str,
    conversation_id: str,
    payload: object,
    interrupt_on_names: frozenset[str] = frozenset(),
    trace: RunnableConfig | None = None,
    claim_terminal: Callable[[], Awaitable[bool]] = _always_claim,
) -> bool:
    """True=已发终态(completed/failed)；False=interrupt 暂停未发终态。

    终态发射前先经 claim_terminal 原子认领：cancel 与自然完成共用同一认领 key，
    多 pod 广播下恰好一个终态落地（认领失败者静默跳过，不重复发终态）。
    """
    stream = events_stream(run_id)
    config = _config(conversation_id, trace)
    await _publish(bus, stream, run_started_event(run_id))
    # 原生 usage callback 经 callback 树跨主/子代理自动聚合 token，与事件投影解耦；
    # 每次 invoke_once 独立计量本段（HITL resume 是新一段）。
    with get_usage_metadata_callback() as usage_cb:
        try:
            run = await agent.astream_events(
                payload, version="v3", config=config, transformers=[CustomTransformer]
            )
            queue: EventQueue = asyncio.Queue()
            async with run:
                drainer = asyncio.create_task(drain(bus, stream, queue))
                await consume_run(run, run_id, queue, subagent_id=None)
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
            if await claim_terminal():
                usage = _sum_usage(usage_cb.usage_metadata)
                await _publish(bus, stream, run_done_event(usage, request_id=run_id))
            return True
        except Exception as error:  # noqa: BLE001 — 顶层兜底：任何异常统一收口为 agent_error
            if await claim_terminal():
                await _publish(bus, stream, run_error_event(error, request_id=run_id))
            return True


def _sum_usage(per_model: Mapping[str, UsageMetadata]) -> dict[str, JsonValue]:
    # 原生 callback 按 model_name 分组；wire 用扁平 total，故跨 model 累加三键。
    total: dict[str, JsonValue] = {}
    for usage in per_model.values():
        for key in ("input_tokens", "output_tokens", "total_tokens"):
            value = usage.get(key)
            if isinstance(value, int):
                prev = total.get(key, 0)
                total[key] = (prev if isinstance(prev, int) else 0) + value
    return total


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
