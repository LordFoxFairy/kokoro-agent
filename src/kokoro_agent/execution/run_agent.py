"""单次 run 编排：run.started → 投影泵 → interrupt 暂停 / typed atomic 终态收口。"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping

from langchain_core.callbacks import get_usage_metadata_callback
from langchain_core.messages import UsageMetadata
from langchain_core.runnables.config import RunnableConfig
from langgraph.stream import CustomTransformer

from kokoro_agent.protocol import (
    RunCompletedPayload,
    RunFailedPayload,
    RunStartedPayload,
)
from kokoro_agent.execution.approvals import awaiting_payloads
from kokoro_agent.execution.events import RunEmitter, SourceResolver
from kokoro_agent.execution.failures import run_failed_payload
from kokoro_agent.execution.protocols import AgentRunnable
from kokoro_agent.execution.publish_agent_events import pump_run

LOGGER = logging.getLogger(__name__)


async def invoke_once(
    emitter: RunEmitter,
    agent: AgentRunnable,
    thread_id: str,
    payload: object,
    *,
    approval_tool_names: frozenset[str],
    source_for: SourceResolver,
    describe_tool: Callable[[str], str | None] = lambda _name: None,
    finalize_terminal: Callable[
        [RunCompletedPayload | RunFailedPayload, tuple[int, int]], Awaitable[bool]
    ],
    record_usage: Callable[[int, int], Awaitable[tuple[int, int]]],
    trace: RunnableConfig | None = None,
    recursion_limit: int = 100,
) -> bool:
    """True=已发终态(completed/failed)；False=interrupt 暂停未发终态。

    终态发射前经 finalize_terminal 同事务提交：cancel/自然完成/异常共用唯一收口，
    多 pod 并发下恰好一个终态落地（认领失败者静默跳过）。
    """
    config = _config(thread_id, trace, recursion_limit)
    if emitter.at_start:
        # run.started 收编进 critical outbox（emitter 内分配 durable_seq=1、落 queued、发布后 published）。
        await emitter.emit(RunStartedPayload())
    # 原生 usage callback 经 callback 树跨主/子代理自动聚合 token；每段独立计量。
    with get_usage_metadata_callback() as usage_cb:
        try:
            run = await agent.astream_events(
                payload,
                version="v3",
                config=config,
                transformers=[CustomTransformer],
            )
            async with run:
                await pump_run(emitter, run, source_for=source_for)
                if await run.interrupted():
                    snapshot = await agent.aget_state(config)
                    for awaiting in awaiting_payloads(
                        snapshot, approval_tool_names, describe_tool=describe_tool
                    ):
                        await emitter.emit(awaiting)
                    # 暂停段的用量当场入账：终态段只报累计值，多段 run 不再少报。
                    await _record(record_usage, usage_cb.usage_metadata)
                    return False
            outcome: RunCompletedPayload | RunFailedPayload = RunCompletedPayload(
                status="completed"
            )
        except Exception as error:  # noqa: BLE001 — execution failures become safe facts
            outcome = run_failed_payload(error)
        # Persistence failures propagate; never turn a failed commit of one
        # outcome into a second, different terminal decision.
        totals = _usage_totals(usage_cb.usage_metadata)
        await finalize_terminal(outcome, totals)
        return True


async def _record(
    record_usage: Callable[[int, int], Awaitable[tuple[int, int]]],
    per_model: Mapping[str, UsageMetadata],
) -> tuple[int, int]:
    # callback 按 model_name 分组；跨 model 累加本段用量后入账，返回 run 级累计。
    return await record_usage(*_usage_totals(per_model))


def _usage_totals(per_model: Mapping[str, UsageMetadata]) -> tuple[int, int]:
    return (
        sum(usage.get("input_tokens", 0) for usage in per_model.values()),
        sum(usage.get("output_tokens", 0) for usage in per_model.values()),
    )


def _config(
    thread_id: str, trace: RunnableConfig | None, recursion_limit: int
) -> RunnableConfig:
    # 失控熔断：无限工具循环在限额处炸成 GraphRecursionError → run.failed fail-loud。
    config: RunnableConfig = {
        "configurable": {"thread_id": thread_id},
        "recursion_limit": recursion_limit,
    }
    if trace is not None:
        callbacks = trace.get("callbacks")
        metadata = trace.get("metadata")
        if callbacks is not None:
            config["callbacks"] = callbacks
        if metadata is not None:
            config["metadata"] = metadata
    return config
