"""单次 run 编排：run.started → 投影泵 → interrupt 暂停 / typed atomic 终态收口。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Literal

from langchain_core.callbacks import get_usage_metadata_callback
from langchain_core.messages import UsageMetadata
from langchain_core.runnables.config import RunnableConfig
from langgraph.stream import CustomTransformer

from kokoro_agent.protocol import (
    RunCompletedPayload,
    RunFailedPayload,
    RunStartedPayload,
)
from kokoro_agent.execution.events import (
    RunEmitter,
    SourceResolver,
    ProgressPersistenceError,
)
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
    initial: bool,
    approval_tool_names: frozenset[str],
    source_for: SourceResolver,
    describe_tool: Callable[[str], str | None] = lambda _name: None,
    finalize_terminal: Callable[
        [RunCompletedPayload | RunFailedPayload, tuple[int, int]], Awaitable[bool]
    ],
    record_usage: Callable[[int, int], Awaitable[tuple[int, int]]],
    on_native_settled: Callable[
        [Callable[[], Awaitable[tuple[int, int]]]],
        Awaitable[Literal["active", "waiting", "unknown"]],
    ],
    trace: RunnableConfig | None = None,
    recursion_limit: int = 100,
) -> bool:
    """True=已发终态(completed/failed)；False=interrupt 暂停未发终态。

    终态发射前经 finalize_terminal 同事务提交：cancel/自然完成/异常共用唯一收口，
    多 pod 并发下恰好一个终态落地（认领失败者静默跳过）。
    """
    config = _config(thread_id, trace, recursion_limit)
    if initial:
        # run.started eligibility is explicit; the Run-locked outbox deduplicates retries.
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
                interrupted = await run.interrupted()
            outcome: RunCompletedPayload | RunFailedPayload = RunCompletedPayload(
                status="completed"
            )
        except ProgressPersistenceError:
            raise
        except Exception as error:  # noqa: BLE001 — execution failures become safe facts
            outcome = run_failed_payload(error)
        else:
            # The SDK context has drained. Persistence failures deliberately sit
            # outside the native exception handler: no invented failed outcome.
            segment = _usage_totals(usage_cb.usage_metadata)
            sealed: tuple[int, int] | None = None
            seal_lock = asyncio.Lock()

            async def seal_usage() -> tuple[int, int]:
                nonlocal sealed
                async with seal_lock:
                    if sealed is None:
                        sealed = await record_usage(*segment)
                    return sealed

            # Waiting/unknown settlement seals before it can release the lease.
            # Active leaves usage to the unique atomic terminal transaction.
            phase = await on_native_settled(seal_usage)
            if interrupted:
                # A conservative reader may still report active after an SDK
                # interrupt. Retain this segment using only the original fence.
                await seal_usage()
                return False
            if phase != "active":
                return False
        # Persistence failures propagate; never turn a failed commit of one
        # outcome into a second, different terminal decision.
        totals = _usage_totals(usage_cb.usage_metadata)
        await finalize_terminal(outcome, totals)
        return True


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
