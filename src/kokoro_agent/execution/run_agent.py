"""单次 run 编排：run.started → 投影泵 → interrupt 暂停 / typed atomic 终态收口。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Literal, TypeGuard

from langchain_core.callbacks import get_usage_metadata_callback
from langchain_core.messages import AIMessage, UsageMetadata
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
from kokoro_agent.domain.chat.projection import ToolDisplayCode, ToolDisplaySource
from kokoro_agent.model.call_scope import (
    DEFAULT_MODEL_CALL_SETTLE_TIMEOUT_S,
    ModelCallScope,
    bind_model_call_scope,
)

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
    tool_display_codes: tuple[tuple[str, ToolDisplayCode], ...] = (),
    tool_display_sources: tuple[ToolDisplaySource, ...] = (),
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
    model_call_settle_timeout_s: float = DEFAULT_MODEL_CALL_SETTLE_TIMEOUT_S,
) -> bool:
    """True=已发终态(completed/failed)；False=interrupt 暂停未发终态。

    终态发射前经 finalize_terminal 同事务提交：cancel/自然完成/异常共用唯一收口，
    多 pod 并发下恰好一个终态落地（认领失败者静默跳过）。
    """
    emitter.bind_tool_display_codes(tool_display_codes)
    emitter.bind_tool_display_sources(tool_display_sources)
    config = _config(thread_id, trace, recursion_limit)
    if initial:
        # run.started eligibility is explicit; the Run-locked outbox deduplicates retries.
        await emitter.emit(RunStartedPayload())
    # 原生 usage callback 经 callback 树跨主/子代理自动聚合 token；每段独立计量。
    with get_usage_metadata_callback() as usage_cb:
        scope = ModelCallScope(timeout_s=model_call_settle_timeout_s)
        native_error: Exception | None = None
        interrupted = False
        final_segment_id: str | None = None
        with bind_model_call_scope(scope):

            async def drive_native() -> tuple[bool, str | None]:
                run = await agent.astream_events(
                    payload,
                    version="v3",
                    config=config,
                    transformers=[CustomTransformer],
                )
                async with run:
                    await pump_run(emitter, run, source_for=source_for)
                    interrupted = await run.interrupted()
                    final_id = (
                        None if interrupted else _final_segment_id(await run.output())
                    )
                    return interrupted, final_id

            try:
                try:
                    driver = asyncio.create_task(drive_native())
                    scope.register_owned_task(driver)
                    interrupted, final_segment_id = await asyncio.shield(driver)
                    outcome: RunCompletedPayload | RunFailedPayload = (
                        RunCompletedPayload(status="completed")
                    )
                except ProgressPersistenceError:
                    raise
                except Exception as error:  # noqa: BLE001 — safe execution fact
                    native_error = error
                    outcome = run_failed_payload(error)
            except BaseException as primary:
                await scope.settle(cancel_active=True, primary=primary)
                raise
            else:
                await scope.settle(
                    cancel_active=native_error is not None, primary=native_error
                )
        if native_error is None:
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
            if final_segment_id is not None:
                outcome = RunCompletedPayload(
                    status="completed",
                    final_segment_id=await emitter.proven_final_segment(
                        final_segment_id
                    ),
                )
        # Persistence failures propagate; never turn a failed commit of one
        # outcome into a second, different terminal decision.
        totals = _usage_totals(usage_cb.usage_metadata)
        await finalize_terminal(outcome, totals)
        return True


def _final_segment_id(output: dict[str, Any] | None) -> str | None:
    """Native terminal state only; never promote preceding progress text."""
    if not isinstance(output, dict):
        return None
    messages = output.get("messages")
    if not _message_sequence(messages) or not messages:
        return None
    final = messages[-1]
    if not isinstance(final, AIMessage) or final.tool_calls or final.invalid_tool_calls:
        return None
    return final.id if final.id and final.id.strip() else None


def _message_sequence(value: object) -> TypeGuard[Sequence[object]]:
    return isinstance(value, list | tuple)


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
