"""单次 run 编排：run.started → 投影泵 → interrupt 暂停 / claim-before-emit 终态收口。"""

from __future__ import annotations


from collections.abc import Awaitable, Callable, Mapping

from langchain_core.callbacks import get_usage_metadata_callback
from langchain_core.messages import UsageMetadata
from langchain_core.runnables.config import RunnableConfig
from langgraph.stream import CustomTransformer

from kokoro_agent.contract import (
    RunCompletedPayload,
    RunStartedPayload,
    TokenUsage,
)
from kokoro_agent.execution.approvals import awaiting_payloads, plan_proposed_payload
from kokoro_agent.execution.events import RunEmitter, SourceResolver, run_failed_payload
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.execution.publish_agent_events import pump_run
from kokoro_agent.storage.execution_context import CompletedExecutionContext


async def invoke_once(
    emitter: RunEmitter,
    agent: InvokableAgent,
    execution_config: RunnableConfig,
    payload: object,
    *,
    approval_tool_names: frozenset[str],
    source_for: SourceResolver,
    describe_tool: Callable[[str], str | None] = lambda _name: None,
    claim_terminal: Callable[[], Awaitable[bool]],
    prepare_completed: Callable[[], Awaitable[CompletedExecutionContext]],
    record_usage: Callable[[int, int], Awaitable[tuple[int, int]]],
    trace: RunnableConfig | None = None,
    recursion_limit: int = 100,
) -> bool:
    """True=已发终态(completed/failed)；False=interrupt 暂停未发终态。

    终态发射前先经 claim_terminal 原子认领：cancel/自然完成/异常三路共用同一认领键，
    多 pod 并发下恰好一个终态落地（认领失败者静默跳过）。
    """
    config = _config(execution_config, trace, recursion_limit, run_id=emitter.run_id)
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
                    proposal = plan_proposed_payload(snapshot, approval_tool_names)
                    if proposal is not None:
                        # 专用 owner 事件取代 generic awaiting，避免浏览器出现两个决策 owner。
                        await emitter.emit(proposal)
                    else:
                        for awaiting in awaiting_payloads(
                            snapshot, approval_tool_names, describe_tool=describe_tool
                        ):
                            await emitter.emit(awaiting)
                    # 暂停段的用量当场入账：终态段只报累计值，多段 run 不再少报。
                    await _record(record_usage, usage_cb.usage_metadata)
                    return False
            total_in, total_out = await _record(record_usage, usage_cb.usage_metadata)
            token_usage = (
                TokenUsage(input_tokens=total_in, output_tokens=total_out)
                if total_in or total_out
                else None
            )
            completed_context = await prepare_completed()
            # One storage claim persists the exact checkpoint owner and creates both durable
            # slots. There is intentionally no second claim_terminal on the success path.
            await emitter.emit_completed(
                completed_context,
                RunCompletedPayload(status="completed", token_usage=token_usage),
            )
            return True
        except Exception as error:  # noqa: BLE001 — 顶层兜底：任何异常统一收口为 run.failed
            if await claim_terminal():
                await emitter.emit(run_failed_payload(error))
            return True


async def _record(
    record_usage: Callable[[int, int], Awaitable[tuple[int, int]]],
    per_model: Mapping[str, UsageMetadata],
) -> tuple[int, int]:
    # callback 按 model_name 分组；跨 model 累加本段用量后入账，返回 run 级累计。
    input_tokens = 0
    output_tokens = 0
    for usage in per_model.values():
        # provider 可能漏报单项：缺省 0，绝不让计量残缺炸成 run.failed。
        input_tokens += usage.get("input_tokens", 0)
        output_tokens += usage.get("output_tokens", 0)
    return await record_usage(input_tokens, output_tokens)


def _config(
    execution_config: RunnableConfig,
    trace: RunnableConfig | None,
    recursion_limit: int,
    *,
    run_id: str,
) -> RunnableConfig:
    # 失控熔断：无限工具循环在限额处炸成 GraphRecursionError → run.failed fail-loud。
    configurable = execution_config.get("configurable")
    if not isinstance(configurable, dict):
        raise ValueError("execution config missing configurable checkpoint identity")
    config: RunnableConfig = {
        "configurable": dict(configurable),
        "recursion_limit": recursion_limit,
    }
    metadata: dict[str, object] = {}
    if trace is not None:
        callbacks = trace.get("callbacks")
        if callbacks is not None:
            config["callbacks"] = callbacks
        trace_metadata = trace.get("metadata")
        if isinstance(trace_metadata, dict):
            metadata.update(trace_metadata)
    execution_metadata = execution_config.get("metadata")
    if isinstance(execution_metadata, dict):
        metadata.update(execution_metadata)
    # Reserved run checkpoint selector is authority-owned; trace data cannot override it.
    metadata["kokoro_run_id"] = run_id
    config["metadata"] = metadata
    return config
