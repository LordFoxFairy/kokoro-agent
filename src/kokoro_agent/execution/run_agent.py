"""单次 run 编排：run.started → 投影泵 → interrupt 暂停 / claim-before-emit 终态收口。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

from langchain_core.callbacks import get_usage_metadata_callback
from langchain_core.messages import UsageMetadata
from langchain_core.runnables.config import RunnableConfig
from langgraph.stream import CustomTransformer

from kokoro_agent.contract import RunCompletedPayload, RunStartedPayload, TokenUsage
from kokoro_agent.execution.approvals import awaiting_payloads
from kokoro_agent.execution.events import RunEmitter, SourceResolver, run_failed_payload
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.execution.publish_agent_events import pump_run


async def invoke_once(
    emitter: RunEmitter,
    agent: InvokableAgent,
    thread_id: str,
    payload: object,
    *,
    approval_tool_names: frozenset[str],
    source_for: SourceResolver,
    claim_terminal: Callable[[], Awaitable[bool]],
    trace: RunnableConfig | None = None,
    context: object | None = None,
) -> bool:
    """True=已发终态(completed/failed)；False=interrupt 暂停未发终态。

    终态发射前先经 claim_terminal 原子认领：cancel/自然完成/异常三路共用同一认领键，
    多 pod 并发下恰好一个终态落地（认领失败者静默跳过）。
    """
    config = _config(thread_id, trace)
    if emitter.at_start:
        await emitter.emit(RunStartedPayload())
    # 原生 usage callback 经 callback 树跨主/子代理自动聚合 token；每段独立计量。
    with get_usage_metadata_callback() as usage_cb:
        try:
            # runtime context 注入：工具/middleware 经 get_runtime/ToolRuntime.context 读取。
            run = await agent.astream_events(
                payload,
                version="v3",
                config=config,
                transformers=[CustomTransformer],
                context=context,
            )
            async with run:
                await pump_run(emitter, run, source_for=source_for)
                if await run.interrupted():
                    snapshot = await agent.aget_state(config)
                    for awaiting in awaiting_payloads(snapshot, approval_tool_names):
                        await emitter.emit(awaiting)
                    return False
            if await claim_terminal():
                await emitter.emit(
                    RunCompletedPayload(
                        status="completed", token_usage=_sum_usage(usage_cb.usage_metadata)
                    )
                )
            return True
        except Exception as error:  # noqa: BLE001 — 顶层兜底：任何异常统一收口为 run.failed
            if await claim_terminal():
                await emitter.emit(run_failed_payload(error))
            return True


def _sum_usage(per_model: Mapping[str, UsageMetadata]) -> TokenUsage | None:
    # callback 按 model_name 分组；wire 用扁平 total，跨 model 累加；全无用量即 null。
    if not per_model:
        return None
    input_tokens = 0
    output_tokens = 0
    for usage in per_model.values():
        # provider 可能漏报单项：缺省 0，绝不让计量残缺炸成 run.failed。
        input_tokens += usage.get("input_tokens", 0)
        output_tokens += usage.get("output_tokens", 0)
    return TokenUsage(input_tokens=input_tokens, output_tokens=output_tokens)


def _config(thread_id: str, trace: RunnableConfig | None) -> RunnableConfig:
    config: RunnableConfig = {"configurable": {"thread_id": thread_id}}
    if trace is not None:
        callbacks = trace.get("callbacks")
        metadata = trace.get("metadata")
        if callbacks is not None:
            config["callbacks"] = callbacks
        if metadata is not None:
            config["metadata"] = metadata
    return config
