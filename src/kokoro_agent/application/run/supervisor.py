"""调度：订阅请求流，按 kind 派发 run.request/resume/cancel，含 resume 幂等护栏。"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables.config import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command
from pydantic import JsonValue

from kokoro_agent.application.protocols.agent import StateView
from kokoro_agent.application.protocols.run_state import RunStateStore
from kokoro_agent.application.protocols.stream import StreamProtocol
from kokoro_agent.interfaces.envelope import AgentEvent
from kokoro_agent.infrastructure.observability import trace_config
from kokoro_agent.infrastructure.permission import build_interrupt_on
from kokoro_agent.infrastructure.run_state import MemoryRunStateStore
from kokoro_agent.application.run.invoke import InvokableAgent, events_stream, invoke_once
from kokoro_agent.interfaces.inbound import (
    InboundMessage,
    ResumeDecision,
    RunCancel,
    RunRequest,
    RunResume,
    parse_inbound,
)

LOGGER = logging.getLogger(__name__)

REQUESTS_STREAM = "kokoro:runs:requests"
MAX_CONCURRENT_RUNS = 8

AgentBuilder = Callable[[RunRequest], InvokableAgent]

# 非 resume / 非 reject 段无被拒工具（不可变空默认，避免可变默认参数）。
_NO_REJECTS: Mapping[str, str] = MappingProxyType({})


class RunSupervisor:
    """注入 agent_builder 构建 run 级 agent；RunStateStore 持久化去重 / 原 request / 终态认领。"""

    def __init__(
        self,
        agent_builder: AgentBuilder,
        checkpointer: BaseCheckpointSaver[str] | None = None,
        store: RunStateStore | None = None,
        max_concurrent: int = MAX_CONCURRENT_RUNS,
    ) -> None:
        self._build = agent_builder
        self._checkpointer = checkpointer if checkpointer is not None else InMemorySaver()
        # 持久化 run 态：请求去重 / resume 重建用原 request / 终态原子认领（多 pod 共享靠它）。
        self._store: RunStateStore = store if store is not None else MemoryRunStateStore()
        self._sem = asyncio.Semaphore(max_concurrent)
        self._tasks: dict[str, asyncio.Task[None]] = {}

    @property
    def tasks(self) -> Mapping[str, asyncio.Task[None]]:
        return self._tasks

    async def serve(self, bus: StreamProtocol) -> None:
        async for item in bus.subscribe(REQUESTS_STREAM):
            msg = parse_inbound(item.event)
            if msg is None:
                LOGGER.warning("dropping unparseable inbound on %s", REQUESTS_STREAM)
                continue
            await self.dispatch(bus, msg)

    async def dispatch(self, bus: StreamProtocol, msg: InboundMessage) -> None:
        if isinstance(msg, RunRequest):
            await self._on_request(bus, msg)
        elif isinstance(msg, RunResume):
            await self._on_resume(bus, msg)
        else:
            await self._on_cancel(bus, msg)

    async def _on_request(self, bus: StreamProtocol, request: RunRequest) -> None:
        # 原子认领：多 pod 广播同一请求时仅首个认领者起 run，其余去重丢弃。
        if not await self._store.try_register(request):
            LOGGER.debug("skipping already-processed run_id=%s", request.run_id)
            return
        payload = {"messages": [HumanMessage(content=request.input)]}
        self._spawn(bus, request, request.run_id, request.conversation_id, payload)

    async def _on_resume(self, bus: StreamProtocol, msg: RunResume) -> None:
        # 已终态权威闸：cancel/自然完成后 stale resume 即使 checkpoint 仍有 pending interrupt 也不续跑。
        if await self._store.is_terminal(msg.run_id):
            LOGGER.warning("dropping resume for already-terminal run_id=%s", msg.run_id)
            return
        request = await self._store.get_request(msg.run_id)
        if request is None:
            LOGGER.warning("dropping resume for unknown run_id=%s", msg.run_id)
            return
        try:
            agent = self._build(request)
        except Exception as error:  # noqa: BLE001 — 构建失败收口为 agent_error
            # claim-before-emit：认领成功才发终态，杜绝与并发 cancel 双终态。
            if await self._store.try_mark_terminal(msg.run_id):
                await self._emit_failed(bus, msg.run_id, error)
            return
        config: RunnableConfig = {"configurable": {"thread_id": request.conversation_id}}
        snapshot = await agent.aget_state(config)
        # 幂等护栏（spec §9.1）：无 pending interrupt 的 resume 是重复/过期帧，丢弃不重跑。
        if not _has_pending_interrupt(snapshot):
            LOGGER.warning("dropping resume without pending interrupt for run_id=%s", msg.run_id)
            return
        names = _interrupt_on_names(request)
        # 机制B：reject 决策按 pending 工具关联，下沉给 invoke→projection 发权威 rejected（replay 安全）。
        rejected = _reject_map(msg.decision, snapshot, names)
        command: Command[object] = Command(resume={"decisions": [_decision_dict(msg.decision)]})
        trace = trace_config(request)
        self._spawn_agent(
            bus, agent, msg.run_id, request.conversation_id, command, names, trace=trace, rejected=rejected
        )

    async def _on_cancel(self, bus: StreamProtocol, msg: RunCancel) -> None:
        # 原子认领终态：自然完成 / 重复 cancel 已认领则失败者直接返回，仅胜者补发 cancelled。
        if not await self._store.try_mark_terminal(msg.run_id):
            return
        task = self._tasks.get(msg.run_id)
        if task is not None and not task.done():
            # 运行中：被 cancel 的 invoke task 不自发终态，统一由此分支补发 cancelled。
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self._emit_cancelled(bus, msg.run_id)

    def _spawn(
        self,
        bus: StreamProtocol,
        request: RunRequest,
        run_id: str,
        conversation_id: str,
        payload: object,
    ) -> None:
        try:
            agent = self._build(request)
        except Exception as error:  # noqa: BLE001 — model 解析等构建失败收口为 agent_error
            # 构建失败即终态：认领后发 agent_error，挡住后续 cancel/resume 补发第二个终态。
            self._tasks[run_id] = asyncio.create_task(self._fail_terminal(bus, run_id, error))
            self._tasks[run_id].add_done_callback(lambda _t: self._tasks.pop(run_id, None))
            return
        trace = trace_config(request)
        names = _interrupt_on_names(request)
        self._spawn_agent(bus, agent, run_id, conversation_id, payload, names, trace=trace)

    def _spawn_agent(
        self,
        bus: StreamProtocol,
        agent: InvokableAgent,
        run_id: str,
        conversation_id: str,
        payload: object,
        interrupt_on_names: frozenset[str],
        trace: RunnableConfig | None = None,
        rejected: Mapping[str, str] = _NO_REJECTS,
    ) -> None:
        task = asyncio.create_task(
            self._guarded(
                bus, agent, run_id, conversation_id, payload, interrupt_on_names, trace, rejected
            )
        )
        self._tasks[run_id] = task
        task.add_done_callback(lambda _t: self._tasks.pop(run_id, None))

    async def _guarded(
        self,
        bus: StreamProtocol,
        agent: InvokableAgent,
        run_id: str,
        conversation_id: str,
        payload: object,
        interrupt_on_names: frozenset[str],
        trace: RunnableConfig | None = None,
        rejected: Mapping[str, str] = _NO_REJECTS,
    ) -> None:
        # Semaphore 仅限活跃 invoke：暂停态不持有，故 resume 重新竞争额度。
        async with self._sem:
            # 终态认领下沉到 invoke_once：认领与发终态相邻原子，cancel 无法穿插重复发。
            await invoke_once(
                bus,
                agent,
                run_id,
                conversation_id,
                payload,
                interrupt_on_names=interrupt_on_names,
                trace=trace,
                claim_terminal=lambda: self._store.try_mark_terminal(run_id),
                rejected=rejected,
            )

    async def _fail_terminal(self, bus: StreamProtocol, run_id: str, error: Exception) -> None:
        # 认领成功才发 agent_error，确保与并发 cancel 互斥为单一终态。
        if await self._store.try_mark_terminal(run_id):
            await self._emit_failed(bus, run_id, error)

    async def _emit_cancelled(self, bus: StreamProtocol, run_id: str) -> None:
        # cancel 终态即 agent_done + status=cancelled（无独立 cancelled event 类型）。
        await self._emit(bus, run_id, "agent_done", {"status": "cancelled"})

    async def _emit_failed(self, bus: StreamProtocol, run_id: str, error: Exception) -> None:
        await self._emit(
            bus, run_id, "agent_error", {"error_kind": type(error).__name__, "message": str(error)}
        )

    @staticmethod
    async def _emit(
        bus: StreamProtocol, run_id: str, event: str, data: dict[str, JsonValue]
    ) -> None:
        envelope = AgentEvent.model_validate({"event": event, "request_id": run_id, "data": data})
        await bus.publish(events_stream(run_id), envelope.model_dump())


def _interrupt_on_names(request: RunRequest) -> frozenset[str]:
    # 与建 agent 时传给 create_deep_agent 的 interrupt_on 同源：取其键集供 awaiting 子序列对齐。
    return frozenset(build_interrupt_on(request.permission_mode))


def _decision_dict(decision: ResumeDecision) -> dict[str, JsonValue]:
    # 各 arm 恰好携带其字段，model_dump 直接得 langgraph resume 所需 decision dict。
    return decision.model_dump()


def _reject_map(
    decision: ResumeDecision, snapshot: StateView, names: frozenset[str]
) -> Mapping[str, str]:
    # reject 决策关联到 pending 工具：单决策对应单 pending（langgraph 按序匹配），projection 据此发权威 rejected。
    if decision.type == "reject":
        return {tool_id: decision.message for tool_id in _pending_tool_ids(snapshot, names)}
    return _NO_REJECTS


def _pending_tool_ids(snapshot: StateView, names: frozenset[str]) -> list[str]:
    # 取触发 interrupt 的 AIMessage 中命中 interrupt_on 名集的工具 id（与 awaiting 投影同源对齐）。
    # langgraph 图状态 values 为 Any：messages 在此边界过滤为 typed AIMessage（同 invoke._messages）。
    raw: Any = snapshot.values.get("messages") or []
    last_ai = next((m for m in reversed(raw) if isinstance(m, AIMessage)), None)
    if last_ai is None:
        return []
    return [tc["id"] or "" for tc in last_ai.tool_calls if tc["name"] in names]


def _has_pending_interrupt(snapshot: StateView) -> bool:
    # StateSnapshot.interrupts 是 typed tuple[Interrupt, ...]：非空即有待审批暂停。
    return bool(snapshot.interrupts)
