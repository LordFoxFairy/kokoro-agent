"""调度：订阅请求流，按 kind 派发 run.request/resume/cancel，含 resume 幂等护栏。"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables.config import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command
from pydantic import JsonValue

from kokoro_agent.application.projection.transformer import tool_resolution_event
from kokoro_agent.application.protocols.agent import StateView
from kokoro_agent.application.protocols.run_state import RunStateStore
from kokoro_agent.application.protocols.stream import StreamProtocol
from kokoro_agent.interfaces.envelope import AgentEvent
from kokoro_agent.infrastructure.observability import trace_config
from kokoro_agent.infrastructure.permission import build_interrupt_on
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
        if store is None:
            raise ValueError("RunSupervisor requires an injected RunStateStore")
        self._store: RunStateStore = store
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
            # per-message 隔离：单条消息的 dispatch 失败（如 resume 路径内联 aget_state/_resolutions
            # 抛错、坏 checkpoint/异形 snapshot）绝不冒泡杀死整个 serve 循环、令整个 worker 罢工；
            # 失败收口为该 run 的 agent_error（claim 守护，不与正常终态双发），循环继续消费下一条。
            # CancelledError 是 BaseException 不被捕获，SIGTERM/优雅停机照常生效。
            try:
                await self.dispatch(bus, msg)
            except Exception as error:  # noqa: BLE001 — 单消息容错：隔离故障，不破坏长驻消费循环
                LOGGER.exception("dispatch failed: kind=%s run_id=%s", type(msg).__name__, msg.run_id)
                await self._fail_terminal(bus, msg.run_id, error)

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
        # 同帧多工具→多决策：按 tool_id 把 wire decisions 对齐到 pending 顺序（langgraph 按序匹配
        # decisions↔interrupt，故必须重排），缺/多/重复/未知 tool_id 即 fail-loud（serve 兜为 agent_error）。
        pending = _pending_tool_calls(snapshot, names)
        ordered = _align_decisions(msg.decisions, pending)
        # reject/respond 工具不经 v3 projection（synthetic ToolMessage 跳过 tool 节点）→ 逐工具据
        # decision 直发终态（与 tool_call_awaiting 同为快照直发，replay 安全）。
        for ev in _resolutions(ordered, pending, run_id=msg.run_id):
            await self._emit_event(bus, msg.run_id, ev)
        command: Command[object] = Command(resume={"decisions": [_decision_dict(d) for d in ordered]})
        trace = trace_config(request)
        self._spawn_agent(
            bus, agent, msg.run_id, request.conversation_id, command, names, trace=trace
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
    ) -> None:
        task = asyncio.create_task(
            self._guarded(bus, agent, run_id, conversation_id, payload, interrupt_on_names, trace)
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

    @staticmethod
    async def _emit_event(bus: StreamProtocol, run_id: str, ev: AgentEvent) -> None:
        await bus.publish(events_stream(run_id), ev.model_dump())


def _interrupt_on_names(request: RunRequest) -> frozenset[str]:
    # 与建 agent 时传给 create_deep_agent 的 interrupt_on 同源：取其键集供 awaiting 子序列对齐。
    return frozenset(build_interrupt_on(request.permission_mode))


def _decision_dict(decision: ResumeDecision) -> dict[str, JsonValue]:
    # langgraph Decision 按序匹配、不含 tool_id；剔除 wire 专用的 tool_id 再喂框架。
    return decision.model_dump(exclude={"tool_id"})


@dataclass(frozen=True)
class _Pending:
    segment_id: str
    tools: tuple[tuple[str, str], ...]  # (tool_id, name)，按 interrupt 顺序


def _pending_tool_calls(snapshot: StateView, names: frozenset[str]) -> _Pending:
    # 触发 interrupt 的 AIMessage 中命中 interrupt_on 的工具子序列（与 langgraph/awaiting 同序）。
    # langgraph 图状态 values 为 Any：messages 在此边界过滤为 typed AIMessage（同 invoke._messages）。
    raw: Any = snapshot.values.get("messages") or []
    last_ai = next((m for m in reversed(raw) if isinstance(m, AIMessage)), None)
    if last_ai is None:
        return _Pending("", ())
    tools = tuple((tc["id"] or "", tc["name"]) for tc in last_ai.tool_calls if tc["name"] in names)
    return _Pending(last_ai.id or "", tools)


def _align_decisions(decisions: list[ResumeDecision], pending: _Pending) -> list[ResumeDecision]:
    # 按 tool_id 把 wire decisions 重排到 pending 顺序；逐工具一一对应，缺/多/重复/未知一律 fail-loud。
    by_id: dict[str, ResumeDecision] = {d.tool_id: d for d in decisions}
    if len(by_id) != len(decisions):
        raise ValueError("resume decisions contain duplicate tool_id")
    pending_ids = [tool_id for tool_id, _name in pending.tools]
    if set(by_id) != set(pending_ids):
        raise ValueError(f"resume decisions {sorted(by_id)} != pending tools {sorted(pending_ids)}")
    return [by_id[tool_id] for tool_id in pending_ids]


def _resolutions(
    decisions: list[ResumeDecision], pending: _Pending, *, run_id: str
) -> list[AgentEvent]:
    # reject/respond 工具不经 v3 projection 浮现 → 逐工具据 decision 直发 tool_call_end。
    # decisions 已经 _align_decisions：每个 tool_id 必在 pending 内，故直接索引（缺失即 invariant
    # 破裂，宁可 KeyError 冒泡被 serve 兜成 agent_error，也不静默发空 name 的终态事件）。
    name_by_id = dict(pending.tools)
    events: list[AgentEvent] = []
    for decision in decisions:
        if decision.type == "reject":
            rejected, responded, message = True, False, decision.message
        elif decision.type == "respond":
            rejected, responded, message = False, True, decision.message
        else:
            continue
        events.append(
            tool_resolution_event(
                tool_id=decision.tool_id,
                segment_id=pending.segment_id,
                name=name_by_id[decision.tool_id],
                result=message,
                request_id=run_id,
                rejected=rejected,
                reject_reason=message if rejected else None,
                responded=responded,
            )
        )
    return events


def _has_pending_interrupt(snapshot: StateView) -> bool:
    # StateSnapshot.interrupts 是 typed tuple[Interrupt, ...]：非空即有待审批暂停。
    return bool(snapshot.interrupts)
