"""长驻调度：请求流消费、per-run control 流独立化、per-message 隔离、租约心跳与过期重拾。"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Mapping

from langchain_core.messages import HumanMessage
from langchain_core.runnables.config import RunnableConfig
from langgraph.types import Command

from kokoro_agent.contract import (
    CONSUMER_GROUP,
    REQUESTS_STREAM,
    InboundMessage,
    RunCancel,
    RunCompletedPayload,
    RunRequest,
    RunResume,
    SubagentSource,
    run_control_stream,
)
from kokoro_agent.execution.approvals import (
    align_decisions,
    has_pending_interrupt,
    pending_frame,
    resolution_payloads,
    resume_command_decisions,
)
from kokoro_agent.execution.events import RunEmitter, run_failed_payload
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.run.context import RunContext
from kokoro_agent.storage.run_state import RunStateStore
from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.worker.messages import parse_inbound

LOGGER = logging.getLogger(__name__)

MAX_CONCURRENT_RUNS = 8

AgentBuilder = Callable[[RunRequest], Awaitable[InvokableAgent]]
ApprovalToolNames = Callable[[RunRequest], frozenset[str]]
TraceFactory = Callable[[RunRequest], RunnableConfig | None]
SourceResolver = Callable[[str], SubagentSource]


class RunSupervisor:
    """注入式装配的长驻调度：RunStateStore 持有去重/租约/原 request/终态认领四类真相。"""

    def __init__(
        self,
        *,
        agent_builder: AgentBuilder,
        store: RunStateStore,
        approval_tool_names: ApprovalToolNames,
        trace_factory: TraceFactory,
        source_for: SourceResolver,
        consumer: str,
        heartbeat_s: float = 30.0,
        max_concurrent: int = MAX_CONCURRENT_RUNS,
    ) -> None:
        self._build = agent_builder
        self._store = store
        self._approval_tool_names = approval_tool_names
        self._trace = trace_factory
        self._source_for = source_for
        self._consumer = consumer
        self._heartbeat_s = heartbeat_s
        self._sem = asyncio.Semaphore(max_concurrent)
        self._tasks: dict[str, asyncio.Task[None]] = {}
        # per-run control 监听任务：认领 run 后订阅其独立 control 流，终态时收束。
        self._control: dict[str, asyncio.Task[None]] = {}
        # per-run 发射器缓存：index 连续性跨 request/resume/cancel 共享；miss 时 attach 续接。
        self._emitters: dict[str, RunEmitter] = {}

    @property
    def tasks(self) -> Mapping[str, asyncio.Task[None]]:
        return self._tasks

    async def serve(self, bus: StreamProtocol) -> None:
        heartbeat = asyncio.create_task(self._heartbeat_loop(bus))
        try:
            async for item in bus.subscribe(
                REQUESTS_STREAM, group=CONSUMER_GROUP, consumer=self._consumer
            ):
                msg = parse_inbound(item.event)
                # parse 后即 ack：坏帧不重投；崩溃窗口的恢复权在 TTL 租约重拾，不在 PEL 重放。
                await bus.ack(REQUESTS_STREAM, CONSUMER_GROUP, item.cursor)
                if msg is None:
                    continue
                # per-message 隔离：单条消息 dispatch 失败绝不冒泡杀死长驻循环；
                # 失败收口为该 run 的 run.failed（claim 守护，不与正常终态双发）。
                # CancelledError 是 BaseException 不被捕获，优雅停机照常生效。
                try:
                    await self.dispatch(bus, msg)
                except Exception as error:  # noqa: BLE001 — 单消息容错：隔离故障，保长驻循环
                    LOGGER.exception(
                        "dispatch failed: kind=%s run_id=%s", type(msg).__name__, msg.run_id
                    )
                    await self._fail_terminal(bus, msg.run_id, error)
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

    async def dispatch(self, bus: StreamProtocol, msg: InboundMessage) -> None:
        if isinstance(msg, RunRequest):
            await self._on_request(bus, msg)
        elif isinstance(msg, RunResume):
            await self._on_resume(bus, msg)
        else:
            await self._on_cancel(bus, msg)

    async def heartbeat_once(self, bus: StreamProtocol) -> None:
        """一轮租约维护：为活跃 run 续租，再把他处过期的 run 重拾续跑。"""
        for run_id in tuple(self._tasks):
            await self._store.renew(run_id)
        for request in await self._store.reclaim_expired():
            if request.run_id in self._tasks:
                # 自己仍在跑（仅心跳迟到被自己拾回）：不双起。
                continue
            LOGGER.warning("reclaiming expired run_id=%s", request.run_id)
            await self._start_run(bus, request)

    async def _heartbeat_loop(self, bus: StreamProtocol) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_s)
            try:
                await self.heartbeat_once(bus)
            except Exception:  # noqa: BLE001 — 存储抖动不杀心跳循环，下一拍重试
                LOGGER.exception("lease heartbeat failed")

    async def _on_request(self, bus: StreamProtocol, request: RunRequest) -> None:
        # 原子认领 + TTL 租约：多 pod 消费同一请求时仅首个认领者起 run。
        if not await self._store.try_claim(request):
            LOGGER.debug("skipping already-claimed run_id=%s", request.run_id)
            return
        await self._start_run(bus, request)

    async def _start_run(self, bus: StreamProtocol, request: RunRequest) -> None:
        try:
            agent = await self._build(request)
        except Exception as error:  # noqa: BLE001 — 构建失败收口为 run.failed
            await self._fail_terminal(bus, request.run_id, error)
            return
        context = RunContext.of(request)
        payload = {"messages": [HumanMessage(content=request.input.content or "")]}
        self._spawn_agent(
            bus,
            agent,
            request.run_id,
            context.scoped_thread_id,
            payload,
            self._approval_tool_names(request),
            trace=self._trace(request),
        )
        # agent 就位后订阅该 run 的独立 control 流：resume/cancel 从此来，与请求流解耦。
        self._ensure_control_listener(bus, request.run_id)

    async def _on_resume(self, bus: StreamProtocol, msg: RunResume) -> None:
        # 终态权威闸：cancel/自然完成后 stale resume 即使 checkpoint 仍有 interrupt 也不续跑。
        if await self._store.is_terminal(msg.run_id):
            LOGGER.warning("dropping resume for already-terminal run_id=%s", msg.run_id)
            return
        request = await self._store.get_request(msg.run_id)
        if request is None:
            LOGGER.warning("dropping resume for unknown run_id=%s", msg.run_id)
            return
        try:
            agent = await self._build(request)
        except Exception as error:  # noqa: BLE001 — 构建失败收口为 run.failed
            await self._fail_terminal(bus, msg.run_id, error)
            return
        context = RunContext.of(request)
        config: RunnableConfig = {"configurable": {"thread_id": context.scoped_thread_id}}
        snapshot = await agent.aget_state(config)
        # 幂等护栏：无 pending interrupt 的 resume 是重复/过期帧，丢弃不重跑。
        if not has_pending_interrupt(snapshot):
            LOGGER.warning("dropping resume without pending interrupt for run_id=%s", msg.run_id)
            return
        names = self._approval_tool_names(request)
        frame = pending_frame(snapshot, names)
        # 按 tool_id 对齐到 pending 顺序；缺/多/重复/未知/respond 越界即 fail-loud（serve 兜为 run.failed）。
        ordered = align_decisions(msg.decisions, frame)
        emitter = await self._emitter(bus, msg.run_id)
        # reject/respond 不经 v3 projection → 据快照+decision 直发 tool.returned。
        for resolution in resolution_payloads(ordered, frame):
            await emitter.emit(resolution)
        command: Command[object] = Command(
            resume={"decisions": resume_command_decisions(ordered, frame)}
        )
        # 离开 HITL 暂停哨兵：恢复活跃租约，重新纳入心跳/过期重拾。
        await self._store.renew(msg.run_id)
        self._spawn_agent(
            bus, agent, msg.run_id, context.scoped_thread_id, command, names,
            trace=self._trace(request),
        )

    async def _on_cancel(self, bus: StreamProtocol, msg: RunCancel) -> None:
        request = await self._store.get_request(msg.run_id)
        if request is None:
            LOGGER.warning("dropping cancel for unknown run_id=%s", msg.run_id)
            return
        # 原子认领终态：自然完成/重复 cancel 已认领则失败者直接返回，仅胜者补发 cancelled。
        if not await self._store.try_mark_terminal(msg.run_id):
            return
        task = self._tasks.get(msg.run_id)
        if task is not None and not task.done():
            # 运行中：被 cancel 的 invoke task 不自发终态，统一由此分支补发 cancelled。
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        emitter = await self._emitter(bus, msg.run_id)
        await emitter.emit(RunCompletedPayload(status="cancelled", token_usage=None))
        self._emitters.pop(msg.run_id, None)
        await self._teardown_control(bus, msg.run_id)

    def _spawn_agent(
        self,
        bus: StreamProtocol,
        agent: InvokableAgent,
        run_id: str,
        thread_id: str,
        payload: object,
        approval_tool_names: frozenset[str],
        *,
        trace: RunnableConfig | None,
    ) -> None:
        task = asyncio.create_task(
            self._guarded(bus, agent, run_id, thread_id, payload, approval_tool_names, trace)
        )
        self._tasks[run_id] = task

        def _pop(_done: asyncio.Task[None]) -> None:
            # 按任务身份弹出：resume 已覆盖同 run_id 的新任务句柄时，旧回调不误删新句柄。
            if self._tasks.get(run_id) is task:
                del self._tasks[run_id]

        task.add_done_callback(_pop)

    async def _guarded(
        self,
        bus: StreamProtocol,
        agent: InvokableAgent,
        run_id: str,
        thread_id: str,
        payload: object,
        approval_tool_names: frozenset[str],
        trace: RunnableConfig | None,
    ) -> None:
        # Semaphore 仅限活跃 invoke：暂停态不持有，resume 重新竞争额度。
        async with self._sem:
            emitter = await self._emitter(bus, run_id)
            terminal = await invoke_once(
                emitter,
                agent,
                thread_id,
                payload,
                approval_tool_names=approval_tool_names,
                source_for=self._source_for,
                trace=trace,
                # 终态认领下沉到 invoke_once：认领与发终态相邻原子，cancel 无法穿插重复发。
                claim_terminal=lambda: self._store.try_mark_terminal(run_id),
            )
        if terminal:
            self._emitters.pop(run_id, None)
            await self._teardown_control(bus, run_id)
        else:
            # interrupt 暂停：租约置哨兵，HITL 等人期间不被过期重拾重跑；control 监听存活等 resume。
            await self._store.pause(run_id)

    def _ensure_control_listener(self, bus: StreamProtocol, run_id: str) -> None:
        existing = self._control.get(run_id)
        if existing is not None and not existing.done():
            return
        self._control[run_id] = asyncio.create_task(self._control_loop(bus, run_id))

    async def _control_loop(self, bus: StreamProtocol, run_id: str) -> None:
        stream = run_control_stream(run_id)
        async for item in bus.subscribe(stream, group=CONSUMER_GROUP, consumer=self._consumer):
            msg = parse_inbound(item.event)
            await bus.ack(stream, CONSUMER_GROUP, item.cursor)
            # control 流按 run 隔离：只认本 run 的 resume/cancel，异帧安全丢弃。
            if msg is None or msg.run_id != run_id or isinstance(msg, RunRequest):
                continue
            try:
                await self.dispatch(bus, msg)
            except Exception as error:  # noqa: BLE001 — 单控制帧容错：隔离故障，保 control 循环
                LOGGER.exception("control dispatch failed: run_id=%s", run_id)
                await self._fail_terminal(bus, run_id, error)

    async def _teardown_control(self, bus: StreamProtocol, run_id: str) -> None:
        # 终态清理：删 control 流后取消监听任务（可能是当前任务，故删流先于 cancel）。
        with contextlib.suppress(Exception):
            await bus.delete(run_control_stream(run_id))
        task = self._control.pop(run_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _emitter(self, bus: StreamProtocol, run_id: str) -> RunEmitter:
        emitter = self._emitters.get(run_id)
        if emitter is None:
            emitter = await RunEmitter.attach(bus, run_id)
            self._emitters[run_id] = emitter
        return emitter

    async def _fail_terminal(self, bus: StreamProtocol, run_id: str, error: Exception) -> None:
        # 认领成功才发 run.failed，与并发 cancel/自然完成互斥为单一终态。
        if await self._store.try_mark_terminal(run_id):
            emitter = await self._emitter(bus, run_id)
            await emitter.emit(run_failed_payload(error))
            self._emitters.pop(run_id, None)
            await self._teardown_control(bus, run_id)
