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
    RunSteer,
    SubagentSource,
    run_events_stream,
    run_control_stream,
)
from kokoro_agent.execution.approvals import (
    approval_frame,
    nested_approved_payloads,
    align_review_decisions,
    review_entries,
    review_frame,
    review_resolution_payloads,
    review_resume_value,
    align_decisions,
    has_pending_interrupt,
    resolution_payloads,
    resume_command_decisions,
)
from kokoro_agent.execution.events import RunEmitter, run_failed_payload
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.orchestration.assemble import AssembledAgent
from kokoro_agent.state import RunScope
from kokoro_agent.storage.ledger import RunLedger
from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.worker.messages import parse_inbound

LOGGER = logging.getLogger(__name__)

MAX_CONCURRENT_RUNS = 8

AgentBuilder = Callable[[RunRequest], Awaitable[AssembledAgent]]
ApprovalToolNames = Callable[[RunRequest], frozenset[str]]
TraceFactory = Callable[[RunRequest], RunnableConfig | None]
SourceResolver = Callable[[str], SubagentSource]


class RunSupervisor:
    """注入式装配的长驻调度：RunLedger 持有去重/租约/原 request/终态认领四类真相。"""

    def __init__(
        self,
        *,
        agent_builder: AgentBuilder,
        store: RunLedger,
        approval_tool_names: ApprovalToolNames,
        trace_factory: TraceFactory,
        source_for: SourceResolver,
        consumer: str,
        heartbeat_s: float = 30.0,
        max_concurrent: int = MAX_CONCURRENT_RUNS,
        recursion_limit: int = 100,
        events_ttl_s: int = 0,
        run_ttl_s: int = 0,
    ) -> None:
        self._build = agent_builder
        self._store = store
        self._approval_tool_names = approval_tool_names
        self._trace = trace_factory
        self._source_for = source_for
        self._consumer = consumer
        self._heartbeat_s = heartbeat_s
        self._recursion_limit = recursion_limit
        # retention（0=关）：终态后事件流存活期 / 终态 run 行清扫龄。
        self._events_ttl_s = events_ttl_s
        self._run_ttl_s = run_ttl_s
        self._sem = asyncio.Semaphore(max_concurrent)
        self._tasks: dict[str, asyncio.Task[None]] = {}
        # per-run control 监听任务：认领 run 后订阅其独立 control 流，终态时收束。
        self._control: dict[str, asyncio.Task[None]] = {}
        # per-run 发射器缓存：index 连续性跨 request/resume/cancel 共享；miss 时 attach 续接。
        self._emitters: dict[str, RunEmitter] = {}

    @property
    def control_listeners(self) -> Mapping[str, asyncio.Task[None]]:
        # 运维可见性：当前挂着的 per-run control 监听（收养泄漏的观测面）。
        return dict(self._control)

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
        elif isinstance(msg, RunSteer):
            # 入账信箱即完成（keep-first 幂等）：注入由 SteeringMiddleware 在下一模型轮消费；
            # 暂停 run 同样入账，resume 后首轮生效；终态后到达无消费者，安全无害。
            await self._store.add_steer(msg.run_id, msg.message_id, msg.content)
        else:
            await self._on_cancel(bus, msg)

    async def drain(self, *, timeout_s: float) -> bool:
        """优雅停机：限时等活跃 run 自然收尾（暂停 run 不算活跃，不阻塞退出）。
        返回 False=超时仍有活跃 run——如实上报，恢复权归 TTL 租约重拾。"""
        pending = [task for task in self._tasks.values() if not task.done()]
        if not pending:
            return True
        _, not_done = await asyncio.wait(pending, timeout=timeout_s)
        return not not_done

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
        # control 监听收养：暂停 run 的认领 worker 崩溃后，其 resume/cancel 无人处理会永久卡死；
        # 每 worker 心跳确保监听存在（control 流是 consumer group，多 worker 收养天然去重）。
        for run_id in await self._store.list_paused():
            self._ensure_control_listener(bus, run_id)
        if self._run_ttl_s > 0:
            purged = await self._store.purge_terminal(self._run_ttl_s * 1000)
            if purged:
                LOGGER.info("retention purged %d terminal runs", purged)

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
            assembled = await self._build(request)
        except Exception as error:  # noqa: BLE001 — 构建失败收口为 run.failed
            await self._fail_terminal(bus, request.run_id, error)
            return
        scope = RunScope.of(request)
        # 身份乘 State 轴：随初始 input 进图并落 checkpoint（resume 不重供，run/state.py 法则）。
        payload = {
            # 稳定 id=message_id：TTL 重拾对已推进 checkpoint 重放原始 input 时，add_messages 按 id 去重。
            "messages": [HumanMessage(content=request.input.content, id=request.input.message_id)],
            "scope": scope.as_state(),
        }
        self._spawn_agent(
            bus,
            assembled,
            request.run_id,
            scope.scoped_thread_id,
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
            assembled = await self._build(request)
        except Exception as error:  # noqa: BLE001 — 构建失败收口为 run.failed
            await self._fail_terminal(bus, msg.run_id, error)
            return
        scope = RunScope.of(request)
        config: RunnableConfig = {"configurable": {"thread_id": scope.scoped_thread_id}}
        snapshot = await assembled.agent.aget_state(config)
        # 幂等护栏：无 pending interrupt 的 resume 是重复/过期帧，丢弃不重跑。
        if not has_pending_interrupt(snapshot):
            LOGGER.warning("dropping resume without pending interrupt for run_id=%s", msg.run_id)
            return
        names = self._approval_tool_names(request)
        entries = review_entries(snapshot.interrupts)
        command: Command[object]
        if entries is not None:
            # 结果审核帧：投影侧 returned 被抑制，裁决后的 returned 在此直发（approve/respond/reject 全量）。
            rframe = review_frame(snapshot, entries)
            ordered = align_review_decisions(msg.decisions, rframe)
            results: dict[str, tuple[str, bool]] = {}
            for tool_id in rframe.tool_ids:
                cached = await self._store.get_tool_result(msg.run_id, tool_id)
                if cached is not None:
                    results[tool_id] = cached
            emitter = await self._emitter(bus, msg.run_id)
            for resolution in review_resolution_payloads(ordered, rframe, results):
                await emitter.emit(resolution)
            command = Command(resume=review_resume_value(ordered))
        else:
            frame, _requests = approval_frame(snapshot, names)
            # 按 tool_id 对齐到 pending 顺序；缺/多/重复/未知/respond 越界即 fail-loud（serve 兜为 run.failed）。
            ordered = align_decisions(msg.decisions, frame)
            emitter = await self._emitter(bus, msg.run_id)
            # reject/respond 不经 v3 projection → 据快照+decision 直发 tool.returned。
            for resolution in resolution_payloads(ordered, frame):
                await emitter.emit(resolution)
            if frame.nested:
                # 子代理内工具无投影通道：approve/edit 的 returned 也在此直发（占位文案），
                # 否则审批卡永远停在 awaiting（工具在子图内执行，projection 早已 drain）。
                for resolution in nested_approved_payloads(ordered, frame):
                    await emitter.emit(resolution)
            command = Command(resume={"decisions": resume_command_decisions(ordered, frame)})
        # 离开 HITL 暂停哨兵：恢复活跃租约，重新纳入心跳/过期重拾。
        await self._store.renew(msg.run_id)
        # 多 worker 收养后 resume/cancel 可能分投两处：build/aget_state 长窗内他处 cancel
        # 已终态则此处收手——终态后绝不再 spawn（复审 #1 竞态收窄）。
        if await self._store.is_terminal(msg.run_id):
            LOGGER.warning("resume lost to concurrent terminal, run_id=%s", msg.run_id)
            return
        self._spawn_agent(
            bus, assembled, msg.run_id, scope.scoped_thread_id, command, names,
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
        assembled: AssembledAgent,
        run_id: str,
        thread_id: str,
        payload: object,
        approval_tool_names: frozenset[str],
        *,
        trace: RunnableConfig | None,
    ) -> None:
        task = asyncio.create_task(
            self._guarded(bus, assembled, run_id, thread_id, payload, approval_tool_names, trace)
        )
        self._tasks[run_id] = task

        def _pop(_done: asyncio.Task[None]) -> None:
            # 按任务身份弹出：resume 已覆盖同 run_id 的新任务句柄时，旧回调不误删新句柄。
            if self._tasks.get(run_id) is task:
                del self._tasks[run_id]

        task.add_done_callback(_pop)

    async def _guarded_entry_gate(self, run_id: str) -> bool:
        # spawn 与他处 cancel 的微竞态最后一闸：进入执行前终态即静默收手。
        # 闸门是尽力而为的额外防线（终态权威在 claim_terminal）：store 故障降级放行，不引爆任务。
        try:
            return not await self._store.is_terminal(run_id)
        except Exception:  # noqa: BLE001 — 防线降级：主正确性不依赖此闸
            LOGGER.exception("terminal entry gate degraded for run_id=%s", run_id)
            return True

    async def _guarded(
        self,
        bus: StreamProtocol,
        assembled: AssembledAgent,
        run_id: str,
        thread_id: str,
        payload: object,
        approval_tool_names: frozenset[str],
        trace: RunnableConfig | None,
    ) -> None:
        # Semaphore 仅限活跃 invoke：暂停态不持有，resume 重新竞争额度。
        async with self._sem:
            if not await self._guarded_entry_gate(run_id):
                LOGGER.warning("skipping execution for terminal run_id=%s", run_id)
                return
            emitter = await self._emitter(bus, run_id)
            terminal = await invoke_once(
                emitter,
                assembled.agent,
                thread_id,
                payload,
                approval_tool_names=approval_tool_names,
                # 审批卡数据：工具自述查询（wire 只带数据，模板文案不上线）。
                describe_tool=assembled.describe_tool,
                source_for=self._source_for,
                trace=trace,
                recursion_limit=self._recursion_limit,
                # 终态认领下沉到 invoke_once：认领与发终态相邻原子，cancel 无法穿插重复发。
                claim_terminal=lambda: self._store.try_mark_terminal(run_id),
                # 用量跨段累计真源：run.completed 报累计而非末段。
                record_usage=lambda i, o: self._store.add_usage(run_id, i, o),
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
        task = asyncio.create_task(self._control_loop(bus, run_id))
        self._control[run_id] = task

        # 自退出（他处终态删流→NOGROUP 收束）也要出表：多 worker 收养下不 pop 即无界泄漏。
        def _pop(done: asyncio.Task[None]) -> None:
            if self._control.get(run_id) is done:
                self._control.pop(run_id, None)

        task.add_done_callback(_pop)

    async def _control_loop(self, bus: StreamProtocol, run_id: str) -> None:
        stream = run_control_stream(run_id)
        try:
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
        except Exception:
            # 终态清理删流先于 cancel（监听可能是当前任务）：删流后阻塞读抛 NOGROUP 属干净收束；
            # 非终态的订阅异常才是真故障，fail-loud。
            if await self._store.is_terminal(run_id):
                LOGGER.debug("control listener closed after terminal teardown: run_id=%s", run_id)
                return
            raise

    async def _teardown_control(self, bus: StreamProtocol, run_id: str) -> None:
        if self._events_ttl_s > 0:
            # 事件流非权威（mongo session_events 才是回放真源）：终态后限期存活。
            await bus.expire(run_events_stream(run_id), self._events_ttl_s)
        # 终态清理：删 control 流后取消监听任务（可能是当前任务，故删流先于 cancel）。
        with contextlib.suppress(Exception):
            await bus.delete(run_control_stream(run_id))
        task = self._control.pop(run_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _emitter(self, bus: StreamProtocol, run_id: str) -> RunEmitter:
        emitter = self._emitters.get(run_id)
        if emitter is None:
            # 审核工具集用于抑制投影侧 raw returned：无 request（如迟到 cancel）按空集处理，
            # 此时不再有投影流量，抑制与否无副作用。
            request = await self._store.get_request(run_id)
            review: frozenset[str] = (
                frozenset() if request is None
                else frozenset(request.runtime.permissions.review_tools)
            )
            emitter = await RunEmitter.attach(bus, run_id, review)
            self._emitters[run_id] = emitter
        return emitter

    async def _fail_terminal(self, bus: StreamProtocol, run_id: str, error: Exception) -> None:
        # 认领成功才发 run.failed，与并发 cancel/自然完成互斥为单一终态。
        if await self._store.try_mark_terminal(run_id):
            emitter = await self._emitter(bus, run_id)
            await emitter.emit(run_failed_payload(error))
            self._emitters.pop(run_id, None)
            await self._teardown_control(bus, run_id)
