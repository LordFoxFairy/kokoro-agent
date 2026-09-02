"""长驻调度：请求流消费、per-run control 流独立化、per-message 隔离、租约心跳与过期重拾。"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import time
from collections.abc import Awaitable, Callable, Mapping

from langchain_core.messages import HumanMessage
from langchain_core.runnables.config import RunnableConfig
from langgraph.types import Command

from kokoro_agent.contract import (
    CONSUMER_GROUP,
    ControlReceiptStatus,
    REQUESTS_STREAM,
    RUN_EVENTS_MAXLEN,
    InboundMessage,
    RunCancel,
    RunCompletedPayload,
    RunControlReceiptPayload,
    RunErrorCode,
    RunRequest,
    RunResume,
    RunSteer,
    SubagentSource,
    run_events_stream,
    run_control_stream,
)
from kokoro_agent import metrics
from kokoro_agent.execution.approvals import (
    approval_frame,
    nested_approved_payloads,
    align_input_decisions,
    align_review_decisions,
    input_entries,
    input_frame,
    review_entries,
    review_frame,
    review_resolution_payloads,
    review_resume_value,
    submit_resume_value,
    align_decisions,
    has_pending_interrupt,
    resolution_payloads,
    resume_command_decisions,
)
from kokoro_agent.execution.events import (
    RunEmitter,
    outbox_wire_event,
    persist_outbox_chat_event,
    run_failed_payload,
)
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.execution.scope import RunScope
from kokoro_agent.features.definition import Feature
from kokoro_agent.repositories.run_repository import OutboxFrame, RunRepository
from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.worker.messages import parse_inbound
from kokoro_agent.policy import Backend
from kokoro_agent.chat.models import ChatEventRecord, ChatMessageDraft
from kokoro_agent.repositories.chat_repository import ChatRepository

LOGGER = logging.getLogger(__name__)

MAX_CONCURRENT_RUNS = 8


def _raw_hash(event: Mapping[str, object]) -> str:
    # 坏帧内容指纹（DLQ 去重/溯源）：稳定 JSON 序列化后 sha256；不可序列化则回退 repr。
    try:
        canonical = json.dumps(event, sort_keys=True, default=str)
    except (TypeError, ValueError):
        canonical = repr(event)
    return hashlib.sha256(canonical.encode()).hexdigest()


AgentBuilder = Callable[[RunRequest], Awaitable[AgentHandle]]
ApprovalToolNames = Callable[[RunRequest], frozenset[str]]
TraceFactory = Callable[[RunRequest], RunnableConfig | None]
SourceResolver = Callable[[str], SubagentSource]
BackendResolver = Callable[[RunRequest], Backend]
FeatureResolver = Callable[[str], Feature]


def _default_backend(_request: RunRequest) -> Backend:
    """Fallback for embedders that do not expose sandbox teardown.

    The worker still owns the actual backend selected by the Feature.  This
    resolver is only used when no teardown hook was supplied, so keeping the
    default explicit avoids an untyped lambda leaking into the supervisor.
    """

    return "state"


class RunSupervisor:
    """注入式装配的长驻调度：RunRepository 持有去重/租约/原 request/终态认领四类真相。"""

    def __init__(
        self,
        *,
        agent_builder: AgentBuilder,
        run_repository: RunRepository,
        approval_tool_names: ApprovalToolNames,
        trace_factory: TraceFactory,
        source_for: SourceResolver,
        feature_for: FeatureResolver | None = None,
        backend_for: BackendResolver | None = None,
        consumer: str,
        heartbeat_s: float = 30.0,
        max_concurrent: int = MAX_CONCURRENT_RUNS,
        recursion_limit: int = 100,
        events_ttl_s: int = 0,
        run_ttl_s: int = 0,
        # R4：published 但回执一直不来（events 流被修剪/丢失）→超此宽限期重发（复用固定身份）。
        outbox_republish_ms: int = 30_000,
        # 终态沙箱回收（审计缺口③）：按 backend 类型主动销毁；None=仅靠 TTL 自清。
        sandbox_teardown: Callable[[Backend, str | None], Awaitable[None]]
        | None = None,
        chat_repository: ChatRepository | None = None,
    ) -> None:
        self._build = agent_builder
        self._run_repository = run_repository
        self._approval_tool_names = approval_tool_names
        self._trace = trace_factory
        self._source_for = source_for
        self._feature_for = feature_for
        self._backend_for: BackendResolver = backend_for or _default_backend
        self._consumer = consumer
        self._heartbeat_s = heartbeat_s
        self._recursion_limit = recursion_limit
        # retention（0=关）：终态后事件流存活期 / 终态 run 行清扫龄。
        self._events_ttl_s = events_ttl_s
        self._run_ttl_s = run_ttl_s
        self._outbox_republish_ms = outbox_republish_ms
        self._sandbox_teardown = sandbox_teardown
        self._chat_repository = chat_repository
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

    async def _control_request(self, run_id: str) -> RunRequest | None:
        return await self._run_repository.get_request(run_id)

    def _control_session_matches(self, request: RunRequest, session_id: str) -> bool:
        if request.session_id == session_id:
            return True
        LOGGER.warning(
            "dropping foreign-session control run_id=%s control_session_id=%s request_session_id=%s",
            request.run_id,
            session_id,
            request.session_id,
        )
        return False

    async def serve(self, bus: StreamProtocol) -> None:
        # critical outbox 补发：启动即扫 queued（落库但发布未确认）行，按 seq 序补发（幂等）。
        await self._republish_outbox(bus)
        # control command 续办（R2）：persisted 未 applied 的 resume/cancel——fingerprint 匹配才续 apply。
        await self._reapply_pending_control(bus)
        heartbeat = asyncio.create_task(self._heartbeat_loop(bus))
        try:
            async for item in bus.subscribe(
                REQUESTS_STREAM, group=CONSUMER_GROUP, consumer=self._consumer
            ):
                msg = parse_inbound(item.event)
                if msg is None:
                    # 不可解析帧：DLQ 记录后 ACK（坏帧无 identity 不重投，不冒泡杀循环）。
                    with contextlib.suppress(Exception):
                        await self._run_repository.quarantine_dispatch(
                            _raw_hash(item.event),
                            source=REQUESTS_STREAM,
                            reason="unparseable",
                        )
                    await bus.ack(REQUESTS_STREAM, CONSUMER_GROUP, item.cursor)
                    continue
                if isinstance(msg, RunRequest):
                    # dispatch 序：CAS claim→赢才执行→ACK 后置到 durable claim 之后。
                    # claim 落库前崩溃 → 不 ACK、不合成终态：留 PEL 重投（§8.3 首行）。
                    try:
                        await self._consume_request(bus, msg)
                    except Exception:  # noqa: BLE001 — 认领落库前故障：不 ACK 留重投，保长驻循环
                        LOGGER.exception("dispatch claim failed run_id=%s", msg.run_id)
                        continue
                    await bus.ack(REQUESTS_STREAM, CONSUMER_GROUP, item.cursor)
                    continue
                # 非 RunRequest 帧（control 流误投/测试直投）：既有语义——先 ACK 再 dispatch，
                # 失败收口为该 run 的 run.failed（claim 守护，不与正常终态双发）。
                await bus.ack(REQUESTS_STREAM, CONSUMER_GROUP, item.cursor)
                try:
                    await self.dispatch(bus, msg)
                except Exception as error:  # noqa: BLE001 — 单消息容错：隔离故障，保长驻循环
                    LOGGER.exception(
                        "dispatch failed: kind=%s run_id=%s",
                        type(msg).__name__,
                        msg.run_id,
                    )
                    await self._fail_terminal(bus, msg.run_id, error)
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

    async def _consume_request(self, bus: StreamProtocol, request: RunRequest) -> None:
        # dispatch CAS（D5）：pending→claimed。输（已 claimed=重复投递 / expired=迟到帧）→丢弃不执行；
        # 赢 → durable 执行认领（try_claim）+ 启动。缺失 intent 与迟到/重复帧同样丢弃。
        # ACK 由 serve 后置于此之后。
        # 用户消息先于 dispatch claim durable：此处失败则不 CAS、不 ACK，重投可安全重试；写入幂等。
        await self._persist_user_message(request)
        if not await self._run_repository.claim_dispatch(request.run_id, self._consumer):
            metrics.record_dispatch_claim(won=False)
            LOGGER.debug("dropping late/duplicate dispatch run_id=%s", request.run_id)
            return
        metrics.record_dispatch_claim(won=True)
        await self._on_request(bus, request)

    async def _republish_outbox(self, bus: StreamProtocol) -> None:
        # 崩溃/瞬时故障后 queued 的 critical 行：按 seq 序补发到事件流（复用固定 event_id/durable_seq，
        # session 按 [run_id,durable_seq] unique 去重）。补发成功后置 published。
        try:
            frames = await self._run_repository.list_unpublished_outbox()
        except Exception:  # noqa: BLE001 — 补发扫描降级不阻断 serve 启动
            LOGGER.exception("critical outbox scan failed")
            return
        for frame in frames:
            try:
                await self._persist_outbox_chat(frame)
                await bus.publish(
                    run_events_stream(frame.run_id),
                    outbox_wire_event(frame),
                    maxlen=RUN_EVENTS_MAXLEN,
                )
                await self._run_repository.mark_critical_published(
                    frame.run_id, frame.durable_seq
                )
                metrics.record_outbox("republished")
            except Exception:  # noqa: BLE001 — 单帧补发失败留 queued，下一拍再试，不阻断其余
                LOGGER.exception(
                    "outbox republish failed run_id=%s seq=%s",
                    frame.run_id,
                    frame.durable_seq,
                )

    async def dispatch(self, bus: StreamProtocol, msg: InboundMessage) -> None:
        if isinstance(msg, RunRequest):
            await self._on_request(bus, msg)
        elif isinstance(msg, RunResume):
            await self._on_resume(bus, msg)
        elif isinstance(msg, RunSteer):
            # 入账信箱即完成（keep-first 幂等）：注入由 SteeringMiddleware 在下一模型轮消费；
            # 暂停 run 同样入账，resume 后首轮生效；终态后到达无消费者，安全无害。
            try:
                request = await self._control_request(msg.run_id)
                if request is None:
                    LOGGER.warning("dropping steer for unknown run_id=%s", msg.run_id)
                    return
                if not self._control_session_matches(request, msg.session_id):
                    return
                await self._run_repository.add_steer(msg.run_id, msg.message_id, msg.content)
            except Exception:  # noqa: BLE001 — 插话丢失可由用户重发；绝不为此把健康 run 判死
                LOGGER.exception("steer persist failed run_id=%s", msg.run_id)
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
            if await self._run_repository.renew(run_id, self._consumer):
                continue
            # fencing（审计缺口：裂脑双跑）：所有权已被他处夺走——让渡本地执行，
            # 不发终态（终态权归新属主）；双跑窗收窄到一个心跳周期。
            task = self._tasks.get(run_id)
            if task is not None and not task.done():
                LOGGER.warning(
                    "fencing: lost lease ownership, yielding run_id=%s", run_id
                )
                task.cancel()
        for request in await self._run_repository.reclaim_expired(self._consumer):
            if request.run_id in self._tasks:
                # 自己仍在跑（仅心跳迟到被自己拾回）：不双起。
                continue
            LOGGER.warning("reclaiming expired run_id=%s", request.run_id)
            await self._start_run(bus, request)
        # control 监听收养：暂停 run 的认领 worker 崩溃后，其 resume/cancel 无人处理会永久卡死；
        # 每 worker 心跳确保监听存在（control 流是 consumer group，多 worker 收养天然去重）。
        for run_id in await self._run_repository.list_paused():
            self._ensure_control_listener(bus, run_id)
        # 存活期间同样补发 queued critical outbox；不是只有启动时才扫描。
        await self._republish_outbox(bus)
        # R4 critical outbox 回执对账：推进 consumed/GC 已确认行，rejected NACK 终局，
        # receipt_state_lost 告警（session 落回执后收敛；无回执时纯 no-op，不影响 live 面）。
        for run_id in await self._run_repository.list_open_outbox_runs():
            await self._reconcile_run_receipts(bus, run_id)
        if self._run_ttl_s > 0:
            purged = await self._run_repository.purge_terminal(self._run_ttl_s * 1000)
            if purged:
                LOGGER.info("retention purged %d terminal runs", purged)
        # OBS-1：本 worker 活跃 run 与租约持有面（活跃 run + 收养的 control 监听）每心跳刷新。
        active = sum(1 for task in self._tasks.values() if not task.done())
        metrics.set_lease_gauges(
            active_runs=active, lease_held=active + len(self._control)
        )

    async def _reconcile_run_receipts(self, bus: StreamProtocol, run_id: str) -> None:
        try:
            outcome = await self._run_repository.reconcile_receipts(
                run_id, self._outbox_republish_ms
            )
        except Exception:  # noqa: BLE001 — 单 run 对账降级不杀心跳，下一拍重试
            LOGGER.exception("receipt reconcile failed run_id=%s", run_id)
            return
        # published 无回执超宽限期：复用固定 durable_seq/event_id 重发（session 去重幂等无害）。
        for frame in outcome.republish:
            try:
                await self._persist_outbox_chat(frame)
                await bus.publish(
                    run_events_stream(frame.run_id),
                    outbox_wire_event(frame),
                    maxlen=RUN_EVENTS_MAXLEN,
                )
                metrics.record_outbox("republished")
            except Exception:  # noqa: BLE001 — 单帧重发失败下一宽限窗再试，不阻断其余
                LOGGER.exception(
                    "stale outbox republish failed run_id=%s seq=%s",
                    run_id,
                    frame.durable_seq,
                )
        if outcome.rejected_seq is not None:
            await self._terminate_contract_incompatible(
                bus, run_id, outcome.rejected_seq
            )
        elif outcome.receipt_state_lost:
            # manifest 行缺失且未 close：不删 outbox（reconcile 已保守跳过），ERROR 告警待排查。
            metrics.record_outbox("receipt_state_lost")
            LOGGER.error(
                "receipt_state_lost: manifest missing before close, run_id=%s", run_id
            )

    async def _terminate_contract_incompatible(
        self, bus: StreamProtocol, run_id: str, rejected_seq: int
    ) -> None:
        # session NACK（quarantine）：停止执行——cancel 在跑任务；分配已被 local fence（=rejected_seq）
        # 冻结，其后 critical 帧一律 superseded 不上 wire。原子认领终态后收束（与自然完成/cancel 互斥）。
        task = self._tasks.get(run_id)
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if await self._run_repository.try_mark_terminal(run_id):
            LOGGER.error(
                "run terminated contract_incompatible: session NACK at seq=%s run_id=%s",
                rejected_seq,
                run_id,
            )
            self._emitters.pop(run_id, None)
            await self._teardown_control(bus, run_id)

    async def _heartbeat_loop(self, bus: StreamProtocol) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_s)
            try:
                await self.heartbeat_once(bus)
            except Exception:  # noqa: BLE001 — 存储抖动不杀心跳循环，下一拍重试
                LOGGER.exception("lease heartbeat failed")

    async def _on_request(self, bus: StreamProtocol, request: RunRequest) -> None:
        # 原子认领 + TTL 租约：多 pod 消费同一请求时仅首个认领者起 run。
        if not await self._run_repository.try_claim(request, self._consumer):
            LOGGER.debug("skipping already-claimed run_id=%s", request.run_id)
            return
        await self._start_run(bus, request)

    async def _start_run(self, bus: StreamProtocol, request: RunRequest) -> None:
        try:
            built = await self._build(request)
        except Exception as error:  # noqa: BLE001 — 构建失败收口为 run.failed
            await self._fail_terminal(
                bus, request.run_id, error, code="assembly_failed"
            )
            return
        scope = RunScope.of(request)
        payload: dict[str, object] = {
            # Native message ID 只服务 LangGraph 重放去重，绝不复用 GA/外部 chat_message_id。
            # run_id 内确定性派生让 TTL 重拾仍命中同一 native message。
            "messages": [
                HumanMessage(
                    content=request.input.content,
                    id=f"native-input:{request.run_id}",
                )
            ],
        }
        self._spawn_agent(
            bus,
            built,
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
        request = await self._control_request(msg.run_id)
        if request is None:
            LOGGER.warning("dropping resume for unknown run_id=%s", msg.run_id)
            return
        if not self._control_session_matches(request, msg.session_id):
            return
        if await self._run_repository.is_terminal(msg.run_id):
            LOGGER.warning("dropping resume for already-terminal run_id=%s", msg.run_id)
            return
        try:
            built = await self._build(request)
        except Exception as error:  # noqa: BLE001 — 构建失败收口为 run.failed
            await self._fail_terminal(bus, msg.run_id, error, code="assembly_failed")
            return
        scope = RunScope.of(request)
        config: RunnableConfig = {"configurable": {"thread_id": scope.scoped_thread_id}}
        snapshot = await built.runnable.aget_state(config)
        # 幂等护栏：无 pending interrupt 的 resume 是重复/过期帧，丢弃不重跑。
        if not has_pending_interrupt(snapshot):
            LOGGER.warning(
                "dropping resume without pending interrupt for run_id=%s", msg.run_id
            )
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
                cached = await self._run_repository.get_tool_result(msg.run_id, tool_id)
                if cached is not None:
                    results[tool_id] = cached
            emitter = await self._emitter(bus, msg.run_id)
            for resolution in review_resolution_payloads(ordered, rframe, results):
                await emitter.emit(resolution)
            command = Command(resume=review_resume_value(ordered))
        elif (input_ents := input_entries(snapshot.interrupts)) is not None:
            # kind=input（如 MCP elicitation）：value 回灌到 request_input 调用点续跑。
            # 不直发 tool.returned——发起工具在 resume 后原地续跑，其 returned 走正常投影浮现。
            iframe = input_frame(snapshot, input_ents)
            ordered = align_input_decisions(msg.decisions, iframe)
            command = Command(resume=submit_resume_value(ordered))
        else:
            frame, requests = approval_frame(snapshot, names)
            # 按 tool_id 对齐到 pending 顺序；缺/多/重复/未知/respond 越界即 fail-loud（serve 兜为 run.failed）。
            ordered = align_decisions(msg.decisions, frame, requests)
            emitter = await self._emitter(bus, msg.run_id)
            # reject/respond 不经 v3 projection → 据快照+decision 直发 tool.returned。
            for resolution in resolution_payloads(ordered, frame):
                await emitter.emit(resolution)
            if frame.nested:
                # 子代理内工具无投影通道：approve/edit 的 returned 也在此直发（占位文案），
                # 否则审批卡永远停在 awaiting（工具在子图内执行，projection 早已 drain）。
                for resolution in nested_approved_payloads(ordered, frame):
                    await emitter.emit(resolution)
            command = Command(
                resume={"decisions": resume_command_decisions(ordered, frame)}
            )
        # 离开 HITL 暂停哨兵：所有权交接给收养 worker（fencing 属主随之更新），恢复活跃租约。
        await self._run_repository.adopt(msg.run_id, self._consumer)
        # 多 worker 收养后 resume/cancel 可能分投两处：build/aget_state 长窗内他处 cancel
        # 已终态则此处收手——终态后绝不再 spawn（复审 #1 竞态收窄）。
        if await self._run_repository.is_terminal(msg.run_id):
            LOGGER.warning("resume lost to concurrent terminal, run_id=%s", msg.run_id)
            return
        self._spawn_agent(
            bus,
            built,
            msg.run_id,
            scope.scoped_thread_id,
            command,
            names,
            trace=self._trace(request),
        )

    async def _on_cancel(self, bus: StreamProtocol, msg: RunCancel) -> None:
        request = await self._control_request(msg.run_id)
        if request is None:
            LOGGER.warning("dropping cancel for unknown run_id=%s", msg.run_id)
            await self._run_repository.mark_control_failed(
                msg.run_id, msg.command_id, "run_not_found"
            )
            return
        if not self._control_session_matches(request, msg.session_id):
            await self._run_repository.mark_control_failed(
                msg.run_id, msg.command_id, "run_scope_forbidden"
            )
            return
        # 原子认领终态：自然完成/重复 cancel 已认领则失败者直接返回，仅胜者补发 cancelled。
        if not await self._run_repository.try_mark_terminal(msg.run_id):
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
        built: AgentHandle,
        run_id: str,
        thread_id: str,
        payload: object,
        approval_tool_names: frozenset[str],
        *,
        trace: RunnableConfig | None,
    ) -> None:
        task = asyncio.create_task(
            self._guarded(
                bus, built, run_id, thread_id, payload, approval_tool_names, trace
            )
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
            return not await self._run_repository.is_terminal(run_id)
        except Exception:  # noqa: BLE001 — 防线降级：主正确性不依赖此闸
            LOGGER.exception("terminal entry gate degraded for run_id=%s", run_id)
            return True

    async def _guarded(
        self,
        bus: StreamProtocol,
        built: AgentHandle,
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
                built.runnable,
                thread_id,
                payload,
                approval_tool_names=approval_tool_names,
                # 审批卡数据：工具自述查询（wire 只带数据，模板文案不上线）。
                describe_tool=built.describe_tool,
                source_for=self._source_for,
                trace=trace,
                recursion_limit=self._recursion_limit,
                # 终态认领下沉到 invoke_once：认领与发终态相邻原子，cancel 无法穿插重复发。
                claim_terminal=lambda: self._run_repository.try_mark_terminal(run_id),
                # 用量跨段累计真源：run.completed 报累计而非末段。
                record_usage=lambda i, o: self._run_repository.add_usage(run_id, i, o),
            )
        if terminal:
            self._emitters.pop(run_id, None)
            await self._teardown_control(bus, run_id)
        else:
            # interrupt 暂停：租约置哨兵，HITL 等人期间不被过期重拾重跑；control 监听存活等 resume。
            await self._run_repository.pause(run_id)

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
            async for item in bus.subscribe(
                stream, group=CONSUMER_GROUP, consumer=self._consumer
            ):
                msg = parse_inbound(item.event)
                # control 流按 run 隔离：只认本 run 的控制命令，异帧 ACK 丢弃。
                if msg is None or msg.run_id != run_id or isinstance(msg, RunRequest):
                    await bus.ack(stream, CONSUMER_GROUP, item.cursor)
                    continue
                # 所有 control kind 都先落同一 command ledger，再 ACK 和 apply；steer
                # 若绕过 ledger，重投会重复写入并失去统一 receipt 语义。
                await self._consume_control_frame(bus, run_id, msg, stream, item.cursor)
        except Exception:
            # 终态清理删流先于 cancel（监听可能是当前任务）：删流后阻塞读抛 NOGROUP 属干净收束；
            # 非终态的订阅异常才是真故障，fail-loud。
            if await self._run_repository.is_terminal(run_id):
                LOGGER.debug(
                    "control listener closed after terminal teardown: run_id=%s", run_id
                )
                return
            raise

    async def _guarded_control_apply(
        self, bus: StreamProtocol, run_id: str, msg: InboundMessage
    ) -> bool:
        # 单控制帧容错：隔离故障收口为 run.failed，保 control 循环（既有 dispatch 语义）。
        try:
            await self.dispatch(bus, msg)
            return True
        except Exception as error:  # noqa: BLE001
            LOGGER.exception("control dispatch failed: run_id=%s", run_id)
            await self._fail_terminal(bus, run_id, error)
            return False

    async def _consume_control_frame(
        self,
        bus: StreamProtocol,
        run_id: str,
        msg: RunResume | RunCancel | RunSteer,
        stream: str,
        cursor: str,
    ) -> None:
        # 落 run_repository command{command_id,fingerprint,status:persisted}→ACK→apply。
        # 这条路径覆盖 resume/cancel/steer，保证 command ledger 是唯一幂等边界。
        request = await self._control_request(run_id)
        if request is None:
            LOGGER.warning("dropping control for unknown run_id=%s", run_id)
            await bus.ack(stream, CONSUMER_GROUP, cursor)
            await self._run_repository.mark_control_failed(
                msg.run_id, msg.command_id, "run_not_found"
            )
            return
        if not self._control_session_matches(request, msg.session_id):
            await bus.ack(stream, CONSUMER_GROUP, cursor)
            await self._run_repository.mark_control_failed(
                msg.run_id, msg.command_id, "run_scope_forbidden"
            )
            return
        fingerprint = await self._control_fingerprint(run_id, msg)
        first = await self._run_repository.record_control_delivery(
            run_id,
            msg.command_id,
            msg.request_digest,
            fingerprint,
            msg.model_dump_json(),
        )
        await bus.ack(stream, CONSUMER_GROUP, cursor)
        if not first:
            # 重复 command_id（重发/重投）→ ledger 命中 → ACK 丢弃不重放（不双放）。
            LOGGER.debug(
                "dropping duplicate control command_id=%s run_id=%s",
                msg.command_id,
                run_id,
            )
            return
        # persisted 时点回执。
        metrics.record_control_delivery("persisted")
        await self._emit_control_receipt(bus, run_id, msg.command_id, "persisted")
        await self._apply_recorded_control(bus, run_id, msg)

    async def _apply_recorded_control(
        self, bus: StreamProtocol, run_id: str, msg: RunResume | RunCancel | RunSteer
    ) -> None:
        # persisted 已发；此处 apply + applied 时点回执。restart 续办亦经此路（不重发 persisted）。
        if isinstance(msg, RunCancel):
            # cancel：apply 即终态，applied 回执须先于 run.completed——session relayRun 遇终态即
            # 收束，其后帧不再消费；且 _on_cancel 的 teardown 会 cancel 本 control 任务，后置回执会被吞。
            await self._run_repository.mark_control_applied(run_id, msg.command_id)
            metrics.record_control_delivery("applied")
            await self._emit_control_receipt(bus, run_id, msg.command_id, "applied")
            if await self._guarded_control_apply(bus, run_id, msg):
                await self._run_repository.mark_control_succeeded(run_id, msg.command_id)
            else:
                await self._run_repository.mark_control_failed(
                    run_id, msg.command_id, "control_apply_failed"
                )
            return
        # resume/steer：apply 后再写 applied，随后把 HTTP receipt 收口为 succeeded。
        if await self._guarded_control_apply(bus, run_id, msg):
            await self._run_repository.mark_control_applied(run_id, msg.command_id)
            await self._run_repository.mark_control_succeeded(run_id, msg.command_id)
            metrics.record_control_delivery("applied")
            await self._emit_control_receipt(bus, run_id, msg.command_id, "applied")
        else:
            await self._run_repository.mark_control_failed(
                run_id, msg.command_id, "control_apply_failed"
            )

    async def _emit_control_receipt(
        self,
        bus: StreamProtocol,
        run_id: str,
        command_id: str,
        status: ControlReceiptStatus,
    ) -> None:
        # 内部 raw kind（走既有 run events 流）：进入 Agent 的 durable outbox，
        # 只供执行进度/recovery 观察，永不写入 chat projection 或直接投影浏览器。
        emitter = await self._emitter(bus, run_id)
        await emitter.emit(
            RunControlReceiptPayload(command_id=command_id, control_status=status)
        )

    async def _control_fingerprint(
        self, run_id: str, msg: RunResume | RunCancel | RunSteer
    ) -> str | None:
        # resume 记录当前 interrupt 指纹（重启续办据此判 stale）；cancel/steer 无 interrupt 依赖。
        if isinstance(msg, RunResume):
            return await self._interrupt_fingerprint(run_id)
        return None

    async def _interrupt_fingerprint(self, run_id: str) -> str | None:
        # 当前 interrupt 指纹：稳定 interrupt.id 集合的 sha256；无 interrupt/取不到=None。
        request = await self._run_repository.get_request(run_id)
        if request is None:
            return None
        try:
            built = await self._build(request)
            scope = RunScope.of(request)
            config: RunnableConfig = {
                "configurable": {"thread_id": scope.scoped_thread_id}
            }
            snapshot = await built.runnable.aget_state(config)
        except Exception:  # noqa: BLE001 — 指纹是 stale 判定辅助，取不到降级 None（续办侧按不匹配处理）
            LOGGER.exception("interrupt fingerprint build failed run_id=%s", run_id)
            return None
        interrupts = snapshot.interrupts
        if not interrupts:
            return None
        joined = ",".join(sorted(str(interrupt.id) for interrupt in interrupts))
        return hashlib.sha256(joined.encode()).hexdigest()

    async def _reapply_pending_control(self, bus: StreamProtocol) -> None:
        # 重启续办：persisted 未 applied 的 control command——fingerprint 匹配当前 interrupt 才 apply，
        # 不匹配/已终态=stale→superseded 不 apply（§8.3「persisted 后 apply 前崩溃」翻绿）。
        try:
            entries = await self._run_repository.list_pending_control_delivery()
        except Exception:  # noqa: BLE001 — 续办扫描降级不阻断 serve 启动
            LOGGER.exception("control command scan failed")
            return
        for entry in entries:
            msg = parse_inbound(json.loads(entry.body))
            if not isinstance(msg, RunResume | RunCancel | RunSteer):
                await self._run_repository.mark_control_superseded(
                    entry.run_id, entry.command_id
                )
                await self._run_repository.mark_control_failed(
                    entry.run_id, entry.command_id, "control_superseded"
                )
                metrics.record_control_delivery("superseded")
                continue
            request = await self._control_request(entry.run_id)
            if request is None or not self._control_session_matches(
                request, msg.session_id
            ):
                await self._run_repository.mark_control_superseded(
                    entry.run_id, entry.command_id
                )
                await self._run_repository.mark_control_failed(
                    entry.run_id, entry.command_id, "control_superseded"
                )
                metrics.record_control_delivery("superseded")
                continue
            if await self._run_repository.is_terminal(entry.run_id):
                await self._run_repository.mark_control_superseded(
                    entry.run_id, entry.command_id
                )
                await self._run_repository.mark_control_failed(
                    entry.run_id, entry.command_id, "control_superseded"
                )
                metrics.record_control_delivery("superseded")
                continue
            if isinstance(msg, RunResume):
                current = await self._interrupt_fingerprint(entry.run_id)
                if entry.fingerprint is None or current != entry.fingerprint:
                    await self._run_repository.mark_control_superseded(
                        entry.run_id, entry.command_id
                    )
                    await self._run_repository.mark_control_failed(
                        entry.run_id, entry.command_id, "control_superseded"
                    )
                    metrics.record_control_delivery("superseded")
                    continue
            await self._apply_recorded_control(bus, entry.run_id, msg)

    async def _teardown_sandbox(self, run_id: str) -> None:
        if self._sandbox_teardown is None:
            return
        request = await self._run_repository.get_request(run_id)
        if request is None:
            return
        sandbox_id = await self._run_repository.get_sandbox_id(run_id)
        await self._sandbox_teardown(self._backend_for(request), sandbox_id)

    async def _teardown_control(self, bus: StreamProtocol, run_id: str) -> None:
        # 终态统一漏斗：三路（自然完成/失败/取消）都经此——沙箱随终态回收。
        await self._teardown_sandbox(run_id)
        if self._events_ttl_s > 0:
            # raw run event stream 只是 Session relay 传输面，终态后限期存活。
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
            request = await self._run_repository.get_request(run_id)
            review: frozenset[str] = frozenset()
            if request is not None and self._feature_for is not None:
                feature = self._feature_for(request.feature_key)
                review = frozenset(
                    tool
                    for agent in feature.agents
                    for tool in agent.permissions.review_tools
                )
            # store 作 critical durable 事实端口：run.started/receipt/completed/failed 经其分配
            # durable_seq/event_id 并落 queued 行；live 帧仍直发。
            emitter = await RunEmitter.attach(
                bus,
                run_id,
                review,
                self._run_repository,
                namespace=(
                    RunScope.of(request).namespace
                    if request is not None and self._chat_repository is not None
                    else None
                ),
                session_id=(
                    request.session_id
                    if request is not None and self._chat_repository is not None
                    else None
                ),
                chat_repository=self._chat_repository if request is not None else None,
            )
            self._emitters[run_id] = emitter
        return emitter

    async def _persist_outbox_chat(self, frame: OutboxFrame) -> ChatEventRecord | None:
        if self._chat_repository is None:
            return None
        request = await self._run_repository.get_request(frame.run_id)
        if request is None:
            return None
        return await persist_outbox_chat_event(
            self._chat_repository,
            RunScope.of(request).namespace,
            request.session_id,
            frame,
        )

    async def _persist_user_message(self, request: RunRequest) -> None:
        if self._chat_repository is None:
            return
        now = int(time.time() * 1000)
        await self._chat_repository.save_message(
            ChatMessageDraft(
                chat_message_id=request.input.message_id,
                namespace=RunScope.of(request).namespace,
                session_id=request.session_id,
                run_id=request.run_id,
                role="user",
                content=request.input.content,
                status="completed",
                created_at=now,
                updated_at=now,
            )
        )

    async def _fail_terminal(
        self,
        bus: StreamProtocol,
        run_id: str,
        error: Exception,
        *,
        code: RunErrorCode | None = None,
    ) -> None:
        # 认领成功才发 run.failed，与并发 cancel/自然完成互斥为单一终态。
        if await self._run_repository.try_mark_terminal(run_id):
            emitter = await self._emitter(bus, run_id)
            await emitter.emit(run_failed_payload(error, code=code))
            self._emitters.pop(run_id, None)
            await self._teardown_control(bus, run_id)
