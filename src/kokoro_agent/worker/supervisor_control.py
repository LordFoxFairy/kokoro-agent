from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging

from langchain_core.runnables.config import RunnableConfig
from langgraph.types import Command

from kokoro_agent import metrics
from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.execution.approvals import (
    align_decisions,
    align_input_decisions,
    align_review_decisions,
    approval_frame,
    has_pending_interrupt,
    input_entries,
    input_frame,
    nested_approved_payloads,
    resolution_payloads,
    resume_command_decisions,
    review_entries,
    review_frame,
    review_resolution_payloads,
    review_resume_value,
    submit_resume_value,
)
from kokoro_agent.protocol import (
    CONSUMER_GROUP,
    ControlReceiptStatus,
    InboundMessage,
    RunCancel,
    RunCompletedPayload,
    RunControlReceiptPayload,
    RunRequest,
    RunResume,
    RunSteer,
    run_control_stream,
    run_events_stream,
)
from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.worker.messages import parse_inbound
from kokoro_agent.worker.supervisor_context import (
    SupervisorContext,
)

LOGGER = logging.getLogger(__name__)


class SupervisorControlMixin(SupervisorContext):
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
                await self._run_repository.add_steer(
                    msg.run_id, msg.message_id, msg.content
                )
            except Exception:  # noqa: BLE001 — 插话丢失可由用户重发；绝不为此把健康 run 判死
                LOGGER.exception("steer persist failed run_id=%s", msg.run_id)
        else:
            await self._on_cancel(bus, msg)

    async def _on_request(self, bus: StreamProtocol, request: RunRequest) -> None:
        # 原子认领 + TTL 租约：多 pod 消费同一请求时仅首个认领者起 run。
        lease = await self._run_repository.try_claim(request, self._consumer)
        if lease is None:
            LOGGER.debug("skipping already-claimed run_id=%s", request.run_id)
            return
        self._leases[request.run_id] = lease
        await self._start_run(bus, request, lease)

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
        lease = await self._run_repository.adopt(msg.run_id, self._consumer)
        if lease is None:
            LOGGER.warning(
                "dropping resume without paused lease ownership run_id=%s", msg.run_id
            )
            return
        self._leases[msg.run_id] = lease
        try:
            built = await self._build(request, lease)
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
            # adopt 已把暂停哨兵切回活跃租约；重复/过期 resume 不启动任务时必须恢复暂停态，
            # 否则该 Run 会成为既无执行任务、又不会被 paused scanner 接管的孤儿。
            await self._run_repository.pause(msg.run_id, lease)
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
            emitter = await self._emitter(bus, msg.run_id, lease)
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
            emitter = await self._emitter(bus, msg.run_id, lease)
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
            lease=lease,
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
        # cancel 是受信 control command：无论哪一个 consumer 收到，都原子提升 generation、
        # fence 当前执行者并认领唯一终态；不能依赖本进程恰好持有 active lease。
        terminal_lease = await self._run_repository.fence_and_mark_terminal(
            msg.run_id, self._consumer
        )
        if terminal_lease is None:
            return
        self._leases[msg.run_id] = terminal_lease
        task = self._tasks.get(msg.run_id)
        if task is not None and not task.done():
            # 运行中：被 cancel 的 invoke task 不自发终态，统一由此分支补发 cancelled。
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        try:
            emitter = await self._emitter(bus, msg.run_id, terminal_lease)
            await emitter.emit(
                RunCompletedPayload(status="cancelled", token_usage=None)
            )
            self._emitters.pop(msg.run_id, None)
        finally:
            await self._teardown_control(bus, msg.run_id)

    async def _control_lease(self, run_id: str) -> LeaseFence | None:
        lease = self._leases.get(run_id)
        if lease is not None and await self._run_repository.is_lease_current(
            run_id, lease
        ):
            return lease
        adopted = await self._run_repository.adopt(run_id, self._consumer)
        if adopted is not None:
            self._leases[run_id] = adopted
        return adopted

    async def _terminate_contract_incompatible(
        self, bus: StreamProtocol, run_id: str, rejected_seq: int
    ) -> None:
        # session NACK（quarantine）：停止执行——cancel 在跑任务；分配已被 local fence（=rejected_seq）
        # 冻结，其后 critical 帧一律 superseded 不上 wire。原子认领终态后收束（与自然完成/cancel 互斥）。
        terminal_lease = await self._run_repository.fence_and_mark_terminal(
            run_id, self._consumer
        )
        if terminal_lease is None:
            return
        self._leases[run_id] = terminal_lease
        task = self._tasks.get(run_id)
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        LOGGER.error(
            "run terminated contract_incompatible: session NACK at seq=%s run_id=%s",
            rejected_seq,
            run_id,
        )
        self._emitters.pop(run_id, None)
        await self._teardown_control(bus, run_id)

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
        self,
        bus: StreamProtocol,
        run_id: str,
        msg: RunResume | RunCancel | RunSteer,
    ) -> None:
        # persisted 已发；此处 apply + applied 时点回执。restart 续办亦经此路（不重发 persisted）。
        if isinstance(msg, RunCancel):
            # cancel：apply 即终态，applied 回执须先于 run.completed——session relayRun 遇终态即
            # 收束，其后帧不再消费；且 _on_cancel 的 teardown 会 cancel 本 control 任务，后置回执会被吞。
            await self._run_repository.mark_control_applied(run_id, msg.command_id)
            metrics.record_control_delivery("applied")
            await self._emit_control_receipt(bus, run_id, msg.command_id, "applied")
            if await self._guarded_control_apply(bus, run_id, msg):
                await self._run_repository.mark_control_succeeded(
                    run_id, msg.command_id
                )
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
        lease = await self._run_repository.get_fence(run_id)
        if lease is None:
            raise RuntimeError(
                f"cannot emit control receipt without run fence: {run_id!r}"
            )
        emitter = await self._emitter(bus, run_id, lease)
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
        # 指纹读取同样会装配 backend/guards；先原子收养暂停 lease，禁止无 fence 构建。
        # 读取完成后恢复暂停哨兵，真正 resume 再 adopt 新 generation。
        lease = await self._run_repository.adopt(run_id, self._consumer)
        if lease is None:
            return None
        self._leases[run_id] = lease
        try:
            built = await self._build(request, lease)
            scope = RunScope.of(request)
            config: RunnableConfig = {
                "configurable": {"thread_id": scope.scoped_thread_id}
            }
            snapshot = await built.runnable.aget_state(config)
        except Exception:  # noqa: BLE001 — 指纹是 stale 判定辅助，取不到降级 None（续办侧按不匹配处理）
            LOGGER.exception("interrupt fingerprint build failed run_id=%s", run_id)
            return None
        finally:
            if not await self._run_repository.pause(run_id, lease):
                self._release_local_ownership(run_id, lease)
        interrupts = snapshot.interrupts
        if not interrupts:
            return None
        joined = ",".join(sorted(str(interrupt.id) for interrupt in interrupts))
        return hashlib.sha256(joined.encode()).hexdigest()

    async def _teardown_control(self, bus: StreamProtocol, run_id: str) -> None:
        # 终态统一漏斗：三路（自然完成/失败/取消）都经此——沙箱随终态回收。
        await self._retry_sandbox_cleanups(run_id=run_id)
        if self._events_ttl_s > 0:
            # raw run event stream 只是 Session relay 传输面，终态后限期存活。
            await bus.expire(run_events_stream(run_id), self._events_ttl_s)
        # 终态清理：删 control 流后取消监听任务（可能是当前任务，故删流先于 cancel）。
        with contextlib.suppress(Exception):
            await bus.delete(run_control_stream(run_id))
        task = self._control.pop(run_id, None)
        if task is not None and not task.done():
            task.cancel()
        self._leases.pop(run_id, None)

    def _release_local_ownership(self, run_id: str, lease: LeaseFence) -> None:
        """Drop only process-local state for one stale generation; never touch shared resources."""

        if self._leases.get(run_id) != lease:
            return
        self._leases.pop(run_id, None)
        self._emitters.pop(run_id, None)
        control = self._control.pop(run_id, None)
        if control is not None and not control.done():
            control.cancel()
