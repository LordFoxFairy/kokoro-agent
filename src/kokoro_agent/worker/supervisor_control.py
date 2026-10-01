from __future__ import annotations

from kokoro_agent.domain.run.models import (
    CancelTerminalAuthority,
    QuarantineTerminalAuthority,
    RunTerminalOutcome,
)
from kokoro_agent.protocol import RunFailedPayload

import asyncio
import contextlib
import logging


from kokoro_agent import metrics
from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.domain.run.interactions import (
    AcceptedResume,
    InteractionConflict,
    ReplayedResume,
    StartedResume,
)
from kokoro_agent.infrastructure.checkpoint_interactions import (
    PreparedNativeResume,
    ObservedNativeResume,
    ResumeReadTarget,
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


class DeliveryBarrierPending(RuntimeError):
    """Keep a durable cancel command persisted until owner delivery resolves."""


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
            await self._consume_request(bus, msg)
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

    async def _on_resume(self, bus: StreamProtocol, msg: RunResume) -> None:
        request = await self._control_request(msg.run_id)
        if request is None or not self._control_session_matches(
            request, msg.session_id
        ):
            return
        await self._resume_owned(bus, request, msg.command_id)

    async def _resume_owned(
        self, bus: StreamProtocol, request: RunRequest, command_id: str
    ) -> None:
        if await self._run_repository.is_terminal(request.run_id):
            return
        try:
            accepted = await self._run_repository.accept_resume(
                request, command_id, self._consumer
            )
        except InteractionConflict:
            # A malformed/stale collection is not an execution failure and must
            # not change the current waiting head or finalize a healthy Run.
            # The command receipt is terminal; later delivery bookkeeping cannot
            # overwrite it. Internal conflict reasons are never public fields.
            await self._run_repository.mark_control_failed(
                request.run_id, command_id, "interaction_conflict"
            )
            return
        if isinstance(accepted, AcceptedResume):
            lease = accepted.lease
        else:
            context = await self._run_repository.read_resume_context(
                request, command_id
            )
            if context is None or context.intent.status.value in (
                "reconciled",
                "terminal",
            ):
                return
            lease = await self._run_repository.get_fence(request.run_id)
            if (
                lease is None
                or lease.owner != self._consumer
                or not await self._run_repository.is_lease_current(
                    request.run_id, lease
                )
            ):
                return
            task = self._tasks.get(request.run_id)
            if task is not None and not task.done():
                return
        self._leases[request.run_id] = lease
        try:
            built = await self._build(request, lease)
        except Exception as error:  # noqa: BLE001 — preserve typed build failure with its original fence
            await self._fail_terminal(
                bus, request.run_id, error, code="assembly_failed", build_lease=lease
            )
            return
        read = await self._interaction_reader(
            request=request,
            lease=lease,
            handle=built,
            target=ResumeReadTarget(command_id=command_id),
        )
        if isinstance(read, ReplayedResume):
            return
        if isinstance(read, ObservedNativeResume):
            for observation in read.observations:
                await self._run_repository.record_checkpoint_observation(
                    request, lease, observation
                )
            await self._run_repository.reconcile_resume(request, lease, read.evidence)
            return
        if not isinstance(read, PreparedNativeResume):
            raise RuntimeError("unexpected resume reader result")
        started = await self._run_repository.start_resume(
            request, lease, command_id, read.plan
        )
        if not isinstance(started, StartedResume):
            return
        self._drained_attempts = {
            key: value
            for key, value in self._drained_attempts.items()
            if key[0] != request.run_id
        }
        self._resume_attempts[request.run_id] = (command_id, started)
        self._spawn_agent(
            bus,
            built,
            request.run_id,
            RunScope.of(request).scoped_thread_id,
            read.command,
            self._approval_tool_names(request),
            trace=self._trace(request),
            lease=lease,
        )
        self._ensure_control_listener(bus, request.run_id)

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
        # Snapshot before projection, then compare under the claims row lock in
        # the atomic cancel commit. A newly started/finished tool must force a
        # retry rather than disappear behind the terminal generation fence.
        snapshot = tuple(await self._run_repository.list_delivery_journal(msg.run_id))
        if any(status == "started" for _, status, _ in snapshot):
            raise DeliveryBarrierPending("delivery journal is still in progress")
        current_fence = await self._run_repository.get_fence(msg.run_id)
        if current_fence is None:
            raise DeliveryBarrierPending("run has no claim fence yet")
        emitter = await self._emitter(bus, msg.run_id, current_fence)
        await emitter.ensure_delivery_events()
        result = await self._run_repository.finalize_terminal(
            msg.run_id,
            CancelTerminalAuthority(owner=self._consumer, command_id=msg.command_id),
            RunTerminalOutcome(
                payload=RunCompletedPayload(status="cancelled", token_usage=None),
                usage=None,
            ),
            snapshot,
        )
        terminal_lease = (
            result.lease if result.status in {"committed", "replayed"} else None
        )
        if terminal_lease is None:
            if not await self._run_repository.is_terminal(msg.run_id):
                raise DeliveryBarrierPending("delivery journal changed before cancel")
            await self._run_repository.mark_control_superseded(
                msg.run_id, msg.command_id
            )
            await self._run_repository.mark_control_failed(
                msg.run_id, msg.command_id, "control_superseded"
            )
            return
        self._leases[msg.run_id] = terminal_lease
        task = self._tasks.get(msg.run_id)
        if task is not None and not task.done():
            # 运行中：被 cancel 的 invoke task 不自发终态，统一由此分支补发 cancelled。
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        try:
            # The command and both critical frames were committed atomically.
            # A process crash here leaves queued rows for startup/heartbeat.
            await self._republish_outbox(bus)
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
        result = await self._run_repository.finalize_terminal(
            run_id,
            QuarantineTerminalAuthority(
                owner=self._consumer, rejected_seq=rejected_seq
            ),
            RunTerminalOutcome(
                payload=RunFailedPayload(code="contract_incompatible", retryable=False),
                usage=None,
            ),
            (),
        )
        terminal_lease = (
            result.lease if result.status in {"committed", "replayed"} else None
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
        fingerprint = None
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
            try:
                await self._on_cancel(bus, msg)
            except DeliveryBarrierPending:
                # Do not mark applied/failed or issue a terminal event. The
                # persisted command is retried by the heartbeat scanner.
                return
            except Exception:
                # Before CAS: persisted command remains retryable. After CAS:
                # command succeeded + queued outbox are already durable.
                LOGGER.exception("cancel apply deferred run_id=%s", run_id)
                return
            metrics.record_control_delivery("applied")
            return
        if isinstance(msg, RunResume):
            # Receipt describes durable acceptance/delivery, never native consumption.
            # Storage/read failures propagate; no alternate terminal decision.
            await self._on_resume(bus, msg)
            request = await self._control_request(run_id)
            context = (
                await self._run_repository.read_resume_context(request, msg.command_id)
                if request is not None
                else None
            )
            if context is None:
                await self._run_repository.mark_control_failed(
                    run_id, msg.command_id, "control_apply_failed"
                )
                return
            await self._run_repository.mark_control_applied(run_id, msg.command_id)
            await self._run_repository.mark_control_succeeded(run_id, msg.command_id)
            await self._emit_control_receipt(bus, run_id, msg.command_id, "applied")
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
        self._resume_attempts.pop(run_id, None)
        self._drained_attempts = {
            key: value
            for key, value in self._drained_attempts.items()
            if key[0] != run_id
        }

    def _release_local_ownership(self, run_id: str, lease: LeaseFence) -> None:
        """Drop only process-local state for one stale generation; never touch shared resources."""

        if self._leases.get(run_id) != lease:
            return
        self._leases.pop(run_id, None)
        self._emitters.pop(run_id, None)
        control = self._control.pop(run_id, None)
        if control is not None and not control.done():
            control.cancel()
