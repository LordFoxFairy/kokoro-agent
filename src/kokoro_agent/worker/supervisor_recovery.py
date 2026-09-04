from __future__ import annotations

import asyncio
import contextlib
import json
import logging


from kokoro_agent import metrics
from kokoro_agent.execution.events import (
    outbox_wire_event,
)
from kokoro_agent.protocol import (
    REQUESTS_MAXLEN,
    REQUESTS_STREAM,
    RUN_EVENTS_MAXLEN,
    RunCancel,
    RunResume,
    RunSteer,
    run_events_stream,
)
from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.worker.messages import parse_inbound
from kokoro_agent.worker.supervisor_context import (
    SANDBOX_CLEANUP_CLAIM_LEASE_MS,
    SANDBOX_CLEANUP_RETRY_BASE_MS,
    SANDBOX_CLEANUP_RETRY_MAX_MS,
    SupervisorContext,
)

LOGGER = logging.getLogger(__name__)


class SupervisorRecoveryMixin(SupervisorContext):
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

    async def _republish_pending_dispatches(self, bus: StreamProtocol) -> None:
        try:
            requests = await self._run_repository.list_pending_dispatches()
        except Exception:  # noqa: BLE001 — durable scanner 下一心跳重试，不杀 Worker
            LOGGER.exception("pending dispatch scan failed")
            return
        for request in requests:
            try:
                await bus.publish(
                    REQUESTS_STREAM,
                    request.model_dump(mode="json", exclude_none=True),
                    maxlen=REQUESTS_MAXLEN,
                )
            except Exception:  # noqa: BLE001 — 行仍是 pending，下一拍重建通知
                LOGGER.exception(
                    "pending dispatch republish failed run_id=%s", request.run_id
                )

    async def heartbeat_once(self, bus: StreamProtocol) -> None:
        """一轮租约维护：为活跃 run 续租，再把他处过期的 run 重拾续跑。"""
        for run_id in tuple(self._tasks):
            task = self._tasks.get(run_id)
            lease = self._task_leases.get(run_id)
            if task is None or lease is None:
                continue
            if await self._run_repository.renew(run_id, lease):
                continue
            # renew 跨 await；期间旧任务可能暂停，resume 已覆盖为新 task/generation。
            # 仅 fence 当时观测到的同一个任务与同一个 lease，绝不取消后来者。
            if (
                self._tasks.get(run_id) is not task
                or self._task_leases.get(run_id) != lease
            ):
                continue
            # fencing（审计缺口：裂脑双跑）：所有权已被他处夺走——让渡本地执行，
            # 不发终态（终态权归新属主）；双跑窗收窄到一个心跳周期。
            if not task.done():
                LOGGER.warning(
                    "fencing: lost lease ownership, yielding run_id=%s", run_id
                )
                task.cancel()
            self._release_local_ownership(run_id, lease)
        for reclaimed in await self._run_repository.reclaim_expired(self._consumer):
            request = reclaimed.request
            task = self._tasks.get(request.run_id)
            if task is not None and not task.done():
                # owner 名相同也不能把新 generation 借给旧执行；先取消，再从 checkpoint 恢复。
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            LOGGER.warning("reclaiming expired run_id=%s", request.run_id)
            self._leases[request.run_id] = reclaimed.lease
            await self._start_run(bus, request, reclaimed.lease)
        # control 监听收养：暂停 run 的认领 worker 崩溃后，其 resume/cancel 无人处理会永久卡死；
        # 每 worker 心跳确保监听存在（control 流是 consumer group，多 worker 收养天然去重）。
        for run_id in await self._run_repository.list_paused():
            self._ensure_control_listener(bus, run_id)
        # 存活期间同样补发 queued critical outbox；不是只有启动时才扫描。
        await self._republish_pending_dispatches(bus)
        await self._republish_outbox(bus)
        # R4 critical outbox 回执对账：推进 consumed/GC 已确认行，rejected NACK 终局，
        # receipt_state_lost 告警（session 落回执后收敛；无回执时纯 no-op，不影响 live 面）。
        for run_id in await self._run_repository.list_open_outbox_runs():
            await self._reconcile_run_receipts(bus, run_id)
        await self._retry_sandbox_cleanups()
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

    async def _heartbeat_loop(self, bus: StreamProtocol) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_s)
            try:
                await self.heartbeat_once(bus)
            except Exception:  # noqa: BLE001 — 存储抖动不杀心跳循环，下一拍重试
                LOGGER.exception("lease heartbeat failed")

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

    async def _retry_sandbox_cleanups(self, run_id: str | None = None) -> None:
        if self._sandbox_teardown is None:
            return
        try:
            intents = await self._run_repository.claim_sandbox_cleanups(
                self._consumer,
                run_id=run_id,
                limit=100,
                lease_ms=SANDBOX_CLEANUP_CLAIM_LEASE_MS,
            )
        except Exception:  # noqa: BLE001 — durable intent stays claimable next heartbeat
            LOGGER.exception("sandbox cleanup claim failed run_id=%s", run_id)
            return
        for intent in intents:
            try:
                await self._sandbox_teardown(
                    intent.backend_kind, intent.sandbox_id, intent.teardown_ref
                )
            except Exception as error:  # noqa: BLE001 — retry metadata is the durable recovery path
                await self._run_repository.reschedule_sandbox_cleanup(
                    intent.cleanup_id,
                    str(error),
                    retry_delay_ms=sandbox_cleanup_retry_delay(intent.attempt_count),
                )
                LOGGER.warning(
                    "sandbox cleanup failed run_id=%s kind=%s sandbox_id=%s attempt=%d",
                    intent.run_id,
                    intent.backend_kind,
                    intent.sandbox_id,
                    intent.attempt_count,
                    exc_info=True,
                )
                continue
            await self._run_repository.complete_sandbox_cleanup(intent.cleanup_id)


def sandbox_cleanup_retry_delay(attempt_count: int) -> int:
    exponent = max(0, min(attempt_count - 1, 6))
    return min(
        SANDBOX_CLEANUP_RETRY_MAX_MS,
        SANDBOX_CLEANUP_RETRY_BASE_MS * (2**exponent),
    )
