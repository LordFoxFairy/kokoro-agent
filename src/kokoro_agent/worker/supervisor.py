"""Long-lived worker supervisor façade.

Message routing, execution, and recovery are implemented by focused mixins;
this module owns construction and the small public lifecycle surface.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
from collections.abc import Mapping

from kokoro_agent.domain.chat.repositories import ChatRepository
from kokoro_agent.domain.run.repository import LeaseFence, RunRepository
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.policy import Backend
from kokoro_agent.protocol import (
    CONSUMER_GROUP,
    REQUESTS_STREAM,
    RunRequest,
)
from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.worker.messages import parse_inbound
from kokoro_agent.worker.supervisor_context import (
    AgentBuilder,
    ApprovalToolNames,
    BackendResolver,
    FeatureResolver,
    MAX_CONCURRENT_RUNS,
    SandboxTeardown,
    SourceResolver,
    TraceFactory,
)
from kokoro_agent.worker.supervisor_control import SupervisorControlMixin
from kokoro_agent.worker.supervisor_execution import SupervisorExecutionMixin
from kokoro_agent.worker.supervisor_recovery import SupervisorRecoveryMixin

LOGGER = logging.getLogger(__name__)


def _raw_hash(event: Mapping[str, object]) -> str:
    # 坏帧内容指纹（DLQ 去重/溯源）：稳定 JSON 序列化后 sha256；不可序列化则回退 repr。
    try:
        canonical = json.dumps(event, sort_keys=True, default=str)
    except (TypeError, ValueError):
        canonical = repr(event)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _default_backend(_request: RunRequest) -> Backend:
    """Explicit default for embedders that do not expose sandbox teardown."""

    return "state"


class RunSupervisor(
    SupervisorRecoveryMixin,
    SupervisorControlMixin,
    SupervisorExecutionMixin,
):
    """Injected long-lived scheduler with one public lifecycle façade."""

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
        sandbox_teardown: SandboxTeardown | None = None,
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
        # 与具体 task 句柄绑定，不能从可能已被 cancel/NACK/reclaim 更新的全局 lease map 回读。
        self._task_leases: dict[str, LeaseFence] = {}
        # per-run control 监听任务：认领 run 后订阅其独立 control 流，终态时收束。
        self._control: dict[str, asyncio.Task[None]] = {}
        # per-run 发射器缓存：index 连续性跨 request/resume/cancel 共享；miss 时 attach 续接。
        self._emitters: dict[str, RunEmitter] = {}
        # 每次 claim/adopt/reclaim 都产生单调 generation；同一 owner 名复用也不能让旧任务续写。
        self._leases: dict[str, LeaseFence] = {}

    @property
    def control_listeners(self) -> Mapping[str, asyncio.Task[None]]:
        # 运维可见性：当前挂着的 per-run control 监听（收养泄漏的观测面）。
        return dict(self._control)

    @property
    def tasks(self) -> Mapping[str, asyncio.Task[None]]:
        return self._tasks

    async def serve(self, bus: StreamProtocol) -> None:
        # PostgreSQL dispatch admission 是唯一 durable 真相；Redis 仅承载可重放通知。启动先修复
        # ingress 在落库后、XADD 前崩溃，或请求流被 maxlen 修剪造成的 pending 缺帧。
        await self._republish_pending_dispatches(bus)
        # critical outbox 补发：启动即扫 queued（落库但发布未确认）行，按 seq 序补发（幂等）。
        await self._republish_outbox(bus)
        # control command 续办（R2）：persisted 未 applied 的 resume/cancel——fingerprint 匹配才续 apply。
        await self._reapply_pending_control(bus)
        await self._retry_sandbox_cleanups()
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

    async def drain(self, *, timeout_s: float) -> bool:
        """优雅停机：限时等活跃 run 自然收尾（暂停 run 不算活跃，不阻塞退出）。
        返回 False=超时仍有活跃 run——如实上报，恢复权归 TTL 租约重拾。"""
        pending = [task for task in self._tasks.values() if not task.done()]
        if not pending:
            return True
        _, not_done = await asyncio.wait(pending, timeout=timeout_s)
        return not not_done
