from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from langchain_core.messages import HumanMessage
from langchain_core.runnables.config import RunnableConfig

from kokoro_agent import metrics
from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.domain.chat.models import ChatEventRecord, ChatMessageDraft
from kokoro_agent.domain.run.repository import LeaseFence, OutboxFrame
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.execution.events import (
    RunEmitter,
    persist_outbox_chat_event,
    run_failed_payload,
)
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.protocol import (
    RunErrorCode,
    RunRequest,
)
from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.worker.supervisor_context import (
    SupervisorContext,
)

LOGGER = logging.getLogger(__name__)


class SupervisorExecutionMixin(SupervisorContext):
    async def _consume_request(self, bus: StreamProtocol, request: RunRequest) -> None:
        # dispatch CAS（D5）：同一 PostgreSQL 事务完成 pending→claimed 与 durable lease 创建。
        # 输（已 claimed/缺失 intent）即丢弃；赢才启动，消除两次 claim 之间的崩溃丢 Run 窗口。
        # ACK 由 serve 后置于此之后。
        # Redis 帧只是 run_id 通知，完整 envelope 必须回读 PostgreSQL，防止陈旧/伪造帧替换
        # identity、session 或 input。用户消息先于原子 claim：失败则不 CAS、不 ACK，可安全重试。
        canonical = await self._run_repository.get_pending_dispatch(request.run_id)
        if canonical is None:
            metrics.record_dispatch_claim(won=False)
            LOGGER.debug(
                "dropping dispatch without pending intent run_id=%s", request.run_id
            )
            return
        await self._persist_user_message(canonical)
        lease = await self._run_repository.claim_dispatch(canonical, self._consumer)
        if lease is None:
            metrics.record_dispatch_claim(won=False)
            LOGGER.debug("dropping late/duplicate dispatch run_id=%s", request.run_id)
            return
        metrics.record_dispatch_claim(won=True)
        self._leases[canonical.run_id] = lease
        await self._start_run(bus, canonical, lease)

    async def _start_run(
        self,
        bus: StreamProtocol,
        request: RunRequest,
        lease: LeaseFence,
    ) -> None:
        try:
            built = await self._build(request, lease)
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
            lease=lease,
        )
        # agent 就位后订阅该 run 的独立 control 流：resume/cancel 从此来，与请求流解耦。
        self._ensure_control_listener(bus, request.run_id)

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
        lease: LeaseFence,
    ) -> None:
        if self._leases.get(run_id) != lease:
            raise RuntimeError(f"cannot spawn run {run_id!r} with a stale lease fence")
        task = asyncio.create_task(
            self._guarded(
                bus,
                built,
                run_id,
                thread_id,
                payload,
                approval_tool_names,
                trace,
                lease,
            )
        )
        self._tasks[run_id] = task
        self._task_leases[run_id] = lease

        def _pop(_done: asyncio.Task[None]) -> None:
            # 按任务身份弹出：resume 已覆盖同 run_id 的新任务句柄时，旧回调不误删新句柄。
            if self._tasks.get(run_id) is task:
                del self._tasks[run_id]
                self._task_leases.pop(run_id, None)

        task.add_done_callback(_pop)

    async def _guarded_entry_gate(self, run_id: str, lease: LeaseFence) -> bool:
        # spawn 与接管/取消竞态的最后一闸：只有当前 generation 才能进入执行。
        try:
            return await self._run_repository.is_lease_current(run_id, lease)
        except Exception:  # noqa: BLE001 — 所有权无法证明时 fail closed
            LOGGER.exception("lease entry gate failed closed for run_id=%s", run_id)
            return False

    async def _guarded(
        self,
        bus: StreamProtocol,
        built: AgentHandle,
        run_id: str,
        thread_id: str,
        payload: object,
        approval_tool_names: frozenset[str],
        trace: RunnableConfig | None,
        lease: LeaseFence,
    ) -> None:
        # Semaphore 仅限活跃 invoke：暂停态不持有，resume 重新竞争额度。
        async with self._sem:
            if not await self._guarded_entry_gate(run_id, lease):
                LOGGER.warning("skipping execution for terminal run_id=%s", run_id)
                return
            emitter = await self._emitter(bus, run_id, lease)
            terminal_claimed = False

            async def claim_terminal() -> bool:
                nonlocal terminal_claimed
                # 一旦本 generation 已认领终态，后续异常收口必须保持成功状态；
                # 不能让第二次 CAS 的 False 覆盖第一次成功，导致漏发终态和漏清理。
                if not terminal_claimed:
                    terminal_claimed = await self._claim_terminal(run_id, lease)
                return terminal_claimed

            async def record_usage(
                input_tokens: int, output_tokens: int
            ) -> tuple[int, int]:
                totals = await self._run_repository.add_usage(
                    run_id, lease, input_tokens, output_tokens
                )
                if totals is None:
                    raise RuntimeError(
                        f"run {run_id!r} lost its lease while recording usage"
                    )
                return totals

            try:
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
                    claim_terminal=claim_terminal,
                    # 用量跨段累计真源：run.completed 报累计而非末段。
                    record_usage=record_usage,
                )
            finally:
                # The terminal CAS durably queues sandbox cleanup in the same
                # transaction.  A failed terminal publish must not skip the
                # immediate attempt; heartbeat recovery remains the backstop.
                await self._retry_sandbox_cleanups(run_id=run_id)
        if terminal:
            if terminal_claimed:
                self._emitters.pop(run_id, None)
                await self._teardown_control(bus, run_id)
            else:
                # invoke 已结束但终态 CAS 输给更新 generation：只释放本地句柄，
                # 绝不删除新 owner 的 control stream 或销毁其 sandbox。
                self._release_local_ownership(run_id, lease)
        else:
            # interrupt 暂停：租约置哨兵，HITL 等人期间不被过期重拾重跑；control 监听存活等 resume。
            if not await self._run_repository.pause(run_id, lease):
                self._release_local_ownership(run_id, lease)

    async def _claim_terminal(self, run_id: str, lease: LeaseFence) -> bool:
        return await self._run_repository.try_mark_terminal(run_id, lease)

    async def _emitter(
        self, bus: StreamProtocol, run_id: str, lease: LeaseFence
    ) -> RunEmitter:
        emitter = self._emitters.get(run_id)
        if emitter is None or emitter.lease != lease:
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
                lease,
                tenant_id=(
                    request.execution_identity.tenant_ref
                    if request is not None and self._chat_repository is not None
                    else None
                ),
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
            request.execution_identity.tenant_ref,
            RunScope.of(request).namespace,
            request.session_id,
            frame,
        )

    async def _persist_user_message(self, request: RunRequest) -> None:
        if self._chat_repository is None:
            return
        now = datetime.now(tz=UTC)
        await self._chat_repository.save_message(
            ChatMessageDraft(
                chat_message_id=request.input.message_id,
                tenant_id=request.execution_identity.tenant_ref,
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
        # resume/control 的失败可能发生在本 worker 尚未缓存租约之前；仅允许收养暂停态，
        # 绝不抢夺仍活跃的其他 generation。无法证明所有权时保持 fail closed，交给持有者/重拾恢复。
        lease = await self._control_lease(run_id)
        if lease is None:
            return
        if await self._claim_terminal(run_id, lease):
            try:
                emitter = await self._emitter(bus, run_id, lease)
                await emitter.emit(run_failed_payload(error, code=code))
                self._emitters.pop(run_id, None)
            finally:
                await self._teardown_control(bus, run_id)
