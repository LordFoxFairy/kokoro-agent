"""Shared types and collaboration contract for the worker supervisor."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from langchain_core.runnables.config import RunnableConfig

from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.domain.chat.models import ChatEventRecord
from kokoro_agent.domain.chat.repositories import ChatRepository
from kokoro_agent.domain.run.repository import (
    LeaseFence,
    OutboxFrame,
    RunRepository,
    SandboxBackendKind,
)
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.features.definition import Feature
from kokoro_agent.policy import Backend
from kokoro_agent.protocol import (
    ControlReceiptStatus,
    InboundMessage,
    RunCancel,
    RunErrorCode,
    RunRequest,
    RunResume,
    RunSteer,
    SubagentSource,
)
from kokoro_agent.streams.protocol import StreamProtocol


MAX_CONCURRENT_RUNS = 8
SANDBOX_CLEANUP_CLAIM_LEASE_MS = 30_000
SANDBOX_CLEANUP_RETRY_BASE_MS = 1_000
SANDBOX_CLEANUP_RETRY_MAX_MS = 60_000

AgentBuilder = Callable[[RunRequest, LeaseFence], Awaitable[AgentHandle]]
ApprovalToolNames = Callable[[RunRequest], frozenset[str]]
TraceFactory = Callable[[RunRequest], RunnableConfig | None]
SourceResolver = Callable[[str], SubagentSource]
BackendResolver = Callable[[RunRequest], Backend]
FeatureResolver = Callable[[str], Feature]
SandboxTeardown = Callable[[SandboxBackendKind, str, str], Awaitable[None]]


class SupervisorContext:
    """State and callbacks shared by the supervisor's focused mixins."""

    _build: AgentBuilder
    _run_repository: RunRepository
    _approval_tool_names: ApprovalToolNames
    _trace: TraceFactory
    _source_for: SourceResolver
    _feature_for: FeatureResolver | None
    _backend_for: BackendResolver
    _consumer: str
    _heartbeat_s: float
    _recursion_limit: int
    _events_ttl_s: int
    _run_ttl_s: int
    _outbox_republish_ms: int
    _sandbox_teardown: SandboxTeardown | None
    _chat_repository: ChatRepository | None
    _sem: asyncio.Semaphore
    _tasks: dict[str, asyncio.Task[None]]
    _task_leases: dict[str, LeaseFence]
    _control: dict[str, asyncio.Task[None]]
    _emitters: dict[str, RunEmitter]
    _leases: dict[str, LeaseFence]

    async def _control_request(self, run_id: str) -> RunRequest | None: ...

    def _control_session_matches(
        self, request: RunRequest, session_id: str
    ) -> bool: ...

    async def _consume_request(
        self, bus: StreamProtocol, request: RunRequest
    ) -> None: ...

    async def _republish_outbox(self, bus: StreamProtocol) -> None: ...

    async def _republish_pending_dispatches(self, bus: StreamProtocol) -> None: ...

    async def dispatch(self, bus: StreamProtocol, msg: InboundMessage) -> None: ...

    async def heartbeat_once(self, bus: StreamProtocol) -> None: ...

    async def _reconcile_run_receipts(
        self, bus: StreamProtocol, run_id: str
    ) -> None: ...

    async def _claim_terminal(self, run_id: str, lease: LeaseFence) -> bool: ...

    def _release_local_ownership(self, run_id: str, lease: LeaseFence) -> None: ...

    async def _control_lease(self, run_id: str) -> LeaseFence | None: ...

    async def _terminate_contract_incompatible(
        self, bus: StreamProtocol, run_id: str, rejected_seq: int
    ) -> None: ...

    async def _heartbeat_loop(self, bus: StreamProtocol) -> None: ...

    async def _on_request(self, bus: StreamProtocol, request: RunRequest) -> None: ...

    async def _start_run(
        self, bus: StreamProtocol, request: RunRequest, lease: LeaseFence
    ) -> None: ...

    async def _on_resume(self, bus: StreamProtocol, msg: RunResume) -> None: ...

    async def _on_cancel(self, bus: StreamProtocol, msg: RunCancel) -> None: ...

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
    ) -> None: ...

    async def _guarded_entry_gate(self, run_id: str, lease: LeaseFence) -> bool: ...

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
    ) -> None: ...

    def _ensure_control_listener(self, bus: StreamProtocol, run_id: str) -> None: ...

    async def _control_loop(self, bus: StreamProtocol, run_id: str) -> None: ...

    async def _guarded_control_apply(
        self, bus: StreamProtocol, run_id: str, msg: InboundMessage
    ) -> bool: ...

    async def _consume_control_frame(
        self,
        bus: StreamProtocol,
        run_id: str,
        msg: RunResume | RunCancel | RunSteer,
        stream: str,
        cursor: str,
    ) -> None: ...

    async def _apply_recorded_control(
        self, bus: StreamProtocol, run_id: str, msg: RunResume | RunCancel | RunSteer
    ) -> None: ...

    async def _emit_control_receipt(
        self,
        bus: StreamProtocol,
        run_id: str,
        command_id: str,
        status: ControlReceiptStatus,
    ) -> None: ...

    async def _control_fingerprint(
        self, run_id: str, msg: RunResume | RunCancel | RunSteer
    ) -> str | None: ...

    async def _interrupt_fingerprint(self, run_id: str) -> str | None: ...

    async def _reapply_pending_control(self, bus: StreamProtocol) -> None: ...

    async def _retry_sandbox_cleanups(self, run_id: str | None = None) -> None: ...

    async def _teardown_control(self, bus: StreamProtocol, run_id: str) -> None: ...

    async def _emitter(
        self, bus: StreamProtocol, run_id: str, lease: LeaseFence
    ) -> RunEmitter: ...

    async def _persist_outbox_chat(
        self, frame: OutboxFrame
    ) -> ChatEventRecord | None: ...

    async def _persist_user_message(self, request: RunRequest) -> None: ...

    async def _fail_terminal(
        self,
        bus: StreamProtocol,
        run_id: str,
        error: Exception,
        *,
        code: RunErrorCode | None = None,
    ) -> None: ...
