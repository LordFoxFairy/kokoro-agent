"""Public façade for the Agent execution PostgreSQL repository.

The façade exposes the domain repository port while capability-specific SQL
lives in small infrastructure adapters.  Callers never depend on those
technical modules directly.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.domain.run.repository import (
    ControlAdmission,
    DispatchAdmission,
    LeaseFence,
    LeasedRun,
    OutboxFrame,
    ReceiptReconcile,
    RunControlCommandRecord,
    RunRepository,
    SandboxBackendKind,
    SandboxCleanupIntent,
    StagedFrame,
    ToolJournalRecord,
)
from kokoro_agent.infrastructure.postgres import DEFAULT_PG_SCHEMA
from kokoro_agent.infrastructure.postgres_run_admission import PostgresRunAdmission
from kokoro_agent.infrastructure.postgres_run_context import (
    PostgresRunRepositoryContext,
)
from kokoro_agent.infrastructure.postgres_run_dispatch import PostgresRunDispatch
from kokoro_agent.infrastructure.postgres_run_effects import PostgresRunEffects
from kokoro_agent.infrastructure.postgres_run_events import PostgresRunEvents
from kokoro_agent.infrastructure.postgres_run_leases import PostgresRunLeases
from kokoro_agent.infrastructure.postgres_run_sandbox import PostgresRunSandbox
from kokoro_agent.protocol import RunRequest

DEFAULT_LEASE_TTL_S = 90

__all__ = [
    "DEFAULT_LEASE_TTL_S",
    "PostgresRunRepository",
    "RunRepositorySettings",
    "make_run_repository",
]


class RunRepositorySettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    database_url: str
    schema_name: str = DEFAULT_PG_SCHEMA
    lease_ttl_ms: Annotated[int, Field(gt=0)]


class PostgresRunRepository:
    """Stable domain-port façade composed from capability adapters."""

    def __init__(
        self,
        database_url: str,
        ttl_ms: int,
        schema: str = DEFAULT_PG_SCHEMA,
        clock: Callable[[], int] | None = None,
    ) -> None:
        context = PostgresRunRepositoryContext(database_url, ttl_ms, schema, clock)
        self._context = context
        self._admission = PostgresRunAdmission(context)
        self._dispatch = PostgresRunDispatch(context)
        self._events = PostgresRunEvents(context)
        self._leases = PostgresRunLeases(context)
        self._effects = PostgresRunEffects(context)
        self._sandbox = PostgresRunSandbox(context)

    async def setup(self) -> None:
        await self._context.setup()

    async def enqueue_dispatch(
        self, request: RunRequest, namespace: str, fence: str
    ) -> DispatchAdmission:
        return await self._admission.enqueue_dispatch(request, namespace, fence)

    async def admit_control(
        self, run_id: str, command_id: str, request_digest: str, body: str
    ) -> ControlAdmission:
        return await self._admission.admit_control(
            run_id, command_id, request_digest, body
        )

    async def mark_control_succeeded(self, run_id: str, command_id: str) -> None:
        return await self._admission.mark_control_succeeded(run_id, command_id)

    async def mark_control_failed(
        self, run_id: str, command_id: str, error_code: str | None = None
    ) -> None:
        return await self._admission.mark_control_failed(run_id, command_id, error_code)

    async def record_control_delivery(
        self,
        run_id: str,
        command_id: str,
        request_digest: str | None,
        fingerprint: str | None,
        body: str,
    ) -> bool:
        return await self._admission.record_control_delivery(
            run_id, command_id, request_digest, fingerprint, body
        )

    async def mark_control_applied(self, run_id: str, command_id: str) -> None:
        return await self._admission.mark_control_applied(run_id, command_id)

    async def mark_control_superseded(self, run_id: str, command_id: str) -> None:
        return await self._admission.mark_control_superseded(run_id, command_id)

    async def list_pending_control_delivery(self) -> list[RunControlCommandRecord]:
        return await self._admission.list_pending_control_delivery()

    async def try_claim(self, request: RunRequest, owner: str) -> LeaseFence | None:
        return await self._dispatch.try_claim(request, owner)

    async def claim_dispatch(
        self, request: RunRequest, consumer: str
    ) -> LeaseFence | None:
        return await self._dispatch.claim_dispatch(request, consumer)

    async def get_pending_dispatch(self, run_id: str) -> RunRequest | None:
        return await self._dispatch.get_pending_dispatch(run_id)

    async def list_pending_dispatches(self, limit: int = 100) -> list[RunRequest]:
        return await self._dispatch.list_pending_dispatches(limit)

    async def quarantine_dispatch(
        self, raw_hash: str, source: str, reason: str
    ) -> None:
        return await self._dispatch.quarantine_dispatch(raw_hash, source, reason)

    async def stage_critical_frame(
        self,
        run_id: str,
        lease: LeaseFence,
        kind: str,
        timestamp: int,
        payload_json: str,
        *,
        terminal: bool,
    ) -> StagedFrame | None:
        return await self._events.stage_critical_frame(
            run_id, lease, kind, timestamp, payload_json, terminal=terminal
        )

    async def next_event_index(self, run_id: str) -> int:
        return await self._events.next_event_index(run_id)

    async def reserve_event_index(self, run_id: str, lease: LeaseFence) -> int | None:
        return await self._events.reserve_event_index(run_id, lease)

    async def mark_critical_published(self, run_id: str, durable_seq: int) -> None:
        return await self._events.mark_critical_published(run_id, durable_seq)

    async def list_unpublished_outbox(self) -> list[OutboxFrame]:
        return await self._events.list_unpublished_outbox()

    async def list_open_outbox_runs(self) -> list[str]:
        return await self._events.list_open_outbox_runs()

    async def reconcile_receipts(
        self, run_id: str, republish_grace_ms: int = 30000
    ) -> ReceiptReconcile:
        return await self._events.reconcile_receipts(run_id, republish_grace_ms)

    async def renew(self, run_id: str, lease: LeaseFence) -> bool:
        return await self._leases.renew(run_id, lease)

    async def adopt(self, run_id: str, owner: str) -> LeaseFence | None:
        return await self._leases.adopt(run_id, owner)

    async def pause(self, run_id: str, lease: LeaseFence) -> bool:
        return await self._leases.pause(run_id, lease)

    async def reclaim_expired(self, owner: str) -> list[LeasedRun]:
        return await self._leases.reclaim_expired(owner)

    async def is_lease_current(self, run_id: str, lease: LeaseFence) -> bool:
        return await self._leases.is_lease_current(run_id, lease)

    async def is_fence_current(self, run_id: str, lease: LeaseFence) -> bool:
        return await self._leases.is_fence_current(run_id, lease)

    async def get_fence(self, run_id: str) -> LeaseFence | None:
        return await self._leases.get_fence(run_id)

    async def get_request(self, run_id: str) -> RunRequest | None:
        return await self._leases.get_request(run_id)

    async def get_request_scoped(
        self, run_id: str, tenant_ref: str, namespace: str
    ) -> RunRequest | None:
        return await self._leases.get_request_scoped(run_id, tenant_ref, namespace)

    async def list_paused(self) -> list[str]:
        return await self._leases.list_paused()

    async def add_tokens(
        self, run_id: str, lease: LeaseFence, count: int
    ) -> int | None:
        return await self._leases.add_tokens(run_id, lease, count)

    async def add_usage(
        self, run_id: str, lease: LeaseFence, input_tokens: int, output_tokens: int
    ) -> tuple[int, int] | None:
        return await self._leases.add_usage(run_id, lease, input_tokens, output_tokens)

    async def purge_terminal(self, max_age_ms: int) -> int:
        return await self._leases.purge_terminal(max_age_ms)

    async def try_mark_terminal(self, run_id: str, lease: LeaseFence) -> bool:
        return await self._leases.try_mark_terminal(run_id, lease)

    async def fence_and_mark_terminal(
        self, run_id: str, owner: str
    ) -> LeaseFence | None:
        return await self._leases.fence_and_mark_terminal(run_id, owner)

    async def is_terminal(self, run_id: str) -> bool:
        return await self._leases.is_terminal(run_id)

    async def execute_active_effect(
        self, run_id: str, lease: LeaseFence, effect: Callable[[], Awaitable[None]]
    ) -> bool:
        return await self._effects.execute_active_effect(run_id, lease, effect)

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None:
        return await self._effects.add_steer(run_id, message_id, content)

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]:
        return await self._effects.peek_steers(run_id)

    async def ack_steers(
        self, run_id: str, lease: LeaseFence, message_ids: list[str]
    ) -> bool:
        return await self._effects.ack_steers(run_id, lease, message_ids)

    async def put_tool_result(
        self, run_id: str, lease: LeaseFence, tool_id: str, result: str, is_error: bool
    ) -> tuple[str, bool] | None:
        return await self._effects.put_tool_result(
            run_id, lease, tool_id, result, is_error
        )

    async def get_tool_result(
        self, run_id: str, tool_id: str
    ) -> tuple[str, bool] | None:
        return await self._effects.get_tool_result(run_id, tool_id)

    async def journal_tool_started(
        self, run_id: str, lease: LeaseFence, tool_call_id: str, name: str
    ) -> bool:
        return await self._effects.journal_tool_started(
            run_id, lease, tool_call_id, name
        )

    async def journal_tool_finished(
        self,
        run_id: str,
        lease: LeaseFence,
        tool_call_id: str,
        result: str,
        is_error: bool,
    ) -> bool:
        return await self._effects.journal_tool_finished(
            run_id, lease, tool_call_id, result, is_error
        )

    async def clear_tool_journal(
        self, run_id: str, lease: LeaseFence, tool_call_id: str
    ) -> bool:
        return await self._effects.clear_tool_journal(run_id, lease, tool_call_id)

    async def get_tool_journal(
        self, run_id: str, tool_call_id: str
    ) -> ToolJournalRecord | None:
        return await self._effects.get_tool_journal(run_id, tool_call_id)

    async def bind_sandbox_id(
        self,
        run_id: str,
        lease: LeaseFence,
        *,
        expected_sandbox_id: str | None,
        sandbox_id: str,
        backend_kind: SandboxBackendKind,
        teardown_ref: str,
    ) -> str | None:
        return await self._sandbox.bind_sandbox_id(
            run_id,
            lease,
            expected_sandbox_id=expected_sandbox_id,
            sandbox_id=sandbox_id,
            backend_kind=backend_kind,
            teardown_ref=teardown_ref,
        )

    async def get_sandbox_id(self, run_id: str) -> str | None:
        return await self._sandbox.get_sandbox_id(run_id)

    async def register_sandbox_cleanup(
        self,
        *,
        run_id: str,
        lease_generation: int,
        backend_kind: SandboxBackendKind,
        sandbox_id: str,
        teardown_ref: str,
    ) -> SandboxCleanupIntent:
        return await self._sandbox.register_sandbox_cleanup(
            run_id=run_id,
            lease_generation=lease_generation,
            backend_kind=backend_kind,
            sandbox_id=sandbox_id,
            teardown_ref=teardown_ref,
        )

    async def claim_sandbox_cleanups(
        self,
        owner: str,
        *,
        run_id: str | None = None,
        limit: int = 100,
        lease_ms: int = 30000,
    ) -> list[SandboxCleanupIntent]:
        return await self._sandbox.claim_sandbox_cleanups(
            owner, run_id=run_id, limit=limit, lease_ms=lease_ms
        )

    async def complete_sandbox_cleanup(self, cleanup_id: str) -> bool:
        return await self._sandbox.complete_sandbox_cleanup(cleanup_id)

    async def reschedule_sandbox_cleanup(
        self, cleanup_id: str, error: str, *, retry_delay_ms: int
    ) -> bool:
        return await self._sandbox.reschedule_sandbox_cleanup(
            cleanup_id, error, retry_delay_ms=retry_delay_ms
        )


@asynccontextmanager
async def make_run_repository(
    settings: RunRepositorySettings,
) -> AsyncGenerator[RunRepository, None]:
    repository = PostgresRunRepository(
        settings.database_url, settings.lease_ttl_ms, settings.schema_name
    )
    await repository.setup()
    try:
        yield repository
    finally:
        pass
