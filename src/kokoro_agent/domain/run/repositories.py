"""Narrow Agent execution repository ports.

The worker uses one injected repository implementation, but a consumer should
depend on the smallest capability it needs.  Keeping the ports here prevents a
chat projection, event publisher, or tool middleware from accidentally
coupling itself to the complete execution persistence surface.
"""

from __future__ import annotations

from kokoro_agent.domain.run.models import (
    StaticRecipeBinding,
    RunTerminalOutcome,
    TerminalAuthority,
    TerminalCommitResult,
)

from typing import Literal, Protocol

from kokoro_agent.protocol import RunRequest
from kokoro_agent.domain.run.models import (
    ControlAdmission,
    DispatchAdmission,
    LeaseFence,
    LeasedRun,
    OutboxFrame,
    ReceiptReconcile,
    RunControlCommandRecord,
    SandboxBackendKind,
    SandboxCleanupIntent,
    StagedFrame,
    ToolJournalRecord,
)


from kokoro_agent.domain.run.interactions import (
    CheckpointObservation,
    ObservationTarget,
    ObservationStored,
    ConsumedPauseEvidence,
    UnknownResumeEvidence,
    QuiescentProbe,
    ReconcileProbeResult,
    InteractionRecoveryTarget,
    ResumeReadContext,
    AcceptedResume,
    DurablePauseSnapshot,
    InteractionCommitted,
    InteractionSnapshot,
    ReplayedResume,
    ResumeDispatchPlan,
    StartedResume,
)


class RunProfilePort(Protocol):
    async def freeze_or_verify_static_recipe(
        self, request: RunRequest, lease: LeaseFence, binding: StaticRecipeBinding
    ) -> Literal["frozen", "matched"]: ...


class RunAdmissionPort(Protocol):
    async def enqueue_dispatch(
        self, request: RunRequest, namespace: str, fence: str
    ) -> DispatchAdmission: ...

    async def try_claim(self, request: RunRequest, owner: str) -> LeaseFence | None: ...

    async def claim_dispatch(
        self, request: RunRequest, consumer: str
    ) -> LeaseFence | None: ...

    async def get_pending_dispatch(self, run_id: str) -> RunRequest | None: ...

    async def list_pending_dispatches(self, limit: int = 100) -> list[RunRequest]: ...

    async def quarantine_dispatch(
        self, raw_hash: str, source: str, reason: str
    ) -> None: ...


class RunEventPort(Protocol):
    async def next_event_index(self, run_id: str) -> int: ...

    async def reserve_event_index(
        self, run_id: str, lease: LeaseFence
    ) -> int | None: ...

    async def stage_critical_frame(
        self,
        run_id: str,
        lease: LeaseFence,
        kind: str,
        timestamp: int,
        payload_json: str,
        *,
        terminal: bool,
        event_id: str | None = None,
    ) -> StagedFrame | None: ...

    async def mark_critical_published(self, run_id: str, durable_seq: int) -> None: ...

    async def list_unpublished_outbox(self) -> list[OutboxFrame]: ...

    async def list_open_outbox_runs(self) -> list[str]: ...

    async def reconcile_receipts(
        self, run_id: str, republish_grace_ms: int = 30_000
    ) -> ReceiptReconcile: ...


class RunControlPort(Protocol):
    async def record_control_delivery(
        self,
        run_id: str,
        command_id: str,
        request_digest: str | None,
        fingerprint: str | None,
        body: str,
    ) -> bool: ...

    async def mark_control_applied(self, run_id: str, command_id: str) -> None: ...

    async def mark_control_superseded(self, run_id: str, command_id: str) -> None: ...

    async def admit_control(
        self, run_id: str, command_id: str, request_digest: str, body: str
    ) -> ControlAdmission: ...

    async def mark_control_succeeded(self, run_id: str, command_id: str) -> None: ...

    async def mark_control_failed(
        self, run_id: str, command_id: str, error_code: str | None = None
    ) -> None: ...

    async def list_pending_control_delivery(self) -> list[RunControlCommandRecord]: ...


class RunLifecyclePort(Protocol):
    async def renew(self, run_id: str, lease: LeaseFence) -> bool: ...

    async def adopt(self, run_id: str, owner: str) -> LeaseFence | None: ...

    async def pause(self, run_id: str, lease: LeaseFence) -> bool: ...

    async def reclaim_expired(self, owner: str) -> list[LeasedRun]: ...

    async def is_lease_current(self, run_id: str, lease: LeaseFence) -> bool: ...

    async def is_fence_current(self, run_id: str, lease: LeaseFence) -> bool: ...

    async def get_fence(self, run_id: str) -> LeaseFence | None: ...

    async def get_request(self, run_id: str) -> RunRequest | None: ...

    async def get_request_scoped(
        self, run_id: str, tenant_ref: str, namespace: str
    ) -> RunRequest | None: ...

    async def list_paused(self) -> list[str]: ...

    async def add_tokens(
        self, run_id: str, lease: LeaseFence, count: int
    ) -> int | None: ...

    async def add_usage(
        self,
        run_id: str,
        lease: LeaseFence,
        input_tokens: int,
        output_tokens: int,
    ) -> tuple[int, int] | None: ...

    async def purge_terminal(self, max_age_ms: int) -> int: ...

    async def verify_terminal_frame(self, frame: OutboxFrame) -> None: ...

    async def finalize_terminal(
        self,
        run_id: str,
        authority: TerminalAuthority,
        outcome: RunTerminalOutcome,
        delivery_snapshot: tuple[tuple[str, str, str], ...],
    ) -> TerminalCommitResult: ...

    async def is_terminal(self, run_id: str) -> bool: ...


class RunSandboxCleanupPort(Protocol):
    async def register_sandbox_cleanup(
        self,
        *,
        run_id: str,
        lease_generation: int,
        backend_kind: SandboxBackendKind,
        sandbox_id: str,
        teardown_ref: str,
    ) -> SandboxCleanupIntent: ...

    async def claim_sandbox_cleanups(
        self,
        owner: str,
        *,
        run_id: str | None = None,
        limit: int = 100,
        lease_ms: int = 30_000,
    ) -> list[SandboxCleanupIntent]: ...

    async def complete_sandbox_cleanup(self, cleanup_id: str) -> bool: ...

    async def reschedule_sandbox_cleanup(
        self, cleanup_id: str, error: str, *, retry_delay_ms: int
    ) -> bool: ...


class RunEffectPort(Protocol):
    async def add_steer(self, run_id: str, message_id: str, content: str) -> None: ...

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]: ...

    async def ack_steers(
        self, run_id: str, lease: LeaseFence, message_ids: list[str]
    ) -> bool: ...

    async def put_tool_result(
        self,
        run_id: str,
        lease: LeaseFence,
        tool_id: str,
        result: str,
        is_error: bool,
    ) -> tuple[str, bool] | None: ...

    async def get_tool_result(
        self, run_id: str, tool_id: str
    ) -> tuple[str, bool] | None: ...

    async def journal_tool_started(
        self, run_id: str, lease: LeaseFence, tool_call_id: str, name: str
    ) -> bool: ...

    async def journal_delivery_intent(
        self, run_id: str, lease: LeaseFence, tool_call_id: str, intent: str
    ) -> bool: ...

    async def journal_tool_finished(
        self,
        run_id: str,
        lease: LeaseFence,
        tool_call_id: str,
        result: str,
        is_error: bool,
    ) -> bool: ...

    async def clear_tool_journal(
        self, run_id: str, lease: LeaseFence, tool_call_id: str
    ) -> bool: ...

    async def get_tool_journal(
        self, run_id: str, tool_call_id: str
    ) -> ToolJournalRecord | None: ...

    async def list_delivery_journal(
        self, run_id: str
    ) -> list[tuple[str, str, str]]: ...

    async def bind_sandbox_id(
        self,
        run_id: str,
        lease: LeaseFence,
        *,
        expected_sandbox_id: str | None,
        sandbox_id: str,
        backend_kind: SandboxBackendKind,
        teardown_ref: str,
    ) -> str | None: ...

    async def get_sandbox_id(self, run_id: str) -> str | None: ...


__all__ = [
    "RunAdmissionPort",
    "RunControlPort",
    "RunEffectPort",
    "RunEventPort",
    "RunLifecyclePort",
    "RunSandboxCleanupPort",
]


class RunInteractionPort(Protocol):
    async def read_interaction(
        self, request: RunRequest
    ) -> InteractionSnapshot | None: ...
    async def record_pause(
        self, request: RunRequest, lease: LeaseFence, pause: DurablePauseSnapshot
    ) -> InteractionCommitted | ReplayedResume: ...
    async def accept_resume(
        self, request: RunRequest, command_id: str, owner: str
    ) -> AcceptedResume | ReplayedResume: ...
    async def start_resume(
        self,
        request: RunRequest,
        lease: LeaseFence,
        command_id: str,
        plan: ResumeDispatchPlan,
    ) -> StartedResume | ReplayedResume: ...
    async def mark_resume_unknown(
        self, request: RunRequest, lease: LeaseFence, command_id: str, attempt_id: str
    ) -> InteractionCommitted | ReplayedResume: ...

    async def record_checkpoint_observation(
        self, request: RunRequest, lease: LeaseFence, observation: CheckpointObservation
    ) -> ObservationStored: ...
    async def read_checkpoint_observations(
        self, request: RunRequest, target: ObservationTarget
    ) -> tuple[CheckpointObservation, ...]: ...
    async def reconcile_resume(
        self,
        request: RunRequest,
        lease: LeaseFence,
        evidence: ConsumedPauseEvidence | UnknownResumeEvidence,
    ) -> InteractionCommitted | ReplayedResume: ...
    async def record_reconcile_probe(
        self, request: RunRequest, lease: LeaseFence, probe: QuiescentProbe
    ) -> ReconcileProbeResult: ...
    async def reset_reconcile_probe(
        self, request: RunRequest, lease: LeaseFence, command_id: str, attempt_id: str
    ) -> None: ...
    async def list_unsettled_interactions(
        self, limit: int
    ) -> tuple[InteractionRecoveryTarget, ...]: ...
    async def read_resume_context(
        self, request: RunRequest, command_id: str
    ) -> ResumeReadContext | None: ...
