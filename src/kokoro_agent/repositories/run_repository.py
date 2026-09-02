"""Agent execution repository port and transport-neutral result records.

Concrete PostgreSQL persistence lives in ``infrastructure/postgres_run_repository.py``.
This module intentionally contains no database driver, SQL, or connection factory.
"""

from __future__ import annotations

from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.contract import RunRequest

__all__ = [
    "RunControlCommandRecord",
    "ControlAdmission",
    "ControlCommandConflict",
    "ControlAdmissionReceipt",
    "ControlAdmissionStatus",
    "DispatchAdmission",
    "DispatchConflict",
    "OutboxFrame",
    "ReceiptReconcile",
    "RunRepository",
    "StagedFrame",
    "ToolJournalRecord",
]


class RunRepository(Protocol):
    async def enqueue_dispatch(
        self, request: RunRequest, namespace: str, fence: str
    ) -> DispatchAdmission: ...

    async def try_claim(self, request: RunRequest, owner: str) -> bool: ...

    async def claim_dispatch(self, run_id: str, consumer: str) -> bool: ...

    async def quarantine_dispatch(
        self, raw_hash: str, source: str, reason: str
    ) -> None: ...

    async def stage_critical_frame(
        self,
        run_id: str,
        kind: str,
        index: int,
        timestamp: int,
        payload_json: str,
        *,
        terminal: bool,
    ) -> StagedFrame | None: ...

    async def mark_critical_published(self, run_id: str, durable_seq: int) -> None: ...

    async def list_unpublished_outbox(self) -> list[OutboxFrame]: ...

    async def list_open_outbox_runs(self) -> list[str]: ...

    async def reconcile_receipts(
        self, run_id: str, republish_grace_ms: int = 30_000
    ) -> ReceiptReconcile: ...

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

    async def renew(self, run_id: str, owner: str) -> bool: ...

    async def adopt(self, run_id: str, owner: str) -> None: ...

    async def pause(self, run_id: str) -> None: ...

    async def reclaim_expired(self, owner: str) -> list[RunRequest]: ...

    async def get_request(self, run_id: str) -> RunRequest | None: ...

    async def list_paused(self) -> list[str]: ...

    async def add_tokens(self, run_id: str, count: int) -> int: ...

    async def add_usage(
        self, run_id: str, input_tokens: int, output_tokens: int
    ) -> tuple[int, int]: ...

    async def purge_terminal(self, max_age_ms: int) -> int: ...

    async def try_mark_terminal(self, run_id: str) -> bool: ...

    async def is_terminal(self, run_id: str) -> bool: ...

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None: ...

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]: ...

    async def ack_steers(self, run_id: str, message_ids: list[str]) -> None: ...

    async def put_tool_result(
        self, run_id: str, tool_id: str, result: str, is_error: bool
    ) -> None: ...

    async def get_tool_result(
        self, run_id: str, tool_id: str
    ) -> tuple[str, bool] | None: ...

    async def journal_tool_started(
        self, run_id: str, tool_call_id: str, name: str
    ) -> bool: ...

    async def journal_tool_finished(
        self, run_id: str, tool_call_id: str, result: str, is_error: bool
    ) -> None: ...

    async def clear_tool_journal(self, run_id: str, tool_call_id: str) -> None: ...

    async def get_tool_journal(
        self, run_id: str, tool_call_id: str
    ) -> ToolJournalRecord | None: ...

    async def put_sandbox_id(self, run_id: str, sandbox_id: str) -> None: ...

    async def get_sandbox_id(self, run_id: str) -> str | None: ...


class DispatchConflict(RuntimeError):
    """A run id was reused with a different immutable launch envelope."""


class ControlCommandConflict(RuntimeError):
    """A command id was reused with a different immutable request digest."""


class DispatchAdmission(BaseModel):
    """Durable admission result used by the HTTP ingress before Redis publish."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    replayed: bool
    publish_required: bool


ControlAdmissionStatus = Literal["pending", "succeeded", "failed"]


class ControlAdmissionReceipt(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    run_id: str
    command_id: str
    request_digest: str
    status: ControlAdmissionStatus
    error_code: str | None = None


class ControlAdmission(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    receipt: ControlAdmissionReceipt
    replayed: bool
    publish_required: bool


class RunControlCommandRecord(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    run_id: str
    command_id: str
    request_digest: str | None = None
    fingerprint: str | None = None
    body: str


class ToolJournalRecord(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    name: str
    status: str
    result: str
    is_error: bool


class StagedFrame(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    durable_seq: int
    event_id: str


class OutboxFrame(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    run_id: str
    durable_seq: int
    event_id: str
    kind: str
    index: int
    timestamp: int
    payload_json: str


class ReceiptReconcile(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    rejected_seq: int | None = None
    receipt_state_lost: bool = False
    consumed_through: int | None = None
    close_requested: bool = False
    republish: list[OutboxFrame] = Field(default_factory=lambda: list[OutboxFrame]())
