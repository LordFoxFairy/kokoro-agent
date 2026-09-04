"""Transport-neutral records returned by Agent execution repositories."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.protocol import RunRequest


class LeaseFence(BaseModel):
    """Monotonic execution ownership token; both fields must match every fenced write."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    owner: str = Field(min_length=1)
    generation: int = Field(ge=1)


class LeasedRun(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    request: RunRequest
    lease: LeaseFence


class DispatchAdmission(BaseModel):
    """Durable admission result used by HTTP ingress before Redis publish."""

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


SandboxBackendKind = Literal["docker", "e2b", "custom"]


class SandboxCleanupIntent(BaseModel):
    """Durable, retryable destruction identity for one run-scoped sandbox."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    cleanup_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    lease_generation: int = Field(ge=1)
    backend_kind: SandboxBackendKind
    sandbox_id: str = Field(min_length=1)
    teardown_ref: str = Field(min_length=1)
    attempt_count: int = Field(ge=0)
    next_attempt_at: int


class StagedFrame(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    durable_seq: int
    event_id: str
    index: int


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


__all__ = [
    "ControlAdmission",
    "ControlAdmissionReceipt",
    "ControlAdmissionStatus",
    "DispatchAdmission",
    "LeaseFence",
    "LeasedRun",
    "OutboxFrame",
    "ReceiptReconcile",
    "RunControlCommandRecord",
    "SandboxBackendKind",
    "SandboxCleanupIntent",
    "StagedFrame",
    "ToolJournalRecord",
]
