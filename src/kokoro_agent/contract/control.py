"""GA control frames.

The public launch frame deliberately contains a product ``feature_key`` and a
trusted execution identity only. Agent configuration belongs to the
worker-local Feature catalog; it is never supplied by a caller or persisted as
request ``runtime`` data.
"""

from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StringConstraints, TypeAdapter

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


IdentityKind = Literal["user", "project", "service"]


class IdentityRef(StrictModel):
    """Opaque IAM-owned identity reference; GA does not interpret its value."""

    kind: IdentityKind
    opaque_ref: NonEmptyStr


class ExecutionIdentity(StrictModel):
    """Who acts, for which tenancy, and under which IAM assertion."""

    tenant_ref: NonEmptyStr
    actor: IdentityRef
    subject: IdentityRef
    identity_assertion_ref: NonEmptyStr


class RunInput(StrictModel):
    message_id: NonEmptyStr
    content: NonEmptyStr


class RunRequest(StrictModel):
    """Worker launch intent; Feature supplies every Agent/runtime decision."""

    kind: Literal["run.request"]
    # Root RPC carries a request id; Redis fixtures may omit it while the
    # transport adapter is being generated.  run_id remains the execution
    # idempotency key in RunRepository.
    request_id: NonEmptyStr | None = None
    run_id: NonEmptyStr
    session_id: NonEmptyStr
    feature_key: NonEmptyStr
    execution_identity: ExecutionIdentity
    input: RunInput
    requested_model_label: NonEmptyStr | None = None
    trace: dict[str, JsonValue] | None = None


class ApproveDecision(StrictModel):
    type: Literal["approve"]
    tool_id: NonEmptyStr
    args: dict[str, JsonValue] | None = None


class EditDecision(StrictModel):
    type: Literal["edit"]
    tool_id: NonEmptyStr
    args: dict[str, JsonValue]


class RejectDecision(StrictModel):
    type: Literal["reject"]
    tool_id: NonEmptyStr
    reason: str | None = None


class RespondDecision(StrictModel):
    type: Literal["respond"]
    tool_id: NonEmptyStr
    response: NonEmptyStr


class SubmitDecision(StrictModel):
    type: Literal["submit"]
    request_id: NonEmptyStr
    value: dict[str, JsonValue]


ResumeDecision = Annotated[
    Union[ApproveDecision, EditDecision, RejectDecision, RespondDecision, SubmitDecision],
    Field(discriminator="type"),
]


class RunResume(StrictModel):
    kind: Literal["run.resume"]
    run_id: NonEmptyStr
    session_id: NonEmptyStr
    command_id: NonEmptyStr
    request_digest: NonEmptyStr | None = None
    decisions: Annotated[list[ResumeDecision], Field(min_length=1)]


class RunCancel(StrictModel):
    kind: Literal["run.cancel"]
    run_id: NonEmptyStr
    session_id: NonEmptyStr
    command_id: NonEmptyStr
    request_digest: NonEmptyStr | None = None


class RunSteer(StrictModel):
    kind: Literal["run.steer"]
    run_id: NonEmptyStr
    session_id: NonEmptyStr
    command_id: NonEmptyStr
    request_digest: NonEmptyStr | None = None
    message_id: NonEmptyStr
    content: NonEmptyStr


InboundMessage = Annotated[
    Union[RunRequest, RunResume, RunCancel, RunSteer],
    Field(discriminator="kind"),
]

inbound_adapter: TypeAdapter[InboundMessage] = TypeAdapter(InboundMessage)


__all__ = [
    "ApproveDecision",
    "EditDecision",
    "ExecutionIdentity",
    "IdentityRef",
    "InboundMessage",
    "NonEmptyStr",
    "RejectDecision",
    "ResumeDecision",
    "RespondDecision",
    "RunCancel",
    "RunInput",
    "RunRequest",
    "RunResume",
    "RunSteer",
    "StrictModel",
    "SubmitDecision",
    "inbound_adapter",
]
