"""GA control frames.

The public launch frame carries a product ``feature_key``, a trusted execution
identity, and an explicit exact skill source selection. Agent configuration
belongs to the worker-local Feature catalog; it is never supplied by a caller
or persisted as request ``runtime`` data.
"""

from __future__ import annotations

import json
import re
from typing import Annotated, Literal, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    TypeAdapter,
    field_validator,
)

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


IdentityKind = Literal["user", "project", "service"]
_SKILL_SOURCE_REF = re.compile(r"skill:[A-Za-z0-9][A-Za-z0-9._:-]{0,190}\Z")
_MAX_SKILL_SOURCE_REFS = 16
_MAX_SKILL_SELECTION_BYTES = 4096
_SKILL_SOURCE_REFS_ADAPTER: TypeAdapter[list[str] | tuple[str, ...]] = TypeAdapter(
    list[str] | tuple[str, ...]
)


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
    # HTTP ingress supplies request_id. Direct worker callers may omit it;
    # run_id remains the execution idempotency key in RunRepository.
    request_id: NonEmptyStr | None = None
    run_id: NonEmptyStr
    session_id: NonEmptyStr
    feature_key: NonEmptyStr
    selected_skill_source_refs: tuple[str, ...]
    execution_identity: ExecutionIdentity
    input: RunInput
    requested_model_label: NonEmptyStr | None = None
    trace: dict[str, JsonValue] | None = None

    @field_validator("selected_skill_source_refs", mode="before")
    @classmethod
    def _parse_skill_source_refs(cls, value: object) -> tuple[str, ...]:
        refs = _SKILL_SOURCE_REFS_ADAPTER.validate_python(value, strict=True)
        return tuple(refs)

    @field_validator("selected_skill_source_refs")
    @classmethod
    def _validate_skill_source_refs(cls, refs: tuple[str, ...]) -> tuple[str, ...]:
        if len(refs) > _MAX_SKILL_SOURCE_REFS:
            raise ValueError("too many selected_skill_source_refs")
        if len(set(refs)) != len(refs) or any(
            _SKILL_SOURCE_REF.fullmatch(ref) is None or ref.startswith("skill:skill:")
            for ref in refs
        ):
            raise ValueError("invalid selected_skill_source_refs")
        encoded = json.dumps(refs, ensure_ascii=True, separators=(",", ":")).encode()
        if len(encoded) > _MAX_SKILL_SELECTION_BYTES:
            raise ValueError("selected_skill_source_refs exceeds byte limit")
        return refs


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
    Union[
        ApproveDecision, EditDecision, RejectDecision, RespondDecision, SubmitDecision
    ],
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
