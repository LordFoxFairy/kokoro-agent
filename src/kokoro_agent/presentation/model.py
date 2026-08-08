"""Strict Presentation Submission, planner-state, and delivery-record models."""

from __future__ import annotations

import hashlib
import json
import unicodedata
from datetime import UTC, datetime, timedelta
from typing import Annotated, Generic, Literal, Self, TypeAlias, TypeVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    TypeAdapter,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel

MAX_OFFICIAL_EVENT_JSON_BYTES = 64 * 1024
MAX_TIMESTAMP = 253_402_300_799_999
MAX_UINT64_DECIMAL = "18446744073709551615"

_Id = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]
_ShortText = Annotated[str, StringConstraints(min_length=1, max_length=1024)]
_SafeText = Annotated[str, StringConstraints(max_length=16_384)]
_CanonicalUtcMilliseconds = Annotated[
    str,
    StringConstraints(
        min_length=24,
        max_length=24,
        pattern=(
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"\.[0-9]{3}Z$"
        ),
    ),
]
_PositiveUint64 = Annotated[
    str,
    StringConstraints(min_length=1, max_length=20, pattern=r"^[1-9][0-9]{0,19}$"),
]
_Timestamp = Annotated[int, Field(ge=0, le=MAX_TIMESTAMP)]
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _canonical_milliseconds(value: str) -> str:
    try:
        moment = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError as error:
        raise ValueError("timestamp is not canonical UTC milliseconds") from error
    rendered = f"{moment:%Y-%m-%dT%H:%M:%S}.{moment.microsecond // 1000:03d}Z"
    if rendered != value:
        raise ValueError("timestamp is not canonical UTC milliseconds")
    return value


def _canonical_milliseconds_since_epoch(value: str) -> int:
    _canonical_milliseconds(value)
    moment = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    delta = moment - _EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


class _StrictAliasModel(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        alias_generator=to_camel,
        populate_by_name=True,
    )


class ClosedRunStartedEvent(_StrictAliasModel):
    type: Literal["RUN_STARTED"]
    timestamp: _Timestamp
    thread_id: _Id
    run_id: _Id


class _SuccessOutcome(_StrictAliasModel):
    type: Literal["success"]


class ClosedRunFinishedEvent(_StrictAliasModel):
    type: Literal["RUN_FINISHED"]
    timestamp: _Timestamp
    thread_id: _Id
    run_id: _Id
    outcome: _SuccessOutcome


class ClosedRunErrorEvent(_StrictAliasModel):
    type: Literal["RUN_ERROR"]
    timestamp: _Timestamp
    message: _SafeText
    code: _Id


class ClosedTextStartEvent(_StrictAliasModel):
    type: Literal["TEXT_MESSAGE_START"]
    timestamp: _Timestamp
    message_id: _Id
    role: Literal["assistant"]


class ClosedTextContentEvent(_StrictAliasModel):
    type: Literal["TEXT_MESSAGE_CONTENT"]
    timestamp: _Timestamp
    message_id: _Id
    delta: Annotated[str, StringConstraints(min_length=1, max_length=16_384)]


class ClosedTextEndEvent(_StrictAliasModel):
    type: Literal["TEXT_MESSAGE_END"]
    timestamp: _Timestamp
    message_id: _Id


class _OwnerContent(_StrictAliasModel):
    owner_version: _PositiveUint64
    updated_at: _CanonicalUtcMilliseconds

    @field_validator("owner_version")
    @classmethod
    def _bounded_owner_version(cls, value: str) -> str:
        if len(value) == len(MAX_UINT64_DECIMAL) and value > MAX_UINT64_DECIMAL:
            raise ValueError("ownerVersion exceeds uint64")
        return value

    @field_validator("updated_at")
    @classmethod
    def _canonical_updated_at(cls, value: str) -> str:
        return _canonical_milliseconds(value)


class SafeSummaryContent(_OwnerContent):
    part_ref: _Id
    summary: _SafeText
    status: Literal["streaming", "complete", "partial", "failed", "canceled"]


class ToolPreviewContent(_OwnerContent):
    tool_call_ref: _Id
    label: _ShortText
    status: Literal[
        "pending", "running", "awaiting-user", "completed", "failed", "canceled"
    ]
    summary: _SafeText | None = None
    result_preview: _SafeText | None = None
    is_error: bool | None = None
    truncated: bool | None = None


class HitlContent(_OwnerContent):
    owner_ref: _Id
    decision_group_ref: _Id
    required_owner_refs: Annotated[tuple[_Id, ...], Field(min_length=1, max_length=64)]
    control_ref: _Id
    kind: Literal["approval", "interaction"]
    title: _ShortText
    description: _SafeText
    allowed_actions: Annotated[
        tuple[Literal["approve", "reject", "edit", "respond"], ...],
        Field(min_length=1, max_length=4),
    ]
    status: Literal["pending"]
    deadline: _CanonicalUtcMilliseconds | None = None

    @field_validator("allowed_actions")
    @classmethod
    def _unique_actions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("allowed actions must be unique")
        return value

    @field_validator("required_owner_refs")
    @classmethod
    def _unique_required_owners(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("required owner refs must be unique")
        return value

    @model_validator(mode="after")
    def _owner_is_group_member(self) -> HitlContent:
        if self.owner_ref not in self.required_owner_refs:
            raise ValueError("ownerRef must belong to requiredOwnerRefs")
        return self


class PlanStep(_StrictAliasModel):
    step_ref: _Id
    label: _ShortText
    status: Literal["pending", "in-progress", "completed", "failed", "canceled"]


class PlanContent(_OwnerContent):
    plan_ref: _Id
    summary: _SafeText
    status: Literal["proposed", "active", "completed", "failed", "canceled"]
    steps: Annotated[tuple[PlanStep, ...], Field(max_length=256)]


class SubagentContent(_OwnerContent):
    subagent_ref: _Id
    status: Literal["pending", "running", "completed", "failed", "canceled"]
    summary: _SafeText | None = None


class NoticeContent(_OwnerContent):
    notice_ref: _Id
    code: _Id
    message: _SafeText
    severity: Literal["info", "warning"]
    retry_class: (
        Literal["never", "after-delay", "after-user-action", "reconcile-receipt"] | None
    ) = None


class ErrorContent(_OwnerContent):
    error_ref: _Id
    code: _Id
    message: _SafeText
    retry_class: Literal[
        "never", "after-delay", "after-user-action", "reconcile-receipt"
    ]
    support_correlation_ref: _Id | None = None


_OwnerContentT = TypeVar("_OwnerContentT", bound=_OwnerContent)


class ClosedActivityBase(_StrictAliasModel, Generic[_OwnerContentT]):
    timestamp: _Timestamp
    message_id: _Id
    content: _OwnerContentT
    replace: Literal[True]

    @model_validator(mode="after")
    def _updated_at_not_after_event(self) -> Self:
        content = self.content
        if _canonical_milliseconds_since_epoch(content.updated_at) > self.timestamp:
            raise ValueError("updatedAt must not be later than event timestamp")
        return self


class ClosedSafeSummaryActivity(ClosedActivityBase[SafeSummaryContent]):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.safe-summary.v1"]


class ClosedToolPreviewActivity(ClosedActivityBase[ToolPreviewContent]):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.tool-preview.v1"]


class ClosedHitlActivity(ClosedActivityBase[HitlContent]):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.hitl.v1"]


class ClosedPlanActivity(ClosedActivityBase[PlanContent]):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.plan.v1"]


class ClosedSubagentActivity(ClosedActivityBase[SubagentContent]):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.subagent.v1"]


class ClosedNoticeActivity(ClosedActivityBase[NoticeContent]):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.notice.v1"]


class ClosedErrorActivity(ClosedActivityBase[ErrorContent]):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.error.v1"]


ClosedActivityEvent: TypeAlias = (
    ClosedSafeSummaryActivity
    | ClosedToolPreviewActivity
    | ClosedHitlActivity
    | ClosedPlanActivity
    | ClosedSubagentActivity
    | ClosedNoticeActivity
    | ClosedErrorActivity
)
CLOSED_ACTIVITY_CLASSES = (
    ClosedSafeSummaryActivity,
    ClosedToolPreviewActivity,
    ClosedHitlActivity,
    ClosedPlanActivity,
    ClosedSubagentActivity,
    ClosedNoticeActivity,
    ClosedErrorActivity,
)
SubmissionEvent: TypeAlias = (
    ClosedRunStartedEvent
    | ClosedRunFinishedEvent
    | ClosedRunErrorEvent
    | ClosedTextStartEvent
    | ClosedTextContentEvent
    | ClosedTextEndEvent
    | ClosedActivityEvent
)

submission_event_adapter: TypeAdapter[SubmissionEvent] = TypeAdapter(SubmissionEvent)
PRESENTATION_SUBMISSION_CONTRACT_REVISION = "kokoro.presentation.submission.v1"
MAX_UINT64 = (1 << 64) - 1
MAX_SAFE_INTEGER = (1 << 53) - 1
_SUBMISSION_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_JSON_VALUE_ADAPTER: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)

_SubmissionId = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]
_AgentThreadRef = Annotated[
    str,
    StringConstraints(
        min_length=14,
        max_length=128,
        pattern=r"^agent\.thread:[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]
_Sha256Digest = Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]
_SubmissionRef = Annotated[
    str,
    StringConstraints(pattern=r"^presentation\.submission:sha256:[0-9a-f]{64}$"),
]
_Uint64Decimal = Annotated[str, StringConstraints(pattern=r"^(0|[1-9][0-9]{0,19})$")]
_SubmissionCanonicalUtcMilliseconds = Annotated[
    str,
    StringConstraints(
        pattern=(
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"\.[0-9]{3}Z$"
        )
    ),
]


class _SubmissionModel(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        alias_generator=to_camel,
        populate_by_name=True,
        revalidate_instances="always",
    )


class SubmissionRoute(_SubmissionModel):
    internal_run_ref: _SubmissionId
    internal_thread_ref: _AgentThreadRef
    internal_message_ref: _SubmissionId | None = None

    @model_validator(mode="after")
    def reject_explicit_null_message_ref(self) -> SubmissionRoute:
        if (
            self.internal_message_ref is None
            and "internal_message_ref" in self.model_fields_set
        ):
            raise ValueError("internalMessageRef must be absent rather than null")
        return self


class SubmissionSource(_SubmissionModel):
    """Caller-owned durable source coordinates; the adapter invents none of them."""

    source_event_ref: _SubmissionId
    event_ordinal: _Uint64Decimal
    recorded_at: _SubmissionCanonicalUtcMilliseconds
    route: SubmissionRoute

    @model_validator(mode="after")
    def validate_source(self) -> SubmissionSource:
        if int(self.event_ordinal) > MAX_UINT64:
            raise ValueError("event ordinal exceeds uint64")
        recorded_at_milliseconds(self.recorded_at)
        return self


def canonical_recorded_at(timestamp_ms: int) -> str:
    if timestamp_ms < 0 or timestamp_ms > MAX_SAFE_INTEGER:
        raise ValueError("event timestamp is not a non-negative safe integer")
    try:
        moment = _SUBMISSION_EPOCH + timedelta(milliseconds=timestamp_ms)
    except OverflowError as error:
        raise ValueError("event timestamp is outside canonical UTC range") from error
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{moment.microsecond // 1000:03d}Z"


def recorded_at_milliseconds(recorded_at: str) -> int:
    try:
        moment = datetime.strptime(recorded_at, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=UTC
        )
    except ValueError as error:
        raise ValueError("recordedAt is not canonical UTC milliseconds") from error
    delta = moment - _SUBMISSION_EPOCH
    milliseconds = (
        delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000
    )
    if canonical_recorded_at(milliseconds) != recorded_at:
        raise ValueError("recordedAt is not canonical UTC milliseconds")
    return milliseconds


def _jcs_string(value: str) -> str:
    if unicodedata.normalize("NFC", value) != value:
        raise ValueError("JCS strings and object keys must be NFC")
    try:
        value.encode("utf-8")
        value.encode("utf-16-be")
    except UnicodeEncodeError as error:
        raise ValueError("JCS strings must contain Unicode scalar values") from error
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _jcs_text(value: JsonValue) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        if abs(value) > MAX_SAFE_INTEGER:
            raise ValueError("JCS integer exceeds interoperable safe range")
        return str(value)
    if isinstance(value, float):
        raise ValueError("JCS submission subset forbids floating-point values")
    if isinstance(value, str):
        return _jcs_string(value)
    if isinstance(value, list):
        return "[" + ",".join(_jcs_text(item) for item in value) + "]"
    keys = sorted(value, key=lambda key: key.encode("utf-16-be"))
    return (
        "{"
        + ",".join(f"{_jcs_string(key)}:{_jcs_text(value[key])}" for key in keys)
        + "}"
    )


def canonical_json_bytes(value: JsonValue) -> bytes:
    """Encode the closed Presentation JSON subset with RFC 8785 key ordering."""
    validated = _JSON_VALUE_ADAPTER.validate_python(value)
    return _jcs_text(validated).encode()


def event_jcs_bytes(event: SubmissionEvent) -> bytes:
    dumped = event.model_dump(mode="json", by_alias=True, exclude_none=True)
    encoded = canonical_json_bytes(dumped)
    if len(encoded) > MAX_OFFICIAL_EVENT_JSON_BYTES:
        raise ValueError("official event JCS exceeds byte limit")
    return encoded


def event_digest(event: SubmissionEvent) -> str:
    return f"sha256:{hashlib.sha256(event_jcs_bytes(event)).hexdigest()}"


def submission_identity(
    *,
    contract_revision: str,
    source: SubmissionSource,
    event_digest_value: str,
) -> str:
    route = source.route
    material = "\0".join(
        (
            contract_revision,
            route.internal_run_ref,
            route.internal_thread_ref,
            route.internal_message_ref or "",
            source.source_event_ref,
            source.event_ordinal,
            source.recorded_at,
            event_digest_value,
        )
    ).encode()
    return f"presentation.submission:sha256:{hashlib.sha256(material).hexdigest()}"


_MessageScopedEvent: TypeAlias = (
    ClosedTextStartEvent
    | ClosedTextContentEvent
    | ClosedTextEndEvent
    | ClosedActivityEvent
)


class PresentationSubmission(_SubmissionModel):
    """Canonical Agent-to-Session input; Session remains admission owner."""

    contract_revision: Annotated[
        str, Field(pattern=r"^kokoro\.presentation\.submission\.v1$")
    ]
    submission_ref: _SubmissionRef
    source: SubmissionSource
    event_digest: _Sha256Digest
    event: SubmissionEvent

    @model_validator(mode="after")
    def validate_material(self) -> PresentationSubmission:
        if self.contract_revision != PRESENTATION_SUBMISSION_CONTRACT_REVISION:
            raise ValueError("PRESENTATION_SUBMISSION_REVISION_INVALID")
        if recorded_at_milliseconds(self.source.recorded_at) != self.event.timestamp:
            raise ValueError("PRESENTATION_SUBMISSION_TIMESTAMP_INVALID")
        route = self.source.route
        if isinstance(self.event, ClosedRunStartedEvent | ClosedRunFinishedEvent):
            if (
                self.event.run_id != route.internal_run_ref
                or self.event.thread_id != route.internal_thread_ref
            ):
                raise ValueError("PRESENTATION_SUBMISSION_ROUTE_INVALID")
            if route.internal_message_ref is not None:
                raise ValueError("PRESENTATION_SUBMISSION_SEGMENT_INVALID")
        elif isinstance(
            self.event,
            (ClosedTextStartEvent, ClosedTextContentEvent, ClosedTextEndEvent),
        ) or isinstance(self.event, CLOSED_ACTIVITY_CLASSES):
            message_event: _MessageScopedEvent = self.event
            if message_event.message_id != route.internal_message_ref:
                raise ValueError("PRESENTATION_SUBMISSION_SEGMENT_INVALID")
        elif route.internal_message_ref is not None:
            raise ValueError("PRESENTATION_SUBMISSION_SEGMENT_INVALID")
        if event_digest(self.event) != self.event_digest:
            raise ValueError("PRESENTATION_SUBMISSION_EVENT_DIGEST_INVALID")
        expected = submission_identity(
            contract_revision=self.contract_revision,
            source=self.source,
            event_digest_value=self.event_digest,
        )
        if self.submission_ref != expected:
            raise ValueError("PRESENTATION_SUBMISSION_REF_INVALID")
        return self

    def envelope_bytes(self) -> bytes:
        return canonical_json_bytes(
            self.model_dump(mode="json", by_alias=True, exclude_none=True)
        )


HITL_OWNER_REF_PREFIX = "agent.hitl-owner:sha256:"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class PresentationMessageState(_FrozenModel):
    internal_message_ref: str = Field(min_length=1, max_length=128)
    source_segment_ref: str = Field(min_length=1, max_length=256)
    state: Literal["open", "closed"]
    opened_ordinal: int = Field(ge=0)
    text_seen: bool = False


class PresentationOwnerState(_FrozenModel):
    owner_key: str = Field(pattern=r"^agent\.presentation-owner:sha256:[0-9a-f]{64}$")
    activity_type: str = Field(min_length=1, max_length=128)
    message_ref: str = Field(min_length=1, max_length=128)
    identity_fingerprint: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    semantic_fingerprint: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    owner_version: str = Field(pattern=r"^[1-9][0-9]{0,19}$")
    updated_at: str = Field(
        pattern=(
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"\.[0-9]{3}Z$"
        )
    )
    terminal_state: str | None = Field(default=None, min_length=1, max_length=64)

    @model_validator(mode="after")
    def validate_version(self) -> PresentationOwnerState:
        if len(self.owner_version) > len(MAX_UINT64_DECIMAL) or (
            len(self.owner_version) == len(MAX_UINT64_DECIMAL)
            and self.owner_version > MAX_UINT64_DECIMAL
        ):
            raise ValueError("ownerVersion exceeds uint64")
        recorded_at_milliseconds(self.updated_at)
        return self


class PresentationDecisionGroupState(_FrozenModel):
    group_key: str = Field(pattern=r"^agent\.decision-group-key:sha256:[0-9a-f]{64}$")
    decision_group_ref: str = Field(min_length=1, max_length=128)
    control_ref: str = Field(min_length=1, max_length=128)
    required_owner_refs: tuple[str, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def validate_group(self) -> PresentationDecisionGroupState:
        if len(self.required_owner_refs) != len(set(self.required_owner_refs)):
            raise ValueError("required owner refs must be unique")
        return self


class PresentationState(_FrozenModel):
    internal_run_ref: str | None = None
    internal_thread_ref: str | None = None
    run_state: Literal["new", "running", "finished", "failed"] = "new"
    next_ordinal: int = Field(default=0, ge=0)
    messages: tuple[PresentationMessageState, ...] = ()
    owners: tuple[PresentationOwnerState, ...] = ()
    decision_groups: tuple[PresentationDecisionGroupState, ...] = ()

    @model_validator(mode="after")
    def validate_durable_identities(self) -> PresentationState:
        if (
            self.messages or self.owners or self.decision_groups
        ) and self.internal_run_ref is None:
            raise ValueError("PRESENTATION_STATE_RUN_REF_REQUIRED")

        message_refs = tuple(message.internal_message_ref for message in self.messages)
        if len(message_refs) != len(set(message_refs)):
            raise ValueError("PRESENTATION_STATE_DUPLICATE_INTERNAL_MESSAGE_REF")
        segment_refs = tuple(message.source_segment_ref for message in self.messages)
        if len(segment_refs) != len(set(segment_refs)):
            raise ValueError("PRESENTATION_STATE_DUPLICATE_SOURCE_SEGMENT_REF")
        if self.internal_run_ref is not None and any(
            message.internal_message_ref
            != derive_message_ref(self.internal_run_ref, message.source_segment_ref)
            for message in self.messages
        ):
            raise ValueError("PRESENTATION_STATE_MESSAGE_PLACEMENT_INVALID")

        owner_keys = tuple(owner.owner_key for owner in self.owners)
        if len(owner_keys) != len(set(owner_keys)):
            raise ValueError("PRESENTATION_STATE_DUPLICATE_OWNER_KEY")
        ownermessage_refs = tuple(owner.message_ref for owner in self.owners)
        if len(ownermessage_refs) != len(set(ownermessage_refs)):
            raise ValueError("PRESENTATION_STATE_DUPLICATE_OWNER_MESSAGE_REF")
        if set(message_refs).intersection(ownermessage_refs):
            raise ValueError("PRESENTATION_STATE_MESSAGE_REF_CONFLICT")
        if self.internal_run_ref is not None and any(
            owner.message_ref
            != activity_message_ref(
                self.internal_run_ref, owner.activity_type, owner.owner_key
            )
            for owner in self.owners
        ):
            raise ValueError("PRESENTATION_STATE_OWNER_PLACEMENT_INVALID")

        group_keys = tuple(group.group_key for group in self.decision_groups)
        if len(group_keys) != len(set(group_keys)):
            raise ValueError("PRESENTATION_STATE_DUPLICATE_DECISION_GROUP_KEY")
        decision_group_refs = tuple(
            group.decision_group_ref for group in self.decision_groups
        )
        control_refs = tuple(group.control_ref for group in self.decision_groups)
        if len(decision_group_refs) != len(set(decision_group_refs)) or len(
            control_refs
        ) != len(set(control_refs)):
            raise ValueError("PRESENTATION_STATE_DECISION_GROUP_REFERENCE_CONFLICT")
        required_owner_refs = tuple(
            owner_ref
            for group in self.decision_groups
            for owner_ref in group.required_owner_refs
        )
        if any(
            not owner_ref.startswith(HITL_OWNER_REF_PREFIX)
            or len(owner_ref) != len(HITL_OWNER_REF_PREFIX) + 64
            or any(
                character not in "0123456789abcdef"
                for character in owner_ref[len(HITL_OWNER_REF_PREFIX) :]
            )
            for owner_ref in required_owner_refs
        ):
            raise ValueError("PRESENTATION_STATE_DECISION_OWNER_REF_INVALID")
        if len(required_owner_refs) != len(set(required_owner_refs)):
            raise ValueError("PRESENTATION_STATE_DECISION_OWNER_REF_CONFLICT")
        if any(
            group.decision_group_ref != private_ref("decision-group", group.group_key)
            or group.control_ref != private_ref("control-proposal", group.group_key)
            for group in self.decision_groups
        ):
            raise ValueError("PRESENTATION_STATE_DECISION_GROUP_PLACEMENT_INVALID")
        valid_hitl_owner_identities = {
            fingerprint(
                "kokoro-agent-presentation-owner-identity-v1",
                {
                    "activityType": "kokoro.hitl.v1",
                    "ownerRef": owner_ref,
                    "decisionGroupRef": group.decision_group_ref,
                    "requiredOwnerRefs": group.required_owner_refs,
                    "controlRef": group.control_ref,
                },
            )
            for group in self.decision_groups
            for owner_ref in group.required_owner_refs
        }
        hitl_owner_identities = tuple(
            owner.identity_fingerprint
            for owner in self.owners
            if owner.activity_type == "kokoro.hitl.v1"
        )
        if len(hitl_owner_identities) != len(set(hitl_owner_identities)):
            raise ValueError("PRESENTATION_STATE_DUPLICATE_HITL_OWNER_MEMBERSHIP")
        if any(
            identity not in valid_hitl_owner_identities
            for identity in hitl_owner_identities
        ):
            raise ValueError("PRESENTATION_STATE_HITL_OWNER_MEMBERSHIP_INVALID")
        return self


class SubmissionBatch(_FrozenModel):
    source_event_ref: str = Field(min_length=1, max_length=128)
    source_payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    submissions: tuple[PresentationSubmission, ...]
    next_state: PresentationState


class DeliveryRecord(_FrozenModel):
    record_ref: str = Field(pattern=r"^presentation\.record:sha256:[0-9a-f]{64}$")
    run_id: str = Field(min_length=1, max_length=128)
    delivery_seq: int = Field(gt=0, le=(1 << 64) - 1)
    envelope_bytes: bytes = Field(min_length=1, max_length=128 * 1024)
    envelope_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    submission_ref: str = Field(
        pattern=r"^presentation\.submission:sha256:[0-9a-f]{64}$"
    )
    submission_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    recorded_at_ms: int = Field(ge=0)
    producer_instance_ref: str = Field(min_length=1, max_length=256)
    producer_generation: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_envelope(self) -> DeliveryRecord:
        expected_digest = f"sha256:{hashlib.sha256(self.envelope_bytes).hexdigest()}"
        if expected_digest != self.envelope_digest:
            raise ValueError("presentation envelope digest mismatch")
        submission = PresentationSubmission.model_validate_json(self.envelope_bytes)
        if submission.envelope_bytes() != self.envelope_bytes:
            raise ValueError("presentation envelope is not canonical")
        if self.run_id != submission.source.route.internal_run_ref:
            raise ValueError("presentation delivery run scope mismatch")
        if (
            submission.submission_ref != self.submission_ref
            or self.submission_digest != self.envelope_digest
        ):
            raise ValueError("presentation submission identity mismatch")
        if self.recorded_at_ms != submission.event.timestamp:
            raise ValueError("presentation recorded time mismatch")
        if self.delivery_seq != int(submission.source.event_ordinal) + 1:
            raise ValueError("delivery sequence does not follow event ordinal")
        expected = _record_ref(self.run_id, self.delivery_seq, self.envelope_digest)
        if self.record_ref != expected:
            raise ValueError("presentation record identity mismatch")
        return self

    @classmethod
    def from_submission(
        cls,
        *,
        run_id: str,
        delivery_seq: int,
        submission: PresentationSubmission,
        producer_instance_ref: str,
        producer_generation: int,
    ) -> DeliveryRecord:
        envelope = submission.envelope_bytes()
        digest = f"sha256:{hashlib.sha256(envelope).hexdigest()}"
        return cls(
            record_ref=_record_ref(run_id, delivery_seq, digest),
            run_id=run_id,
            delivery_seq=delivery_seq,
            envelope_bytes=envelope,
            envelope_digest=digest,
            submission_ref=submission.submission_ref,
            submission_digest=digest,
            recorded_at_ms=submission.event.timestamp,
            producer_instance_ref=producer_instance_ref,
            producer_generation=producer_generation,
        )


def agent_thread_ref(namespace: str, thread_id: str) -> str:
    if not namespace or not thread_id:
        raise ValueError("PRESENTATION_THREAD_SCOPE_INVALID")
    material = f"kokoro-agent-thread-v1\0{namespace}\0{thread_id}".encode()
    return f"agent.thread:{hashlib.sha256(material).hexdigest()}"


def derive_message_ref(run_id: str, segment_id: str) -> str:
    material = f"kokoro-agent-message-v1\0{run_id}\0{segment_id}".encode()
    return f"agent.message:{hashlib.sha256(material).hexdigest()}"


def activity_message_ref(run_id: str, activity_type: str, owner_ref: str) -> str:
    material = (
        f"kokoro-agent-activity-v1\0{run_id}\0{activity_type}\0{owner_ref}".encode()
    )
    return f"agent.activity:{hashlib.sha256(material).hexdigest()}"


def private_ref(domain: str, *parts: str) -> str:
    digest = hashlib.sha256()
    digest.update(f"kokoro-agent-{domain}-v1\0".encode())
    for part in parts:
        encoded = part.encode()
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return f"agent.{domain}:sha256:{digest.hexdigest()}"


def fingerprint(domain: str, value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    digest = hashlib.sha256(domain.encode() + b"\0" + encoded).hexdigest()
    return f"sha256:{digest}"


def _record_ref(run_id: str, sequence: int, digest: str) -> str:
    material = f"kokoro-presentation-record-v1\0{run_id}\0{sequence}\0{digest}".encode()
    return f"presentation.record:sha256:{hashlib.sha256(material).hexdigest()}"


__all__ = [
    "DeliveryRecord",
    "PRESENTATION_SUBMISSION_CONTRACT_REVISION",
    "PresentationDecisionGroupState",
    "PresentationMessageState",
    "PresentationOwnerState",
    "PresentationState",
    "PresentationSubmission",
    "SubmissionBatch",
    "SubmissionRoute",
    "SubmissionSource",
    "agent_thread_ref",
    "canonical_json_bytes",
    "canonical_recorded_at",
    "event_digest",
    "event_jcs_bytes",
    "recorded_at_milliseconds",
]
