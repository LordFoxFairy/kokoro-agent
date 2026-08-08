"""Closed Agent submission subset of the pinned official AG-UI vocabulary."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Generic, Literal, Self, TypeAlias, TypeVar

from ag_ui.core import EventType
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    TypeAdapter,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel

AGUI_UPSTREAM_COMMIT = "54f13419055b4d0f442c71e1efab18b310982ce1"
AGUI_UPSTREAM_PYTHON_VERSION = "0.1.19"
MAX_OFFICIAL_EVENT_JSON_BYTES = 64 * 1024
MAX_TIMESTAMP = 253_402_300_799_999
MAX_UINT64_DECIMAL = "18446744073709551615"

AllowedOfficialEventType: TypeAlias = Literal[
    "RUN_STARTED",
    "RUN_FINISHED",
    "RUN_ERROR",
    "TEXT_MESSAGE_START",
    "TEXT_MESSAGE_CONTENT",
    "TEXT_MESSAGE_END",
    "ACTIVITY_SNAPSHOT",
]
AllowedActivityType: TypeAlias = Literal[
    "kokoro.safe-summary.v1",
    "kokoro.tool-preview.v1",
    "kokoro.hitl.v1",
    "kokoro.plan.v1",
    "kokoro.subagent.v1",
    "kokoro.notice.v1",
    "kokoro.error.v1",
]

ALLOWED_OFFICIAL_EVENT_TYPES: frozenset[str] = frozenset(
    event_type.value
    for event_type in (
        EventType.RUN_STARTED,
        EventType.RUN_FINISHED,
        EventType.RUN_ERROR,
        EventType.TEXT_MESSAGE_START,
        EventType.TEXT_MESSAGE_CONTENT,
        EventType.TEXT_MESSAGE_END,
        EventType.ACTIVITY_SNAPSHOT,
    )
)
ALLOWED_ACTIVITY_TYPES: frozenset[str] = frozenset(
    {
        "kokoro.safe-summary.v1",
        "kokoro.tool-preview.v1",
        "kokoro.hitl.v1",
        "kokoro.plan.v1",
        "kokoro.subagent.v1",
        "kokoro.notice.v1",
        "kokoro.error.v1",
    }
)

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
    return (
        delta.days * 86_400_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )


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
        if (
            len(value) == len(MAX_UINT64_DECIMAL)
            and value > MAX_UINT64_DECIMAL
        ):
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
    retry_class: Literal[
        "never", "after-delay", "after-user-action", "reconcile-receipt"
    ] | None = None


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
ClosedAguiEvent: TypeAlias = (
    ClosedRunStartedEvent
    | ClosedRunFinishedEvent
    | ClosedRunErrorEvent
    | ClosedTextStartEvent
    | ClosedTextContentEvent
    | ClosedTextEndEvent
    | ClosedActivityEvent
)

closed_agui_event_adapter: TypeAdapter[ClosedAguiEvent] = TypeAdapter(ClosedAguiEvent)


__all__ = [
    "AGUI_UPSTREAM_COMMIT",
    "AGUI_UPSTREAM_PYTHON_VERSION",
    "ALLOWED_ACTIVITY_TYPES",
    "ALLOWED_OFFICIAL_EVENT_TYPES",
    "AllowedOfficialEventType",
    "CLOSED_ACTIVITY_CLASSES",
    "ClosedActivityBase",
    "ClosedActivityEvent",
    "ClosedAguiEvent",
    "MAX_OFFICIAL_EVENT_JSON_BYTES",
    "MAX_TIMESTAMP",
    "closed_agui_event_adapter",
]
