"""Closed Agent candidate subset of the pinned official AG-UI vocabulary."""

from __future__ import annotations

from typing import Annotated, Literal, TypeAlias

from ag_ui.core import EventType
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    TypeAdapter,
    field_validator,
)
from pydantic.alias_generators import to_camel

AGUI_CANDIDATE_PROFILE_REVISION = "kokoro-agent-agui-candidate.v1"
AGUI_UPSTREAM_COMMIT = "54f13419055b4d0f442c71e1efab18b310982ce1"
AGUI_UPSTREAM_PYTHON_VERSION = "0.1.19"
MAX_OFFICIAL_EVENT_JSON_BYTES = 64 * 1024
MAX_TIMESTAMP = 8_640_000_000_000_000

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
    "kokoro.media.v1",
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
        "kokoro.media.v1",
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
_DateTime = Annotated[
    str,
    StringConstraints(
        min_length=20,
        max_length=35,
        pattern=(
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"(?:\.[0-9]{3})?(?:Z|[+-][0-9]{2}:[0-9]{2})$"
        ),
    ),
]
_Timestamp = Annotated[int, Field(ge=0, le=MAX_TIMESTAMP)]


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


class SafeSummaryContent(_StrictAliasModel):
    part_ref: _Id
    summary: _SafeText
    status: Literal["streaming", "complete", "partial", "failed", "canceled"]


class ToolPreviewContent(_StrictAliasModel):
    tool_call_ref: _Id
    label: _ShortText
    status: Literal[
        "pending", "running", "awaiting-user", "completed", "failed", "canceled"
    ]
    summary: _SafeText | None = None
    result_preview: _SafeText | None = None
    is_error: bool | None = None
    truncated: bool | None = None


class HitlContent(_StrictAliasModel):
    owner_ref: _Id
    expected_version: Annotated[int, Field(ge=1)]
    kind: Literal["approval", "interaction"]
    title: _ShortText
    description: _SafeText
    allowed_actions: Annotated[tuple[_Id, ...], Field(min_length=1, max_length=16)]
    status: Literal["pending", "accepted", "rejected", "expired", "canceled"]
    deadline: _DateTime | None = None
    receipt_ref: _Id | None = None

    @field_validator("allowed_actions")
    @classmethod
    def _unique_actions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("allowed actions must be unique")
        return value


class PlanStep(_StrictAliasModel):
    step_ref: _Id
    label: _ShortText
    status: Literal["pending", "in-progress", "completed", "failed", "canceled"]


class PlanContent(_StrictAliasModel):
    plan_ref: _Id
    summary: _SafeText
    status: Literal["proposed", "active", "completed", "failed", "canceled"]
    steps: Annotated[tuple[PlanStep, ...], Field(max_length=256)]


class SubagentContent(_StrictAliasModel):
    subagent_ref: _Id
    status: Literal["pending", "running", "completed", "failed", "canceled"]
    summary: _SafeText | None = None


class MediaContent(_StrictAliasModel):
    operation_ref: _Id
    state: Literal[
        "pending",
        "queued",
        "active",
        "finalizing",
        "completed",
        "partial",
        "failed",
        "canceled",
        "unknown",
    ]
    progress_bps: Annotated[int, Field(ge=0, le=10_000)]
    summary: _SafeText | None = None


class NoticeContent(_StrictAliasModel):
    notice_ref: _Id
    code: _Id
    message: _SafeText
    severity: Literal["info", "warning"]
    retry_class: Literal[
        "never", "after-delay", "after-user-action", "reconcile-receipt"
    ] | None = None


class ErrorContent(_StrictAliasModel):
    error_ref: _Id
    code: _Id
    message: _SafeText
    retry_class: Literal[
        "never", "after-delay", "after-user-action", "reconcile-receipt"
    ]
    support_correlation_ref: _Id | None = None


class ClosedActivityBase(_StrictAliasModel):
    timestamp: _Timestamp
    message_id: _Id
    replace: Literal[True]


class ClosedSafeSummaryActivity(ClosedActivityBase):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.safe-summary.v1"]
    content: SafeSummaryContent


class ClosedToolPreviewActivity(ClosedActivityBase):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.tool-preview.v1"]
    content: ToolPreviewContent


class ClosedHitlActivity(ClosedActivityBase):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.hitl.v1"]
    content: HitlContent


class ClosedPlanActivity(ClosedActivityBase):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.plan.v1"]
    content: PlanContent


class ClosedSubagentActivity(ClosedActivityBase):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.subagent.v1"]
    content: SubagentContent


class ClosedMediaActivity(ClosedActivityBase):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.media.v1"]
    content: MediaContent


class ClosedNoticeActivity(ClosedActivityBase):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.notice.v1"]
    content: NoticeContent


class ClosedErrorActivity(ClosedActivityBase):
    type: Literal["ACTIVITY_SNAPSHOT"]
    activity_type: Literal["kokoro.error.v1"]
    content: ErrorContent


ClosedActivityEvent: TypeAlias = (
    ClosedSafeSummaryActivity
    | ClosedToolPreviewActivity
    | ClosedHitlActivity
    | ClosedPlanActivity
    | ClosedSubagentActivity
    | ClosedMediaActivity
    | ClosedNoticeActivity
    | ClosedErrorActivity
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
    "AGUI_CANDIDATE_PROFILE_REVISION",
    "AGUI_UPSTREAM_COMMIT",
    "AGUI_UPSTREAM_PYTHON_VERSION",
    "ALLOWED_ACTIVITY_TYPES",
    "ALLOWED_OFFICIAL_EVENT_TYPES",
    "AllowedOfficialEventType",
    "ClosedActivityBase",
    "ClosedAguiEvent",
    "MAX_OFFICIAL_EVENT_JSON_BYTES",
    "MAX_TIMESTAMP",
    "closed_agui_event_adapter",
]
