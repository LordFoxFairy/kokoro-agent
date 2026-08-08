"""Official AG-UI SDK adapter for Root R1 Presentation submissions."""

from __future__ import annotations

from typing import Literal, TypeAlias

from ag_ui.core import (
    ActivitySnapshotEvent,
    BaseEvent,
    RunErrorEvent,
    RunFinishedEvent,
    RunFinishedSuccessOutcome,
    RunStartedEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    TextMessageStartEvent,
)
from pydantic import ValidationError

from kokoro_agent.presentation.model import (
    CLOSED_ACTIVITY_CLASSES,
    ClosedActivityEvent,
    ClosedRunFinishedEvent,
    ClosedRunStartedEvent,
    ClosedTextContentEvent,
    ClosedTextEndEvent,
    ClosedTextStartEvent,
    PRESENTATION_SUBMISSION_CONTRACT_REVISION,
    PresentationSubmission,
    SubmissionEvent,
    SubmissionSource,
    canonical_recorded_at,
    event_digest,
    submission_event_adapter,
    submission_identity,
)

AGUI_UPSTREAM_COMMIT = "54f13419055b4d0f442c71e1efab18b310982ce1"
AGUI_UPSTREAM_PYTHON_VERSION = "0.1.19"
ALLOWED_OFFICIAL_EVENT_TYPES = frozenset(
    {
        "RUN_STARTED",
        "RUN_FINISHED",
        "RUN_ERROR",
        "TEXT_MESSAGE_START",
        "TEXT_MESSAGE_CONTENT",
        "TEXT_MESSAGE_END",
        "ACTIVITY_SNAPSHOT",
    }
)

_ALLOWED_OFFICIAL_CLASSES = (
    RunStartedEvent,
    RunFinishedEvent,
    RunErrorEvent,
    TextMessageStartEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    ActivitySnapshotEvent,
)
OfficialEvent: TypeAlias = (
    RunStartedEvent
    | RunFinishedEvent
    | RunErrorEvent
    | TextMessageStartEvent
    | TextMessageContentEvent
    | TextMessageEndEvent
    | ActivitySnapshotEvent
)


class SubmissionProtocolError(ValueError):
    """Stable fail-closed error raised at the official SDK trust boundary."""


def _closed_submission_source(source: SubmissionSource) -> SubmissionSource:
    try:
        serialized = source.model_dump(mode="json", by_alias=True, exclude_none=True)
        return SubmissionSource.model_validate(serialized)
    except (AttributeError, TypeError, ValueError, ValidationError) as error:
        raise SubmissionProtocolError(
            "PRESENTATION_SUBMISSION_SOURCE_INVALID"
        ) from error


def _closed_official_event(event: BaseEvent) -> SubmissionEvent:
    if (
        not isinstance(event, _ALLOWED_OFFICIAL_CLASSES)
        or event.type.value not in ALLOWED_OFFICIAL_EVENT_TYPES
    ):
        raise SubmissionProtocolError("AGUI_EVENT_TYPE_FORBIDDEN")
    if event.raw_event is not None:
        raise SubmissionProtocolError("AGUI_EVENT_SHAPE_FORBIDDEN")
    if isinstance(event, RunStartedEvent) and event.parent_run_id is not None:
        raise SubmissionProtocolError("RUN_STARTED_PARENT_FORBIDDEN")
    if isinstance(event, RunFinishedEvent):
        if not isinstance(event.outcome, RunFinishedSuccessOutcome):
            raise SubmissionProtocolError("RUN_FINISHED_SUCCESS_REQUIRED")
        if event.result is not None:
            raise SubmissionProtocolError("RUN_FINISHED_RESULT_FORBIDDEN")
    try:
        upstream_json = event.model_dump_json(by_alias=True, exclude_none=True)
        closed = submission_event_adapter.validate_json(upstream_json)
    except (TypeError, ValueError, ValidationError) as error:
        raise SubmissionProtocolError("AGUI_EVENT_SHAPE_FORBIDDEN") from error
    try:
        event_digest(closed)
    except ValueError as error:
        message = str(error)
        code = (
            "AGUI_EVENT_JSON_TOO_LARGE"
            if "exceeds byte limit" in message
            else "AGUI_EVENT_SHAPE_FORBIDDEN"
        )
        raise SubmissionProtocolError(code) from error
    return closed


def _validate_source_scope(event: SubmissionEvent, source: SubmissionSource) -> None:
    route = source.route
    if canonical_recorded_at(event.timestamp) != source.recorded_at:
        raise SubmissionProtocolError("AGUI_EVENT_TIMESTAMP_CONFLICT")
    if isinstance(event, ClosedRunStartedEvent | ClosedRunFinishedEvent):
        if (
            event.run_id != route.internal_run_ref
            or event.thread_id != route.internal_thread_ref
        ):
            raise SubmissionProtocolError("AGUI_EVENT_SCOPE_CONFLICT")
        if route.internal_message_ref is not None:
            raise SubmissionProtocolError("AGUI_EVENT_SEGMENT_CONFLICT")
        return
    if isinstance(
        event,
        (ClosedTextStartEvent, ClosedTextContentEvent, ClosedTextEndEvent),
    ) or isinstance(event, CLOSED_ACTIVITY_CLASSES):
        message_event: (
            ClosedTextStartEvent
            | ClosedTextContentEvent
            | ClosedTextEndEvent
            | ClosedActivityEvent
        ) = event
        if route.internal_message_ref != message_event.message_id:
            raise SubmissionProtocolError("AGUI_EVENT_SEGMENT_CONFLICT")
    elif route.internal_message_ref is not None:
        raise SubmissionProtocolError("AGUI_EVENT_SEGMENT_CONFLICT")


def build_submission(
    event: BaseEvent, *, source: SubmissionSource
) -> PresentationSubmission:
    """Close an official SDK model and seal the canonical Root R1 submission."""

    source = _closed_submission_source(source)
    closed = _closed_official_event(event)
    _validate_source_scope(closed, source)
    digest = event_digest(closed)
    try:
        return PresentationSubmission(
            contract_revision=PRESENTATION_SUBMISSION_CONTRACT_REVISION,
            submission_ref=submission_identity(
                contract_revision=PRESENTATION_SUBMISSION_CONTRACT_REVISION,
                source=source,
                event_digest_value=digest,
            ),
            source=source,
            event_digest=digest,
            event=closed,
        )
    except ValidationError as error:
        raise SubmissionProtocolError("PRESENTATION_SUBMISSION_INVALID") from error


def make_run_started(*, thread_id: str, run_id: str, timestamp: int) -> OfficialEvent:
    return RunStartedEvent(thread_id=thread_id, run_id=run_id, timestamp=timestamp)


def make_run_finished(
    *, thread_id: str, run_id: str, timestamp: int, outcome: object | None = None
) -> OfficialEvent:
    del outcome
    return RunFinishedEvent(
        thread_id=thread_id,
        run_id=run_id,
        timestamp=timestamp,
        outcome=RunFinishedSuccessOutcome(),
    )


def make_run_error(*, message: str, code: str, timestamp: int) -> OfficialEvent:
    return RunErrorEvent(message=message, code=code, timestamp=timestamp)


def make_text_start(
    *, message_id: str, role: Literal["assistant"], timestamp: int
) -> OfficialEvent:
    return TextMessageStartEvent(
        message_id=message_id,
        role=role,
        timestamp=timestamp,
    )


def make_text_content(*, message_id: str, delta: str, timestamp: int) -> OfficialEvent:
    return TextMessageContentEvent(
        message_id=message_id,
        delta=delta,
        timestamp=timestamp,
    )


def make_text_end(*, message_id: str, timestamp: int) -> OfficialEvent:
    return TextMessageEndEvent(message_id=message_id, timestamp=timestamp)


def make_activity_snapshot(
    *, message_id: str, timestamp: int, activity_type: str, content: dict[str, object]
) -> OfficialEvent:
    return ActivitySnapshotEvent(
        message_id=message_id,
        activity_type=activity_type,
        content=content,
        replace=True,
        timestamp=timestamp,
    )


__all__ = [
    "AGUI_UPSTREAM_COMMIT",
    "AGUI_UPSTREAM_PYTHON_VERSION",
    "OfficialEvent",
    "SubmissionProtocolError",
    "build_submission",
    "make_activity_snapshot",
    "make_run_error",
    "make_run_finished",
    "make_run_started",
    "make_text_content",
    "make_text_end",
    "make_text_start",
]
