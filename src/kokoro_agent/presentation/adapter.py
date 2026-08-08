"""Official AG-UI SDK adapter for Root R1 Presentation submissions."""

from __future__ import annotations

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

from kokoro_agent.presentation.profile import (
    ALLOWED_OFFICIAL_EVENT_TYPES,
    CLOSED_ACTIVITY_CLASSES,
    ClosedActivityEvent,
    ClosedAguiEvent,
    ClosedRunFinishedEvent,
    ClosedRunStartedEvent,
    ClosedTextContentEvent,
    ClosedTextEndEvent,
    ClosedTextStartEvent,
    closed_agui_event_adapter,
)
from kokoro_agent.presentation.submission import (
    PRESENTATION_SUBMISSION_CONTRACT_REVISION,
    PresentationSubmission,
    SubmissionSource,
    canonical_recorded_at,
    event_digest,
    submission_identity,
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


def _closed_official_event(event: BaseEvent) -> ClosedAguiEvent:
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
        closed = closed_agui_event_adapter.validate_json(upstream_json)
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


def _validate_source_scope(event: ClosedAguiEvent, source: SubmissionSource) -> None:
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


__all__ = [
    "SubmissionProtocolError",
    "build_submission",
]
