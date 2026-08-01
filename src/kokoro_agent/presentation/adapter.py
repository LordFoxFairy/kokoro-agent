"""Pure Agent fact to official AG-UI candidate mapping; no transport or stream state."""

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

from kokoro_agent.contract import (
    AgentEvent,
    RunCompleted,
    RunFailed,
    RunStarted,
)
from kokoro_agent.presentation.candidate import (
    AgentAguiCandidateRoute,
    AgentAguiCandidateSource,
    AgentAguiEventCandidate,
    candidate_identity,
    canonical_recorded_at,
    event_digest,
)
from kokoro_agent.presentation.profile import (
    AGUI_CANDIDATE_PROFILE_REVISION,
    ALLOWED_OFFICIAL_EVENT_TYPES,
    ClosedActivityBase,
    ClosedAguiEvent,
    ClosedRunFinishedEvent,
    ClosedRunStartedEvent,
    ClosedTextContentEvent,
    ClosedTextEndEvent,
    ClosedTextStartEvent,
    closed_agui_event_adapter,
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


class CandidateProtocolError(ValueError):
    """Stable fail-closed error raised at the official SDK trust boundary."""


def _closed_official_event(event: BaseEvent) -> ClosedAguiEvent:
    if (
        not isinstance(event, _ALLOWED_OFFICIAL_CLASSES)
        or event.type.value not in ALLOWED_OFFICIAL_EVENT_TYPES
    ):
        raise CandidateProtocolError("AGUI_EVENT_TYPE_FORBIDDEN")
    if event.raw_event is not None:
        raise CandidateProtocolError("AGUI_EVENT_SHAPE_FORBIDDEN")
    if isinstance(event, RunFinishedEvent):
        if not isinstance(event.outcome, RunFinishedSuccessOutcome):
            raise CandidateProtocolError("RUN_FINISHED_SUCCESS_REQUIRED")
        if event.result is not None:
            raise CandidateProtocolError("RUN_FINISHED_RESULT_FORBIDDEN")
    try:
        upstream_json = event.model_dump_json(by_alias=True, exclude_none=True)
        closed = closed_agui_event_adapter.validate_json(upstream_json)
    except (TypeError, ValueError, ValidationError) as error:
        raise CandidateProtocolError("AGUI_EVENT_SHAPE_FORBIDDEN") from error
    try:
        event_digest(closed)
    except ValueError as error:
        message = str(error)
        code = (
            "AGUI_EVENT_JSON_TOO_LARGE"
            if "exceeds byte limit" in message
            else "AGUI_EVENT_SHAPE_FORBIDDEN"
        )
        raise CandidateProtocolError(code) from error
    return closed


def _validate_source_scope(
    event: ClosedAguiEvent, source: AgentAguiCandidateSource
) -> None:
    route = source.route
    if canonical_recorded_at(event.timestamp) != source.recorded_at:
        raise CandidateProtocolError("AGUI_EVENT_TIMESTAMP_CONFLICT")
    if isinstance(event, ClosedRunStartedEvent | ClosedRunFinishedEvent):
        if (
            event.run_id != route.internal_run_ref
            or event.thread_id != route.internal_thread_ref
        ):
            raise CandidateProtocolError("AGUI_EVENT_SCOPE_CONFLICT")
        if route.internal_message_ref is not None:
            raise CandidateProtocolError("AGUI_EVENT_SEGMENT_CONFLICT")
        return
    if isinstance(
        event,
        (ClosedTextStartEvent, ClosedTextContentEvent, ClosedTextEndEvent),
    ) or isinstance(event, ClosedActivityBase):
        message_event: (
            ClosedTextStartEvent
            | ClosedTextContentEvent
            | ClosedTextEndEvent
            | ClosedActivityBase
        ) = event
        if route.internal_message_ref != message_event.message_id:
            raise CandidateProtocolError("AGUI_EVENT_SEGMENT_CONFLICT")
    elif route.internal_message_ref is not None:
        raise CandidateProtocolError("AGUI_EVENT_SEGMENT_CONFLICT")


def build_agui_candidate(
    event: BaseEvent, *, source: AgentAguiCandidateSource
) -> AgentAguiEventCandidate:
    """Construct with official models, close the shape, then seal identity/digest."""

    closed = _closed_official_event(event)
    _validate_source_scope(closed, source)
    digest = event_digest(closed)
    try:
        return AgentAguiEventCandidate(
            profile_revision=AGUI_CANDIDATE_PROFILE_REVISION,
            candidate_ref=candidate_identity(
                source=source,
                event_digest_value=digest,
            ),
            source=source,
            event_digest=digest,
            event=closed,
        )
    except ValidationError as error:
        raise CandidateProtocolError("AGUI_CANDIDATE_INVALID") from error


def _source(
    event: AgentEvent,
    *,
    thread_ref: str,
    source_event_ref: str,
) -> AgentAguiCandidateSource:
    return AgentAguiCandidateSource(
        source_event_ref=source_event_ref,
        source_ordinal=str(event.index),
        recorded_at=canonical_recorded_at(event.timestamp),
        route=AgentAguiCandidateRoute(
            internal_run_ref=event.run_id,
            internal_thread_ref=thread_ref,
        ),
    )


def _safe_text(value: str, maximum: int = 16_384) -> str:
    return value if len(value) <= maximum else f"{value[: maximum - 1]}…"


def map_agent_event_candidates(
    event: AgentEvent,
    *,
    thread_ref: str,
    source_event_ref: str,
) -> tuple[AgentAguiEventCandidate, ...]:
    """Map one source fact without inventing stream state or durable source identity.

    ``source_event_ref`` must come from durable Agent owner authority. Only the three statelessly
    complete run lifecycle mappings are enabled. Existing message/tool/subagent facts cannot create
    an admissible START-bound sequence without a future atomic segment-transition source batch, so
    they deliberately produce zero candidates instead of fabricating presentation state.
    """

    if isinstance(event, RunStarted):
        source = _source(
            event,
            thread_ref=thread_ref,
            source_event_ref=source_event_ref,
        )
        return (
            build_agui_candidate(
                RunStartedEvent(
                    thread_id=thread_ref,
                    run_id=event.run_id,
                    timestamp=event.timestamp,
                ),
                source=source,
            ),
        )
    if isinstance(event, RunCompleted):
        if event.payload.status != "completed":
            return ()
        source = _source(
            event,
            thread_ref=thread_ref,
            source_event_ref=source_event_ref,
        )
        return (
            build_agui_candidate(
                RunFinishedEvent(
                    thread_id=thread_ref,
                    run_id=event.run_id,
                    timestamp=event.timestamp,
                    outcome=RunFinishedSuccessOutcome(),
                ),
                source=source,
            ),
        )
    if isinstance(event, RunFailed):
        source = _source(
            event,
            thread_ref=thread_ref,
            source_event_ref=source_event_ref,
        )
        return (
            build_agui_candidate(
                RunErrorEvent(
                    message=_safe_text(event.payload.message),
                    code=event.payload.code,
                    timestamp=event.timestamp,
                ),
                source=source,
            ),
        )
    return ()


__all__ = [
    "CandidateProtocolError",
    "build_agui_candidate",
    "map_agent_event_candidates",
]
