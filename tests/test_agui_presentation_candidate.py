from __future__ import annotations

import copy
import hashlib
from importlib.metadata import version

import pytest
from ag_ui.core import (
    ActivitySnapshotEvent,
    CustomEvent,
    EventType,
    RawEvent,
    RunFinishedEvent,
    RunFinishedSuccessOutcome,
    RunStartedEvent,
    TextMessageContentEvent,
)
from pydantic import JsonValue, ValidationError

from kokoro_agent.contract import (
    MessageDelta,
    MessageDeltaPayload,
    RunCompleted,
    RunCompletedPayload,
    RunStarted,
    RunStartedPayload,
    ThinkingDelta,
    ThinkingDeltaPayload,
    ToolInvoked,
    ToolInvokedPayload,
)
from kokoro_agent.presentation import (
    AGUI_CANDIDATE_PROFILE_REVISION,
    AGUI_UPSTREAM_COMMIT,
    AgentAguiCandidateRoute,
    AgentAguiCandidateSource,
    AgentAguiEventCandidate,
    CandidateProtocolError,
    build_agui_candidate,
    map_agent_event_candidates,
)
from kokoro_agent.presentation.candidate import event_jcs_bytes


TIMESTAMP = 0
SOURCE = AgentAguiCandidateSource(
    source_event_ref="source.event.1",
    source_ordinal="0",
    recorded_at="1970-01-01T00:00:00.000Z",
    route=AgentAguiCandidateRoute(
        internal_run_ref="run.1",
        internal_thread_ref="thread.1",
    ),
)

_ALLOWED_ACTIVITY_CASES: list[tuple[str, dict[str, JsonValue]]] = [
    (
        "kokoro.safe-summary.v1",
        {"partRef": "part.1", "summary": "safe", "status": "complete"},
    ),
    (
        "kokoro.tool-preview.v1",
        {"toolCallRef": "tool.1", "label": "search", "status": "running"},
    ),
    (
        "kokoro.hitl.v1",
        {
            "ownerRef": "owner.1",
            "expectedVersion": 1,
            "kind": "approval",
            "title": "Approve",
            "description": "Review the action",
            "allowedActions": ["approve", "reject"],
            "status": "pending",
        },
    ),
    (
        "kokoro.plan.v1",
        {
            "planRef": "plan.1",
            "summary": "Plan",
            "status": "proposed",
            "steps": [{"stepRef": "step.1", "label": "First", "status": "pending"}],
        },
    ),
    ("kokoro.subagent.v1", {"subagentRef": "subagent.1", "status": "running"}),
    (
        "kokoro.media.v1",
        {"operationRef": "media.1", "state": "active", "progressBps": 5000},
    ),
    (
        "kokoro.notice.v1",
        {
            "noticeRef": "notice.1",
            "code": "working",
            "message": "正在处理",
            "severity": "info",
        },
    ),
    (
        "kokoro.error.v1",
        {
            "errorRef": "error.1",
            "code": "tool.failed",
            "message": "Failed safely",
            "retryClass": "never",
        },
    ),
]


def test_profile_pins_official_python_sdk_commit() -> None:
    assert AGUI_CANDIDATE_PROFILE_REVISION == "kokoro-agent-agui-candidate.v1"
    assert AGUI_UPSTREAM_COMMIT == "54f13419055b4d0f442c71e1efab18b310982ce1"
    assert version("ag-ui-protocol") == "0.1.19"
    assert EventType.RUN_STARTED.value == "RUN_STARTED"


def test_official_aliases_become_the_single_typed_event_fact() -> None:
    candidate = build_agui_candidate(
        RunStartedEvent(
            thread_id="thread.1",
            run_id="run.1",
            timestamp=TIMESTAMP,
        ),
        source=SOURCE,
    )
    dumped = candidate.model_dump(mode="json", by_alias=True, exclude_none=True)

    assert dumped["event"] == {
        "runId": "run.1",
        "threadId": "thread.1",
        "timestamp": 0,
        "type": "RUN_STARTED",
    }
    assert "internalMessageRef" not in dumped["source"]["route"]
    official = RunStartedEvent.model_validate(dumped["event"])
    assert isinstance(official, RunStartedEvent)
    assert official.run_id == "run.1"


def test_run_started_parent_is_forbidden_until_session_derives_lineage() -> None:
    with pytest.raises(CandidateProtocolError, match="RUN_STARTED_PARENT_FORBIDDEN"):
        build_agui_candidate(
            RunStartedEvent(
                thread_id="thread.1",
                run_id="run.1",
                parent_run_id="run.parent",
                timestamp=TIMESTAMP,
            ),
            source=SOURCE,
        )


def test_run_finished_requires_explicit_success_and_forbids_result() -> None:
    successful = build_agui_candidate(
        RunFinishedEvent(
            thread_id="thread.1",
            run_id="run.1",
            timestamp=TIMESTAMP,
            outcome=RunFinishedSuccessOutcome(),
        ),
        source=SOURCE,
    )
    dumped = successful.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert dumped["event"]["outcome"] == {"type": "success"}

    with pytest.raises(CandidateProtocolError, match="RUN_FINISHED_SUCCESS_REQUIRED"):
        build_agui_candidate(
            RunFinishedEvent(
                thread_id="thread.1",
                run_id="run.1",
                timestamp=TIMESTAMP,
            ),
            source=SOURCE,
        )

    with pytest.raises(CandidateProtocolError, match="RUN_FINISHED_RESULT_FORBIDDEN"):
        build_agui_candidate(
            RunFinishedEvent(
                thread_id="thread.1",
                run_id="run.1",
                timestamp=TIMESTAMP,
                outcome=RunFinishedSuccessOutcome(),
                result={"secret": "not-a-presentation-owner"},
            ),
            source=SOURCE,
        )


def test_forbidden_official_families_and_extra_fields_fail_closed() -> None:
    with pytest.raises(CandidateProtocolError, match="AGUI_EVENT_TYPE_FORBIDDEN"):
        build_agui_candidate(
            RawEvent(event={"payload": "raw"}, timestamp=TIMESTAMP),
            source=SOURCE,
        )
    with pytest.raises(CandidateProtocolError, match="AGUI_EVENT_TYPE_FORBIDDEN"):
        build_agui_candidate(
            CustomEvent(name="unsafe", value={}, timestamp=TIMESTAMP),
            source=SOURCE,
        )

    upstream_allows_extra = RunStartedEvent.model_validate(
        {
            "threadId": "thread.1",
            "runId": "run.1",
            "timestamp": TIMESTAMP,
            "userId": "user.must-not-cross",
        }
    )
    with pytest.raises(CandidateProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_agui_candidate(upstream_allows_extra, source=SOURCE)

    with pytest.raises(CandidateProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_agui_candidate(
            RunStartedEvent(
                thread_id="thread.1",
                run_id="run.1",
                timestamp=TIMESTAMP,
                raw_event={"provider": "forbidden"},
            ),
            source=SOURCE,
        )

    upstream_with_input = RunStartedEvent.model_validate(
        {
            "threadId": "thread.1",
            "runId": "run.1",
            "timestamp": TIMESTAMP,
            "input": {
                "threadId": "thread.1",
                "runId": "run.1",
                "state": {},
                "messages": [],
                "tools": [],
                "context": [],
                "forwardedProps": {},
            },
        }
    )
    with pytest.raises(CandidateProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_agui_candidate(upstream_with_input, source=SOURCE)


@pytest.mark.parametrize(("activity_type", "content"), _ALLOWED_ACTIVITY_CASES)
def test_all_closed_activity_arms_use_official_models_then_strict_content(
    activity_type: str, content: dict[str, JsonValue]
) -> None:
    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "activity.1"}
            )
        }
    )
    candidate = build_agui_candidate(
        ActivitySnapshotEvent(
            message_id="activity.1",
            activity_type=activity_type,
            content=content,
            timestamp=TIMESTAMP,
        ),
        source=source,
    )
    dumped = candidate.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert dumped["event"]["activityType"] == activity_type


def test_activity_content_is_closed_even_though_official_model_allows_any() -> None:
    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "activity.1"}
            )
        }
    )
    valid = build_agui_candidate(
        ActivitySnapshotEvent(
            message_id="activity.1",
            activity_type="kokoro.notice.v1",
            content={
                "noticeRef": "notice.1",
                "code": "working",
                "message": "正在处理",
                "severity": "info",
            },
            timestamp=TIMESTAMP,
        ),
        source=source,
    )
    assert valid.event.type == "ACTIVITY_SNAPSHOT"

    with pytest.raises(CandidateProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_agui_candidate(
            ActivitySnapshotEvent(
                message_id="activity.1",
                activity_type="kokoro.artifact.v1",
                content={"artifactRef": "artifact.1"},
                timestamp=TIMESTAMP,
            ),
            source=source,
        )


def test_jcs_digest_identity_and_unicode_are_deterministic() -> None:
    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "message.1"}
            )
        }
    )
    event = TextMessageContentEvent(
        message_id="message.1",
        delta="你好，Kokoro 👋",
        timestamp=TIMESTAMP,
    )
    first = build_agui_candidate(event, source=source)
    second = build_agui_candidate(event, source=source)

    assert first == second
    assert event_jcs_bytes(first.event) == (
        b'{"delta":"\xe4\xbd\xa0\xe5\xa5\xbd\xef\xbc\x8cKokoro \xf0\x9f\x91\x8b",'
        b'"messageId":"message.1","timestamp":0,"type":"TEXT_MESSAGE_CONTENT"}'
    )
    assert first.event_digest == (
        "sha256:" + hashlib.sha256(event_jcs_bytes(first.event)).hexdigest()
    )
    assert first.candidate_ref.startswith("agui_candidate:sha256:")


def test_lone_surrogates_are_rejected_before_digesting() -> None:
    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "message.1"}
            )
        }
    )
    with pytest.raises(CandidateProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_agui_candidate(
            TextMessageContentEvent(
                message_id="message.1",
                delta="\ud800",
                timestamp=TIMESTAMP,
            ),
            source=source,
        )


def test_candidate_envelope_rejects_extra_and_tampered_material() -> None:
    candidate = build_agui_candidate(
        RunStartedEvent(
            thread_id="thread.1",
            run_id="run.1",
            timestamp=TIMESTAMP,
        ),
        source=SOURCE,
    )
    raw = candidate.model_dump(mode="json", by_alias=True, exclude_none=True)

    with pytest.raises(ValidationError):
        AgentAguiEventCandidate.model_validate({**raw, "siteId": "site.forbidden"})

    tampered = copy.deepcopy(raw)
    tampered["eventDigest"] = "sha256:" + "0" * 64
    with pytest.raises(ValidationError, match="official event digest mismatch"):
        AgentAguiEventCandidate.model_validate(tampered)


def test_internal_message_ref_must_be_absent_instead_of_null() -> None:
    with pytest.raises(ValidationError, match="must be absent rather than null"):
        AgentAguiCandidateRoute.model_validate(
            {
                "internalRunRef": "run.1",
                "internalThreadRef": "thread.1",
                "internalMessageRef": None,
            }
        )


def test_route_recorded_at_and_message_binding_are_enforced() -> None:
    wrong_route = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_thread_ref": "thread.other"}
            )
        }
    )
    with pytest.raises(CandidateProtocolError, match="AGUI_EVENT_SCOPE_CONFLICT"):
        build_agui_candidate(
            RunStartedEvent(
                thread_id="thread.1",
                run_id="run.1",
                timestamp=TIMESTAMP,
            ),
            source=wrong_route,
        )

    different_time = SOURCE.model_copy(
        update={"recorded_at": "1970-01-01T00:00:00.001Z"}
    )
    with pytest.raises(CandidateProtocolError, match="AGUI_EVENT_TIMESTAMP_CONFLICT"):
        build_agui_candidate(
            RunStartedEvent(
                thread_id="thread.1",
                run_id="run.1",
                timestamp=TIMESTAMP,
            ),
            source=different_time,
        )

    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "message.1"}
            )
        }
    )
    with pytest.raises(CandidateProtocolError, match="AGUI_EVENT_SEGMENT_CONFLICT"):
        build_agui_candidate(
            TextMessageContentEvent(
                message_id="message.other",
                delta="delta",
                timestamp=TIMESTAMP,
            ),
            source=source,
        )


@pytest.mark.parametrize("source_ordinal", ["00", "01", "18446744073709551616"])
def test_source_ordinal_is_canonical_uint64_decimal(source_ordinal: str) -> None:
    with pytest.raises(ValidationError):
        AgentAguiCandidateSource(
            source_event_ref="source.1",
            source_ordinal=source_ordinal,
            recorded_at="1970-01-01T00:00:00.000Z",
            route=SOURCE.route,
        )


def test_closed_text_and_candidate_size_limits_are_enforced() -> None:
    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "message.1"}
            )
        }
    )
    with pytest.raises(CandidateProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_agui_candidate(
            TextMessageContentEvent(
                message_id="message.1",
                delta="界" * 16_385,
                timestamp=TIMESTAMP,
            ),
            source=source,
        )


def test_pure_mapping_emits_only_semantically_safe_candidates() -> None:
    started = RunStarted(
        kind="run.started",
        run_id="run.1",
        index=0,
        timestamp=TIMESTAMP,
        durable_seq=41,
        event_id="run.1:0",
        payload=RunStartedPayload(),
    )
    start_candidates = map_agent_event_candidates(
        started,
        thread_ref="thread.1",
        source_event_ref="run.1:0",
    )
    assert [candidate.event.type for candidate in start_candidates] == ["RUN_STARTED"]
    assert start_candidates[0].source.source_ordinal == "0"
    assert start_candidates[0].source.source_ordinal != str(started.durable_seq)

    completed = RunCompleted(
        kind="run.completed",
        run_id="run.1",
        index=1,
        timestamp=1,
        payload=RunCompletedPayload(status="completed"),
    )
    finish_candidates = map_agent_event_candidates(
        completed,
        thread_ref="thread.1",
        source_event_ref="source.event.2",
    )
    finish_dump = finish_candidates[0].event.model_dump(
        mode="json", by_alias=True, exclude_none=True
    )
    assert finish_dump["outcome"] == {"type": "success"}

    canceled = completed.model_copy(
        update={"payload": RunCompletedPayload(status="cancelled")}
    )
    assert (
        map_agent_event_candidates(
            canceled,
            thread_ref="thread.1",
            source_event_ref="source.event.cancelled",
        )
        == ()
    )

    thinking = ThinkingDelta(
        kind="thinking.delta",
        run_id="run.1",
        index=2,
        timestamp=2,
        payload=ThinkingDeltaPayload(segment_id="thinking.1", delta="private"),
    )
    assert (
        map_agent_event_candidates(
            thinking,
            thread_ref="thread.1",
            source_event_ref="source.event.thinking",
        )
        == ()
    )


def test_current_tool_facts_map_to_zero_until_atomic_segment_transition_exists() -> None:
    invoked = ToolInvoked(
        kind="tool.invoked",
        run_id="run.1",
        index=4,
        timestamp=4,
        payload=ToolInvokedPayload(
            segment_id="tool.1",
            tool_id="tool.1",
            name="web_fetch",
            args={"authorization": "Bearer secret"},
        ),
    )
    assert (
        map_agent_event_candidates(
            invoked,
            thread_ref="thread.1",
            source_event_ref="source.event.tool",
        )
        == ()
    )


def test_message_delta_maps_to_zero_without_fabricating_stream_start() -> None:
    delta = MessageDelta(
        kind="message.delta",
        run_id="run.1",
        index=3,
        timestamp=3,
        payload=MessageDeltaPayload(segment_id="message.1", delta="hello"),
    )
    assert (
        map_agent_event_candidates(
            delta,
            thread_ref="thread.1",
            source_event_ref="source.event.message",
        )
        == ()
    )
