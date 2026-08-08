from __future__ import annotations

import copy
import hashlib
import json
from importlib.metadata import version
from pathlib import Path
from typing import cast

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

from kokoro_agent.presentation import (
    PRESENTATION_SUBMISSION_CONTRACT_REVISION,
    AGUI_UPSTREAM_COMMIT,
    SubmissionRoute,
    SubmissionSource,
    PresentationSubmission,
    SubmissionProtocolError,
    build_submission,
)
from kokoro_agent.presentation.model import canonical_json_bytes, event_jcs_bytes


TIMESTAMP = 0
ROOT_PRESENTATION_CORPUS = (
    Path(__file__).resolve().parents[1]
    / "src/generated/contracts/presentation/corpus-v1.json"
)
SOURCE = SubmissionSource(
    source_event_ref="source.event.1",
    event_ordinal="0",
    recorded_at="1970-01-01T00:00:00.000Z",
    route=SubmissionRoute(
        internal_run_ref="run.1",
        internal_thread_ref="agent.thread:test.1",
    ),
)

_ALLOWED_ACTIVITY_CASES: list[tuple[str, dict[str, JsonValue]]] = [
    (
        "kokoro.safe-summary.v1",
        {
            "partRef": "part.1",
            "ownerVersion": "1",
            "summary": "safe",
            "status": "complete",
            "updatedAt": "1970-01-01T00:00:00.000Z",
        },
    ),
    (
        "kokoro.tool-preview.v1",
        {
            "toolCallRef": "tool.1",
            "ownerVersion": "1",
            "label": "search",
            "status": "running",
            "updatedAt": "1970-01-01T00:00:00.000Z",
        },
    ),
    (
        "kokoro.hitl.v1",
        {
            "ownerRef": "owner.1",
            "ownerVersion": "1",
            "decisionGroupRef": "decision-group.1",
            "requiredOwnerRefs": ["owner.1"],
            "controlRef": "control.1",
            "kind": "approval",
            "title": "Approve",
            "description": "Review the action",
            "allowedActions": ["approve", "reject"],
            "status": "pending",
            "updatedAt": "1970-01-01T00:00:00.000Z",
        },
    ),
    (
        "kokoro.plan.v1",
        {
            "planRef": "plan.1",
            "ownerVersion": "1",
            "summary": "Plan",
            "status": "proposed",
            "steps": [{"stepRef": "step.1", "label": "First", "status": "pending"}],
            "updatedAt": "1970-01-01T00:00:00.000Z",
        },
    ),
    (
        "kokoro.subagent.v1",
        {
            "subagentRef": "subagent.1",
            "ownerVersion": "1",
            "status": "running",
            "updatedAt": "1970-01-01T00:00:00.000Z",
        },
    ),
    (
        "kokoro.notice.v1",
        {
            "noticeRef": "notice.1",
            "ownerVersion": "1",
            "code": "working",
            "message": "正在处理",
            "severity": "info",
            "updatedAt": "1970-01-01T00:00:00.000Z",
        },
    ),
    (
        "kokoro.error.v1",
        {
            "errorRef": "error.1",
            "ownerVersion": "1",
            "code": "tool.failed",
            "message": "Failed safely",
            "retryClass": "never",
            "updatedAt": "1970-01-01T00:00:00.000Z",
        },
    ),
]


def test_profile_pins_official_python_sdk_commit() -> None:
    assert PRESENTATION_SUBMISSION_CONTRACT_REVISION == "kokoro.presentation.submission.v1"
    assert AGUI_UPSTREAM_COMMIT == "54f13419055b4d0f442c71e1efab18b310982ce1"
    assert version("ag-ui-protocol") == "0.1.19"
    assert EventType.RUN_STARTED.value == "RUN_STARTED"


def test_official_aliases_become_the_single_typed_event_fact() -> None:
    submission = build_submission(
        RunStartedEvent(
            thread_id="agent.thread:test.1",
            run_id="run.1",
            timestamp=TIMESTAMP,
        ),
        source=SOURCE,
    )
    dumped = submission.model_dump(mode="json", by_alias=True, exclude_none=True)

    assert dumped["event"] == {
        "runId": "run.1",
        "threadId": "agent.thread:test.1",
        "timestamp": 0,
        "type": "RUN_STARTED",
    }
    assert "internalMessageRef" not in dumped["source"]["route"]
    official = RunStartedEvent.model_validate(dumped["event"])
    assert isinstance(official, RunStartedEvent)
    assert official.run_id == "run.1"


def test_run_started_parent_is_forbidden_until_session_derives_lineage() -> None:
    with pytest.raises(SubmissionProtocolError, match="RUN_STARTED_PARENT_FORBIDDEN"):
        build_submission(
            RunStartedEvent(
                thread_id="agent.thread:test.1",
                run_id="run.1",
                parent_run_id="run.parent",
                timestamp=TIMESTAMP,
            ),
            source=SOURCE,
        )


@pytest.mark.parametrize(
    "source",
    [
        SOURCE.model_copy(update={"event_ordinal": "01"}),
        SOURCE.model_copy(update={"source_event_ref": "invalid ref"}),
        SOURCE.model_copy(
            update={
                "route": SOURCE.route.model_copy(
                    update={"internal_thread_ref": "invalid thread ref"}
                )
            }
        ),
        SubmissionSource.model_construct(
            source_event_ref="source.event.constructed",
            event_ordinal="18446744073709551616",
            recorded_at="1970-01-01T00:00:00.000Z",
            route=SOURCE.route,
        ),
    ],
)
def test_builder_revalidates_caller_source_instances(
    source: SubmissionSource,
) -> None:
    with pytest.raises(SubmissionProtocolError, match="PRESENTATION_SUBMISSION_SOURCE_INVALID"):
        build_submission(
            RunStartedEvent(
                thread_id="agent.thread:test.1",
                run_id="run.1",
                timestamp=TIMESTAMP,
            ),
            source=source,
        )


def test_run_finished_requires_explicit_success_and_forbids_result() -> None:
    successful = build_submission(
        RunFinishedEvent(
            thread_id="agent.thread:test.1",
            run_id="run.1",
            timestamp=TIMESTAMP,
            outcome=RunFinishedSuccessOutcome(),
        ),
        source=SOURCE,
    )
    dumped = successful.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert dumped["event"]["outcome"] == {"type": "success"}

    with pytest.raises(SubmissionProtocolError, match="RUN_FINISHED_SUCCESS_REQUIRED"):
        build_submission(
            RunFinishedEvent(
                thread_id="agent.thread:test.1",
                run_id="run.1",
                timestamp=TIMESTAMP,
            ),
            source=SOURCE,
        )

    with pytest.raises(SubmissionProtocolError, match="RUN_FINISHED_RESULT_FORBIDDEN"):
        build_submission(
            RunFinishedEvent(
                thread_id="agent.thread:test.1",
                run_id="run.1",
                timestamp=TIMESTAMP,
                outcome=RunFinishedSuccessOutcome(),
                result={"secret": "not-a-presentation-owner"},
            ),
            source=SOURCE,
        )


def test_forbidden_official_families_and_extra_fields_fail_closed() -> None:
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_TYPE_FORBIDDEN"):
        build_submission(
            RawEvent(event={"payload": "raw"}, timestamp=TIMESTAMP),
            source=SOURCE,
        )
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_TYPE_FORBIDDEN"):
        build_submission(
            CustomEvent(name="unsafe", value={}, timestamp=TIMESTAMP),
            source=SOURCE,
        )

    upstream_allows_extra = RunStartedEvent.model_validate(
        {
            "threadId": "agent.thread:test.1",
            "runId": "run.1",
            "timestamp": TIMESTAMP,
            "userId": "user.must-not-cross",
        }
    )
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_submission(upstream_allows_extra, source=SOURCE)

    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_submission(
            RunStartedEvent(
                thread_id="agent.thread:test.1",
                run_id="run.1",
                timestamp=TIMESTAMP,
                raw_event={"provider": "forbidden"},
            ),
            source=SOURCE,
        )

    upstream_with_input = RunStartedEvent.model_validate(
        {
            "threadId": "agent.thread:test.1",
            "runId": "run.1",
            "timestamp": TIMESTAMP,
            "input": {
                "threadId": "agent.thread:test.1",
                "runId": "run.1",
                "state": {},
                "messages": [],
                "tools": [],
                "context": [],
                "forwardedProps": {},
            },
        }
    )
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_submission(upstream_with_input, source=SOURCE)


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
    submission = build_submission(
        ActivitySnapshotEvent(
            message_id="activity.1",
            activity_type=activity_type,
            content=content,
            timestamp=TIMESTAMP,
        ),
        source=source,
    )
    dumped = submission.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert dumped["event"]["activityType"] == activity_type


def test_activity_content_is_closed_even_though_official_model_allows_any() -> None:
    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "activity.1"}
            )
        }
    )
    valid = build_submission(
        ActivitySnapshotEvent(
            message_id="activity.1",
            activity_type="kokoro.notice.v1",
            content={
                "noticeRef": "notice.1",
                "ownerVersion": "1",
                "code": "working",
                "message": "正在处理",
                "severity": "info",
                "updatedAt": "1970-01-01T00:00:00.000Z",
            },
            timestamp=TIMESTAMP,
        ),
        source=source,
    )
    assert valid.event.type == "ACTIVITY_SNAPSHOT"

    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_submission(
            ActivitySnapshotEvent(
                message_id="activity.1",
                activity_type="kokoro.artifact.v1",
                content={"artifactRef": "artifact.1"},
                timestamp=TIMESTAMP,
            ),
            source=source,
        )


@pytest.mark.parametrize(
    ("activity_type", "content"),
    [
        (
            "kokoro.media.v1",
            {
                "operationRef": "media.1",
                "ownerVersion": "1",
                "state": "active",
                "progressBps": 5000,
                "updatedAt": "1970-01-01T00:00:00.000Z",
            },
        ),
        (
            "kokoro.artifact.v1",
            {
                "artifactRef": "artifact.1",
                "ownerVersion": "1",
                "updatedAt": "1970-01-01T00:00:00.000Z",
            },
        ),
        (
            "kokoro.cost.v1",
            {
                "costRef": "cost.1",
                "ownerVersion": "1",
                "updatedAt": "1970-01-01T00:00:00.000Z",
            },
        ),
    ],
)
def test_platform_owned_activity_arms_are_forbidden_to_agent(
    activity_type: str, content: dict[str, JsonValue]
) -> None:
    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "activity.1"}
            )
        }
    )
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_submission(
            ActivitySnapshotEvent(
                message_id="activity.1",
                activity_type=activity_type,
                content=content,
                timestamp=TIMESTAMP,
            ),
            source=source,
        )


@pytest.mark.parametrize("owner_version", [1, 0, "0", "01", "18446744073709551616"])
def test_activity_owner_version_is_positive_uint64_decimal_string(
    owner_version: JsonValue,
) -> None:
    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "activity.1"}
            )
        }
    )
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_submission(
            ActivitySnapshotEvent(
                message_id="activity.1",
                activity_type="kokoro.safe-summary.v1",
                content={
                    "partRef": "part.1",
                    "ownerVersion": owner_version,
                    "summary": "safe",
                    "status": "complete",
                    "updatedAt": "1970-01-01T00:00:00.000Z",
                },
                timestamp=TIMESTAMP,
            ),
            source=source,
        )


@pytest.mark.parametrize(
    "updated_at",
    ["0000-01-01T00:00:00.000Z", "1970-01-01T00:00:00.001Z"],
)
def test_activity_updated_at_is_canonical_and_not_after_event(updated_at: str) -> None:
    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "activity.1"}
            )
        }
    )
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_submission(
            ActivitySnapshotEvent(
                message_id="activity.1",
                activity_type="kokoro.safe-summary.v1",
                content={
                    "partRef": "part.1",
                    "ownerVersion": "1",
                    "summary": "safe",
                    "status": "complete",
                    "updatedAt": updated_at,
                },
                timestamp=TIMESTAMP,
            ),
            source=source,
        )


def test_hitl_submission_is_pending_proposal_with_complete_immutable_ancestry() -> None:
    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "activity.1"}
            )
        }
    )
    content: dict[str, JsonValue] = {
        "ownerRef": "owner.1",
        "ownerVersion": "1",
        "decisionGroupRef": "decision-group.1",
        "requiredOwnerRefs": ["owner.1"],
        "controlRef": "control.1",
        "kind": "approval",
        "title": "Approve",
        "description": "Review",
        "allowedActions": ["approve", "reject"],
        "status": "accepted",
        "receiptRef": "receipt.forbidden",
        "updatedAt": "1970-01-01T00:00:00.000Z",
    }
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_submission(
            ActivitySnapshotEvent(
                message_id="activity.1",
                activity_type="kokoro.hitl.v1",
                content=content,
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
    first = build_submission(event, source=source)
    second = build_submission(event, source=source)

    assert first == second
    assert event_jcs_bytes(first.event) == (
        b'{"delta":"\xe4\xbd\xa0\xe5\xa5\xbd\xef\xbc\x8cKokoro \xf0\x9f\x91\x8b",'
        b'"messageId":"message.1","timestamp":0,"type":"TEXT_MESSAGE_CONTENT"}'
    )
    assert first.event_digest == (
        "sha256:" + hashlib.sha256(event_jcs_bytes(first.event)).hexdigest()
    )
    assert first.submission_ref.startswith("presentation.submission:sha256:")


def test_lone_surrogates_are_rejected_before_digesting() -> None:
    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "message.1"}
            )
        }
    )
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_submission(
            TextMessageContentEvent(
                message_id="message.1",
                delta="\ud800",
                timestamp=TIMESTAMP,
            ),
            source=source,
        )


@pytest.mark.parametrize(
    "value",
    [
        "e\u0301",
        {"e\u0301": "value"},
        {"outer": [{"inner": "e\u0301"}]},
    ],
    ids=("scalar", "key", "nested-value"),
)
def test_jcs_rejects_every_non_nfc_key_and_string_value(value: JsonValue) -> None:
    with pytest.raises(ValueError, match="NFC"):
        canonical_json_bytes(value)


def test_root_submission_corpus_is_accepted_and_rejected_exactly() -> None:
    corpus = cast(dict[str, object], json.loads(ROOT_PRESENTATION_CORPUS.read_text()))
    submissions = cast(dict[str, object], corpus["submissions"])
    accepted = cast(list[dict[str, object]], submissions["accepted"])
    rejected = cast(list[dict[str, object]], submissions["rejected"])

    parsed = [
        PresentationSubmission.model_validate_json(json.dumps(row["submission"]))
        for row in accepted
    ]
    assert len(parsed) == 14
    assert all(
        item.envelope_bytes()
        == canonical_json_bytes(
            item.model_dump(mode="json", by_alias=True, exclude_none=True)
        )
        for item in parsed
    )
    for row in rejected:
        with pytest.raises(ValidationError):
            PresentationSubmission.model_validate_json(json.dumps(row["submission"]))


def test_submission_envelope_rejects_extra_and_tampered_material() -> None:
    submission = build_submission(
        RunStartedEvent(
            thread_id="agent.thread:test.1",
            run_id="run.1",
            timestamp=TIMESTAMP,
        ),
        source=SOURCE,
    )
    raw = submission.model_dump(mode="json", by_alias=True, exclude_none=True)

    with pytest.raises(ValidationError):
        PresentationSubmission.model_validate({**raw, "siteId": "site.forbidden"})

    tampered = copy.deepcopy(raw)
    tampered["eventDigest"] = "sha256:" + "0" * 64
    with pytest.raises(ValidationError, match="PRESENTATION_SUBMISSION_EVENT_DIGEST_INVALID"):
        PresentationSubmission.model_validate(tampered)


def test_internal_message_ref_must_be_absent_instead_of_null() -> None:
    with pytest.raises(ValidationError, match="must be absent rather than null"):
        SubmissionRoute.model_validate(
            {
                "internalRunRef": "run.1",
                "internalThreadRef": "agent.thread:test.1",
                "internalMessageRef": None,
            }
        )


def test_internal_thread_ref_requires_agent_owner_brand() -> None:
    with pytest.raises(ValidationError):
        SubmissionRoute.model_validate(
            {
                "internalRunRef": "run.1",
                "internalThreadRef": "thread.session.1",
            }
        )


def test_route_recorded_at_and_message_binding_are_enforced() -> None:
    wrong_route = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_thread_ref": "agent.thread:test.other"}
            )
        }
    )
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_SCOPE_CONFLICT"):
        build_submission(
            RunStartedEvent(
                thread_id="agent.thread:test.1",
                run_id="run.1",
                timestamp=TIMESTAMP,
            ),
            source=wrong_route,
        )

    different_time = SOURCE.model_copy(
        update={"recorded_at": "1970-01-01T00:00:00.001Z"}
    )
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_TIMESTAMP_CONFLICT"):
        build_submission(
            RunStartedEvent(
                thread_id="agent.thread:test.1",
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
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_SEGMENT_CONFLICT"):
        build_submission(
            TextMessageContentEvent(
                message_id="message.other",
                delta="delta",
                timestamp=TIMESTAMP,
            ),
            source=source,
        )


@pytest.mark.parametrize("event_ordinal", ["00", "01", "18446744073709551616"])
def test_event_ordinal_is_canonical_uint64_decimal(event_ordinal: str) -> None:
    with pytest.raises(ValidationError):
        SubmissionSource(
            source_event_ref="source.1",
            event_ordinal=event_ordinal,
            recorded_at="1970-01-01T00:00:00.000Z",
            route=SOURCE.route,
        )


def test_closed_text_and_submission_size_limits_are_enforced() -> None:
    source = SOURCE.model_copy(
        update={
            "route": SOURCE.route.model_copy(
                update={"internal_message_ref": "message.1"}
            )
        }
    )
    with pytest.raises(SubmissionProtocolError, match="AGUI_EVENT_SHAPE_FORBIDDEN"):
        build_submission(
            TextMessageContentEvent(
                message_id="message.1",
                delta="界" * 16_385,
                timestamp=TIMESTAMP,
            ),
            source=source,
        )
