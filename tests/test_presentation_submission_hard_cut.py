from __future__ import annotations

import hashlib
from pathlib import Path

from ag_ui.core import RunStartedEvent

import kokoro_agent.presentation.adapter as presentation_adapter
import kokoro_agent.presentation.runtime as presentation_runtime
import kokoro_agent.storage.mongo as presentation_storage
import pytest
from pydantic import ValidationError
from kokoro_agent.presentation.submission import (
    PresentationSubmission,
    SubmissionRoute,
    SubmissionSource,
)


REPOSITORY = Path(__file__).resolve().parents[1]
PRESENTATION_SOURCE = REPOSITORY / "src/kokoro_agent/presentation"


def _source() -> SubmissionSource:
    return SubmissionSource(
        source_event_ref="agent.source.0",
        event_ordinal="0",
        recorded_at="1970-01-01T00:00:01.000Z",
        route=SubmissionRoute(
            internal_run_ref="run.1",
            internal_thread_ref="agent.thread:thread.1",
        ),
    )


def test_official_adapter_builds_root_submission_directly() -> None:
    builder = getattr(presentation_adapter, "build_submission", None)

    assert builder is not None, "official adapter must expose build_submission"
    submission = builder(
        RunStartedEvent(
            thread_id="agent.thread:thread.1",
            run_id="run.1",
            timestamp=1_000,
        ),
        source=_source(),
    )

    assert isinstance(submission, PresentationSubmission)
    assert submission.source.event_ordinal == "0"
    assert submission.submission_ref.startswith("presentation.submission:sha256:")
    assert submission.envelope_bytes() == submission.envelope_bytes()


def test_delivery_record_persists_the_canonical_submission_envelope() -> None:
    builder = getattr(presentation_adapter, "build_submission", None)
    record_type = getattr(presentation_runtime, "DeliveryRecord", None)

    assert builder is not None
    assert record_type is not None, "runtime must expose DeliveryRecord"
    submission = builder(
        RunStartedEvent(
            thread_id="agent.thread:thread.1",
            run_id="run.1",
            timestamp=1_000,
        ),
        source=_source(),
    )
    record = record_type.from_submission(
        run_id="run.1",
        delivery_seq=1,
        submission=submission,
        producer_instance_ref="producer.1",
        producer_generation=1,
    )

    assert record.envelope_bytes == submission.envelope_bytes()
    assert record.envelope_digest == (
        "sha256:" + hashlib.sha256(record.envelope_bytes).hexdigest()
    )
    assert record.submission_ref == submission.submission_ref
    assert record.submission_digest == record.envelope_digest


def test_delivery_record_rejects_recorded_time_drift() -> None:
    builder = getattr(presentation_adapter, "build_submission", None)
    record_type = getattr(presentation_runtime, "DeliveryRecord", None)
    assert builder is not None and record_type is not None
    submission = builder(
        RunStartedEvent(
            thread_id="agent.thread:thread.1",
            run_id="run.1",
            timestamp=1_000,
        ),
        source=_source(),
    )
    record = record_type.from_submission(
        run_id="run.1",
        delivery_seq=1,
        submission=submission,
        producer_instance_ref="producer.1",
        producer_generation=1,
    )

    with pytest.raises(ValidationError, match="recorded time"):
        record_type.model_validate(
            {**record.model_dump(mode="python"), "recorded_at_ms": 1_001}
        )


def test_delivery_sequence_is_submission_ordinal_plus_one() -> None:
    builder = getattr(presentation_adapter, "build_submission", None)
    record_type = getattr(presentation_runtime, "DeliveryRecord", None)
    assert builder is not None and record_type is not None
    submission = builder(
        RunStartedEvent(
            thread_id="agent.thread:thread.1",
            run_id="run.1",
            timestamp=1_000,
        ),
        source=_source(),
    )

    with pytest.raises(ValidationError, match="event ordinal"):
        record_type.from_submission(
            run_id="run.1",
            delivery_seq=2,
            submission=submission,
            producer_instance_ref="producer.1",
            producer_generation=1,
        )


def test_retired_bridge_and_vocabulary_are_deleted_from_production() -> None:
    retired_term = "candi" + "date"
    assert not (PRESENTATION_SOURCE / f"{retired_term}.py").exists()
    assert not hasattr(PresentationSubmission, "from_" + retired_term)

    forbidden = (
        "AgentAgui" + retired_term.title(),
        "AgentAguiEvent" + retired_term.title(),
        "Presentation" + retired_term.title(),
        "build_agui_" + retired_term,
        "agui_" + retired_term + ":",
        "kokoro-agent-agui-" + retired_term + ".v1",
    )
    production = (
        list(PRESENTATION_SOURCE.rglob("*.py"))
        + [
            REPOSITORY / "src/kokoro_agent/storage/ledger.py",
            REPOSITORY / "src/kokoro_agent/storage/mongo.py",
            REPOSITORY / "src/kokoro_agent/worker/supervisor.py",
        ]
    )
    matches = {
        token: str(path.relative_to(REPOSITORY))
        for path in production
        for token in forbidden
        if token in path.read_text()
    }

    assert matches == {}


def test_mongo_uses_only_submission_delivery_collection_names() -> None:
    expected = {
        "AGENT_PRESENTATION_DELIVERY_RECORD_COLLECTION": (
            "agent_presentation_delivery_record"
        ),
        "AGENT_PRESENTATION_SOURCE_COMMIT_COLLECTION": (
            "agent_presentation_source_commit"
        ),
        "AGENT_PRESENTATION_PLANNER_STATE_COLLECTION": (
            "agent_presentation_planner_state"
        ),
        "AGENT_PRESENTATION_DELIVERY_STATE_COLLECTION": (
            "agent_presentation_delivery_state"
        ),
        "AGENT_PRESENTATION_ADMISSION_COMMAND_RECEIPT_COLLECTION": (
            "agent_presentation_admission_command_receipt"
        ),
    }

    assert {
        name: getattr(presentation_storage, name, None) for name in expected
    } == expected
    mongo_source = (REPOSITORY / "src/kokoro_agent/storage/mongo.py").read_text()
    for retired in (
        "agent_presentation_" + "candi" + "date",
        "agent_presentation_source_" + "batch",
        '"agent_presentation_' + 'state"',
        '"agent_presentation_' + 'delivery"',
        '"agent_presentation_admission_' + 'command"',
    ):
        assert retired not in mongo_source
