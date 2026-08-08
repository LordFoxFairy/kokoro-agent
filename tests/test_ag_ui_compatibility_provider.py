from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest
from ag_ui.core import (
    ActivitySnapshotEvent,
    RunFinishedEvent,
    RunStartedEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    TextMessageStartEvent,
)
from pydantic import ValidationError

from kokoro_agent.presentation import PresentationSubmission
from kokoro_agent.presentation.model import (
    ClosedSafeSummaryActivity,
    ClosedTextContentEvent,
    ClosedTextEndEvent,
    ClosedTextStartEvent,
)
from scripts.compat.ag_ui_submission_provider import (
    PRESENTATION_COMPATIBILITY_FIXTURE_PROFILE,
    AguiCompatibilityFixtureInput,
    CompatibilityProviderError,
    build_session_compatibility_input,
    main,
    read_bounded_regular_file,
    run_cli,
)


def _fixture() -> AguiCompatibilityFixtureInput:
    return AguiCompatibilityFixtureInput.model_validate(
        {
            "profileRevision": PRESENTATION_COMPATIBILITY_FIXTURE_PROFILE,
            "scope": {
                "siteId": "site.compat.1",
                "sessionId": "session.compat.1",
                "streamEpoch": "1",
            },
            "producer": {
                "producerInstanceRef": "agent.compat.1",
                "producerGeneration": "1",
            },
            "cursorAuthority": {"prefix": "compat.cursor."},
            "replayPageLimit": 2,
            "fixture": {
                "internalThreadRef": "agent.thread:compat.1",
                "internalRunRef": "run.compat.1",
                "internalMessageRef": "message.compat.1",
                "internalActivityRef": "activity.compat.1",
                "sourceEventPrefix": "agent.event.compat",
                "startedAtMs": 1_735_689_600_000,
                "textDeltas": ["Hello, ", "Kokoro!"],
            },
        }
    )


def test_provider_uses_production_submissions_for_complete_official_sequence() -> None:
    output = build_session_compatibility_input(_fixture()).model_dump(
        mode="json", by_alias=True, exclude_none=True
    )

    assert output["profileRevision"] == "kokoro-session-presentation-compatibility-input.v1"
    assert output["scope"] == {
        "siteId": "site.compat.1",
        "sessionId": "session.compat.1",
        "streamEpoch": "1",
    }
    assert len(output["submissions"]) == 7

    envelopes = [
        PresentationSubmission.model_validate_json(
            base64.b64decode(submission["envelopeBase64"], validate=True)
        )
        for submission in output["submissions"]
    ]
    assert [envelope.event.type for envelope in envelopes] == [
        "RUN_STARTED",
        "ACTIVITY_SNAPSHOT",
        "TEXT_MESSAGE_START",
        "TEXT_MESSAGE_CONTENT",
        "TEXT_MESSAGE_CONTENT",
        "TEXT_MESSAGE_END",
        "RUN_FINISHED",
    ]
    official_types = (
        RunStartedEvent,
        ActivitySnapshotEvent,
        TextMessageStartEvent,
        TextMessageContentEvent,
        TextMessageContentEvent,
        TextMessageEndEvent,
        RunFinishedEvent,
    )
    assert all(
        official_type.model_validate(
            envelope.event.model_dump(mode="json", by_alias=True, exclude_none=True)
        )
        for official_type, envelope in zip(official_types, envelopes, strict=True)
    )
    assert [envelope.source.event_ordinal for envelope in envelopes] == [
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
    ]
    assert [submission["binding"]["sourceEventRef"] for submission in output["submissions"]] == [
        f"agent.event.compat.{ordinal}" for ordinal in range(7)
    ]
    assert [submission["binding"]["expectedEventOrdinal"] for submission in output["submissions"]] == [
        str(ordinal) for ordinal in range(7)
    ]
    assert all(
        submission["submissionRef"] == envelope.submission_ref
        for submission, envelope in zip(output["submissions"], envelopes, strict=True)
    )
    assert "message" not in output["submissions"][0]["binding"]
    assert "message" not in output["submissions"][-1]["binding"]
    assert all(
        submission["binding"]["message"]
        == {
            "internalMessageRef": "message.compat.1",
            "segmentOrdinal": 0,
            "runInternalRunRef": "run.compat.1",
            "runSegmentOrdinal": 0,
        }
        for submission in output["submissions"][2:-1]
    )
    activity = envelopes[1].event
    assert isinstance(activity, ClosedSafeSummaryActivity)
    assert activity.message_id == "activity.compat.1"
    assert activity.message_id not in {
        envelope.event.message_id
        for envelope in envelopes
        if isinstance(
            envelope.event,
            (ClosedTextStartEvent, ClosedTextContentEvent, ClosedTextEndEvent),
        )
    }
    assert output["submissions"][1]["binding"]["message"]["internalMessageRef"] == (
        "activity.compat.1"
    )


def test_fixture_input_is_closed_and_bounded() -> None:
    raw = _fixture().model_dump(mode="json", by_alias=True)
    raw["siteId"] = "forbidden"
    with pytest.raises(ValidationError):
        AguiCompatibilityFixtureInput.model_validate(raw)

    raw = _fixture().model_dump(mode="json", by_alias=True)
    raw["fixture"]["textDeltas"] = ["only-one"]
    with pytest.raises(ValidationError):
        AguiCompatibilityFixtureInput.model_validate(raw)

    raw = _fixture().model_dump(mode="json", by_alias=True)
    raw["fixture"]["sourceEventPrefix"] = "a" * 127
    with pytest.raises(ValidationError):
        AguiCompatibilityFixtureInput.model_validate(raw)

    raw = _fixture().model_dump(mode="json", by_alias=True)
    raw["fixture"]["startedAtMs"] = 253_402_300_799_988
    with pytest.raises(ValidationError):
        AguiCompatibilityFixtureInput.model_validate(raw)


def test_bounded_reader_rejects_symlink_and_capacity(tmp_path: Path) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_text("{}", encoding="utf-8")
    symlink_path = tmp_path / "input-link.json"
    symlink_path.symlink_to(input_path)

    with pytest.raises(CompatibilityProviderError, match="AGUI_COMPAT_INPUT_FILE_INVALID"):
        read_bounded_regular_file(symlink_path)

    input_path.write_bytes(b"x" * 65_537)
    with pytest.raises(CompatibilityProviderError, match="AGUI_COMPAT_INPUT_CAPACITY_EXCEEDED"):
        read_bounded_regular_file(input_path)


def test_cli_is_one_line_json_and_rejects_unknown_fields(tmp_path: Path) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_text(
        json.dumps(_fixture().model_dump(mode="json", by_alias=True)),
        encoding="utf-8",
    )

    rendered = run_cli(input_path)
    assert rendered.endswith("\n")
    assert rendered.count("\n") == 1
    assert json.loads(rendered)["profileRevision"] == (
        "kokoro-session-presentation-compatibility-input.v1"
    )

    raw = _fixture().model_dump(mode="json", by_alias=True)
    raw["fixture"]["providerSecret"] = "forbidden"
    input_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(CompatibilityProviderError, match="AGUI_COMPAT_INPUT_INVALID"):
        run_cli(input_path)


def test_process_boundary_emits_only_json_or_stable_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_text(
        json.dumps(_fixture().model_dump(mode="json", by_alias=True)),
        encoding="utf-8",
    )

    assert main(("--input", str(input_path))) == 0
    success = capsys.readouterr()
    assert success.err == ""
    assert success.out.count("\n") == 1
    assert json.loads(success.out)["profileRevision"] == (
        "kokoro-session-presentation-compatibility-input.v1"
    )

    assert main(("--unknown", str(input_path))) == 2
    failure = capsys.readouterr()
    assert failure.out == ""
    assert failure.err == "AGUI_COMPAT_USAGE_INVALID\n"
