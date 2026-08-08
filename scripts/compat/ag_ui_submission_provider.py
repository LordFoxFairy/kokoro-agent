#!/usr/bin/env python3
"""Emit official Agent AG-UI submissions for Root compatibility verification."""

from __future__ import annotations

import base64
import os
import stat
import sys
from pathlib import Path
from typing import Annotated, Literal, TypeAlias

from ag_ui.core import (
    ActivitySnapshotEvent,
    BaseEvent,
    RunFinishedEvent,
    RunFinishedSuccessOutcome,
    RunStartedEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    TextMessageStartEvent,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel

from kokoro_agent.presentation import (
    SubmissionRoute,
    SubmissionSource,
    PresentationSubmission,
    SubmissionProtocolError,
    build_submission,
)
from kokoro_agent.presentation.model import canonical_recorded_at

PRESENTATION_COMPATIBILITY_FIXTURE_PROFILE = (
    "kokoro-agent-presentation-compatibility-fixture.v1"
)
SESSION_PRESENTATION_COMPATIBILITY_INPUT_PROFILE = (
    "kokoro-session-presentation-compatibility-input.v1"
)
MAXIMUM_INPUT_BYTES = 65_536
MAXIMUM_OUTPUT_BYTES = 2_097_152
MAXIMUM_SUBMISSION_ENVELOPE_BYTES = 131_072
MAXIMUM_TEXT_DELTAS = 8
MAXIMUM_UINT64 = (1 << 64) - 1
MAXIMUM_START_TIMESTAMP_MS = 253_402_300_799_987

_Id = Annotated[
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
_PositiveUint64 = Annotated[
    str,
    StringConstraints(pattern=r"^[1-9][0-9]{0,19}$"),
]
_Uint64 = Annotated[
    str,
    StringConstraints(pattern=r"^(0|[1-9][0-9]{0,19})$"),
]
_CursorPrefix = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z._~-][A-Za-z0-9._~-]*$",
    ),
]
_SourceEventPrefix = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=125,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]
_TextDelta = Annotated[str, StringConstraints(min_length=1, max_length=16_384)]
_SubmissionRef = Annotated[
    str,
    StringConstraints(pattern=r"^presentation\.submission:sha256:[0-9a-f]{64}$"),
]
_Base64Envelope = Annotated[
    str,
    StringConstraints(
        min_length=4,
        max_length=((MAXIMUM_SUBMISSION_ENVELOPE_BYTES + 2) // 3) * 4,
        pattern=(
            r"^(?:[A-Za-z0-9+/]{4})*"
            r"(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"
        ),
    ),
]
CompatibilityEventType: TypeAlias = Literal[
    "RUN_STARTED",
    "ACTIVITY_SNAPSHOT",
    "TEXT_MESSAGE_START",
    "TEXT_MESSAGE_CONTENT",
    "TEXT_MESSAGE_END",
    "RUN_FINISHED",
]


class CompatibilityProviderError(RuntimeError):
    """Stable process-boundary failure code."""


class _ClosedModel(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        alias_generator=to_camel,
        populate_by_name=True,
        revalidate_instances="always",
    )


class CompatibilityScope(_ClosedModel):
    site_id: _Id
    session_id: _Id
    stream_epoch: _PositiveUint64

    @field_validator("stream_epoch")
    @classmethod
    def validate_stream_epoch(cls, value: str) -> str:
        if int(value) > MAXIMUM_UINT64:
            raise ValueError("streamEpoch exceeds uint64")
        return value


class CompatibilityProducer(_ClosedModel):
    producer_instance_ref: _Id
    producer_generation: _PositiveUint64

    @field_validator("producer_generation")
    @classmethod
    def validate_producer_generation(cls, value: str) -> str:
        if int(value) > MAXIMUM_UINT64:
            raise ValueError("producerGeneration exceeds uint64")
        return value


class CompatibilityCursorAuthority(_ClosedModel):
    prefix: _CursorPrefix


class AguiCompatibilityFixture(_ClosedModel):
    internal_thread_ref: _AgentThreadRef = "agent.thread:compatibility"
    internal_run_ref: _Id = "run.compatibility"
    internal_message_ref: _Id = "message.compatibility"
    internal_activity_ref: _Id = "activity.compatibility"
    source_event_prefix: _SourceEventPrefix = "agent.event.compatibility"
    started_at_ms: Annotated[
        int,
        Field(strict=True, ge=0, le=MAXIMUM_START_TIMESTAMP_MS),
    ] = 1_735_689_600_000
    text_deltas: Annotated[
        tuple[_TextDelta, ...],
        Field(min_length=2, max_length=MAXIMUM_TEXT_DELTAS),
    ] = ("Hello from ", "Kokoro.")

    @field_validator("text_deltas", mode="before")
    @classmethod
    def freeze_text_deltas(
        cls, value: tuple[str, ...] | list[str]
    ) -> tuple[str, ...]:
        return tuple(value)


class AguiCompatibilityFixtureInput(_ClosedModel):
    profile_revision: Literal[
        "kokoro-agent-presentation-compatibility-fixture.v1"
    ]
    scope: CompatibilityScope
    producer: CompatibilityProducer
    cursor_authority: CompatibilityCursorAuthority
    replay_page_limit: Annotated[int, Field(strict=True, ge=1, le=512)] = 2
    fixture: AguiCompatibilityFixture = Field(default_factory=AguiCompatibilityFixture)

    @model_validator(mode="after")
    def validate_profile(self) -> AguiCompatibilityFixtureInput:
        if self.profile_revision != PRESENTATION_COMPATIBILITY_FIXTURE_PROFILE:
            raise ValueError("fixture profile mismatch")
        return self


class CompatibilityRunBinding(_ClosedModel):
    internal_run_ref: _Id
    segment_ordinal: Literal[0]


class CompatibilityMessageBinding(_ClosedModel):
    internal_message_ref: _Id
    segment_ordinal: Literal[0]
    run_internal_run_ref: _Id
    run_segment_ordinal: Literal[0]


class CompatibilitySubmissionBinding(_ClosedModel):
    source_event_ref: _Id
    expected_event_ordinal: _Uint64
    internal_thread_ref: _AgentThreadRef
    event_type: CompatibilityEventType
    run: CompatibilityRunBinding
    message: CompatibilityMessageBinding | None = None

    @model_validator(mode="after")
    def reject_explicit_null_message(self) -> CompatibilitySubmissionBinding:
        if self.message is None and "message" in self.model_fields_set:
            raise ValueError("message must be absent rather than null")
        return self


class CompatibilitySubmission(_ClosedModel):
    submission_ref: _SubmissionRef
    envelope_base64: _Base64Envelope
    binding: CompatibilitySubmissionBinding


class SessionPresentationCompatibilityInput(_ClosedModel):
    profile_revision: Literal[
        "kokoro-session-presentation-compatibility-input.v1"
    ]
    scope: CompatibilityScope
    producer: CompatibilityProducer
    cursor_authority: CompatibilityCursorAuthority
    replay_page_limit: Annotated[int, Field(strict=True, ge=1, le=512)]
    submissions: Annotated[
        tuple[CompatibilitySubmission, ...],
        Field(min_length=7, max_length=5 + MAXIMUM_TEXT_DELTAS),
    ]

    @model_validator(mode="after")
    def validate_sequence(self) -> SessionPresentationCompatibilityInput:
        ordinals = tuple(
            submission.binding.expected_event_ordinal
            for submission in self.submissions
        )
        expected = tuple(str(index) for index in range(len(self.submissions)))
        if ordinals != expected:
            raise ValueError("submission ordinals are not contiguous")
        return self


def _submission_source(
    fixture: AguiCompatibilityFixture,
    *,
    ordinal: int,
    internal_message_ref: str | None,
) -> SubmissionSource:
    timestamp = fixture.started_at_ms + ordinal
    return SubmissionSource(
        source_event_ref=f"{fixture.source_event_prefix}.{ordinal}",
        event_ordinal=str(ordinal),
        recorded_at=canonical_recorded_at(timestamp),
        route=SubmissionRoute(
            internal_run_ref=fixture.internal_run_ref,
            internal_thread_ref=fixture.internal_thread_ref,
            **(
                {"internal_message_ref": internal_message_ref}
                if internal_message_ref is not None
                else {}
            ),
        ),
    )


def _official_events(
    fixture: AguiCompatibilityFixture,
) -> tuple[tuple[BaseEvent, str | None], ...]:
    events: list[tuple[BaseEvent, str | None]] = [
        (
            RunStartedEvent(
                thread_id=fixture.internal_thread_ref,
                run_id=fixture.internal_run_ref,
                timestamp=fixture.started_at_ms,
            ),
            None,
        ),
        (
            ActivitySnapshotEvent(
                message_id=fixture.internal_activity_ref,
                activity_type="kokoro.safe-summary.v1",
                content={
                    "partRef": "agent.compat.safe-summary.1",
                    "ownerVersion": "1",
                    "summary": "Compared safe alternatives.",
                    "status": "complete",
                    "updatedAt": canonical_recorded_at(fixture.started_at_ms + 1),
                },
                timestamp=fixture.started_at_ms + 1,
            ),
            fixture.internal_activity_ref,
        ),
        (
            TextMessageStartEvent(
                message_id=fixture.internal_message_ref,
                role="assistant",
                timestamp=fixture.started_at_ms + 2,
            ),
            fixture.internal_message_ref,
        ),
    ]
    events.extend(
        (
            TextMessageContentEvent(
                message_id=fixture.internal_message_ref,
                delta=delta,
                timestamp=fixture.started_at_ms + 3 + index,
            ),
            fixture.internal_message_ref,
        )
        for index, delta in enumerate(fixture.text_deltas)
    )
    events.extend(
        (
            (
                TextMessageEndEvent(
                    message_id=fixture.internal_message_ref,
                    timestamp=fixture.started_at_ms + 3 + len(fixture.text_deltas),
                ),
                fixture.internal_message_ref,
            ),
            (
                RunFinishedEvent(
                    thread_id=fixture.internal_thread_ref,
                    run_id=fixture.internal_run_ref,
                    timestamp=fixture.started_at_ms + 4 + len(fixture.text_deltas),
                    outcome=RunFinishedSuccessOutcome(),
                ),
                None,
            ),
        )
    )
    return tuple(events)


def _session_submission(
    submission: PresentationSubmission,
) -> CompatibilitySubmission:
    envelope = submission.envelope_bytes()
    if not envelope or len(envelope) > MAXIMUM_SUBMISSION_ENVELOPE_BYTES:
        raise CompatibilityProviderError("PRESENTATION_COMPAT_SUBMISSION_CAPACITY_EXCEEDED")
    source = submission.source
    route = source.route
    message = (
        CompatibilityMessageBinding(
            internal_message_ref=route.internal_message_ref,
            segment_ordinal=0,
            run_internal_run_ref=route.internal_run_ref,
            run_segment_ordinal=0,
        )
        if route.internal_message_ref is not None
        else None
    )
    return CompatibilitySubmission(
        submission_ref=submission.submission_ref,
        envelope_base64=base64.b64encode(envelope).decode("ascii"),
        binding=CompatibilitySubmissionBinding(
            source_event_ref=source.source_event_ref,
            expected_event_ordinal=source.event_ordinal,
            internal_thread_ref=route.internal_thread_ref,
            event_type=_compatibility_event_type(submission),
            run=CompatibilityRunBinding(
                internal_run_ref=route.internal_run_ref,
                segment_ordinal=0,
            ),
            **({"message": message} if message is not None else {}),
        ),
    )


def _compatibility_event_type(
    submission: PresentationSubmission,
) -> CompatibilityEventType:
    match submission.event.type:
        case "RUN_STARTED":
            return "RUN_STARTED"
        case "ACTIVITY_SNAPSHOT":
            return "ACTIVITY_SNAPSHOT"
        case "TEXT_MESSAGE_START":
            return "TEXT_MESSAGE_START"
        case "TEXT_MESSAGE_CONTENT":
            return "TEXT_MESSAGE_CONTENT"
        case "TEXT_MESSAGE_END":
            return "TEXT_MESSAGE_END"
        case "RUN_FINISHED":
            return "RUN_FINISHED"
        case _:
            raise CompatibilityProviderError("AGUI_COMPAT_EVENT_TYPE_INVALID")


def build_session_compatibility_input(
    source: AguiCompatibilityFixtureInput,
) -> SessionPresentationCompatibilityInput:
    """Build Session input only through official models and the production adapter."""

    try:
        fixture = source.fixture
        events = _official_events(fixture)
        submissions = tuple(
            _session_submission(
                build_submission(
                    event,
                    source=_submission_source(
                        fixture,
                        ordinal=ordinal,
                        internal_message_ref=internal_message_ref,
                    ),
                )
            )
            for ordinal, (event, internal_message_ref) in enumerate(events)
        )
        return SessionPresentationCompatibilityInput(
            profile_revision=SESSION_PRESENTATION_COMPATIBILITY_INPUT_PROFILE,
            scope=source.scope,
            producer=source.producer,
            cursor_authority=source.cursor_authority,
            replay_page_limit=source.replay_page_limit,
            submissions=submissions,
        )
    except (SubmissionProtocolError, ValidationError, ValueError) as error:
        raise CompatibilityProviderError("PRESENTATION_COMPAT_SUBMISSION_INVALID") from error


def read_bounded_regular_file(path: Path) -> bytes:
    """Read an immutable regular-file snapshot without following a symlink."""

    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise CompatibilityProviderError("AGUI_COMPAT_INPUT_FILE_INVALID") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size < 1:
            raise CompatibilityProviderError("AGUI_COMPAT_INPUT_FILE_INVALID")
        if before.st_size > MAXIMUM_INPUT_BYTES:
            raise CompatibilityProviderError("AGUI_COMPAT_INPUT_CAPACITY_EXCEEDED")
        remaining = before.st_size
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                raise CompatibilityProviderError("AGUI_COMPAT_INPUT_FILE_CHANGED")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise CompatibilityProviderError("AGUI_COMPAT_INPUT_FILE_CHANGED")
        after = os.fstat(descriptor)
        if (
            before.st_dev != after.st_dev
            or before.st_ino != after.st_ino
            or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
        ):
            raise CompatibilityProviderError("AGUI_COMPAT_INPUT_FILE_CHANGED")
        return b"".join(chunks)
    except OSError as error:
        raise CompatibilityProviderError("AGUI_COMPAT_INPUT_FILE_INVALID") from error
    finally:
        os.close(descriptor)


def run_cli(input_path: Path) -> str:
    try:
        source = AguiCompatibilityFixtureInput.model_validate_json(
            read_bounded_regular_file(input_path)
        )
    except (ValidationError, ValueError) as error:
        raise CompatibilityProviderError("AGUI_COMPAT_INPUT_INVALID") from error
    output = build_session_compatibility_input(source).model_dump_json(
        by_alias=True,
        exclude_none=True,
    )
    if len(output.encode("utf-8")) > MAXIMUM_OUTPUT_BYTES:
        raise CompatibilityProviderError("AGUI_COMPAT_OUTPUT_CAPACITY_EXCEEDED")
    return f"{output}\n"


def main(argv: tuple[str, ...] | None = None) -> int:
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 2 or arguments[0] != "--input" or not arguments[1]:
        sys.stderr.write("AGUI_COMPAT_USAGE_INVALID\n")
        return 2
    try:
        sys.stdout.write(run_cli(Path(arguments[1])))
    except CompatibilityProviderError as error:
        sys.stderr.write(f"{error}\n")
        return 1
    except Exception:
        sys.stderr.write("AGUI_COMPAT_PROVIDER_FAILED\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
