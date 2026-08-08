"""Strict Root R1 Presentation submission envelope and canonical encoding."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Annotated, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    TypeAdapter,
    model_validator,
)
from pydantic.alias_generators import to_camel

from kokoro_agent.presentation.profile import (
    CLOSED_ACTIVITY_CLASSES,
    ClosedActivityEvent,
    ClosedAguiEvent,
    ClosedRunFinishedEvent,
    ClosedRunStartedEvent,
    ClosedTextContentEvent,
    ClosedTextEndEvent,
    ClosedTextStartEvent,
    MAX_OFFICIAL_EVENT_JSON_BYTES,
)

PRESENTATION_SUBMISSION_CONTRACT_REVISION = "kokoro.presentation.submission.v1"
MAX_UINT64 = (1 << 64) - 1
MAX_SAFE_INTEGER = (1 << 53) - 1
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_JSON_VALUE_ADAPTER: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)

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
_Sha256Digest = Annotated[
    str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")
]
_SubmissionRef = Annotated[
    str,
    StringConstraints(pattern=r"^presentation\.submission:sha256:[0-9a-f]{64}$"),
]
_Uint64Decimal = Annotated[
    str, StringConstraints(pattern=r"^(0|[1-9][0-9]{0,19})$")
]
_CanonicalUtcMilliseconds = Annotated[
    str,
    StringConstraints(
        pattern=(
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"\.[0-9]{3}Z$"
        )
    ),
]


class _SubmissionModel(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        alias_generator=to_camel,
        populate_by_name=True,
        revalidate_instances="always",
    )


class SubmissionRoute(_SubmissionModel):
    internal_run_ref: _Id
    internal_thread_ref: _AgentThreadRef
    internal_message_ref: _Id | None = None

    @model_validator(mode="after")
    def reject_explicit_null_message_ref(self) -> SubmissionRoute:
        if (
            self.internal_message_ref is None
            and "internal_message_ref" in self.model_fields_set
        ):
            raise ValueError("internalMessageRef must be absent rather than null")
        return self


class SubmissionSource(_SubmissionModel):
    """Caller-owned durable source coordinates; the adapter invents none of them."""

    source_event_ref: _Id
    event_ordinal: _Uint64Decimal
    recorded_at: _CanonicalUtcMilliseconds
    route: SubmissionRoute

    @model_validator(mode="after")
    def validate_source(self) -> SubmissionSource:
        if int(self.event_ordinal) > MAX_UINT64:
            raise ValueError("event ordinal exceeds uint64")
        recorded_at_milliseconds(self.recorded_at)
        return self


def canonical_recorded_at(timestamp_ms: int) -> str:
    if timestamp_ms < 0 or timestamp_ms > MAX_SAFE_INTEGER:
        raise ValueError("event timestamp is not a non-negative safe integer")
    try:
        moment = _EPOCH + timedelta(milliseconds=timestamp_ms)
    except OverflowError as error:
        raise ValueError("event timestamp is outside canonical UTC range") from error
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{moment.microsecond // 1000:03d}Z"


def recorded_at_milliseconds(recorded_at: str) -> int:
    try:
        moment = datetime.strptime(recorded_at, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=UTC
        )
    except ValueError as error:
        raise ValueError("recordedAt is not canonical UTC milliseconds") from error
    delta = moment - _EPOCH
    milliseconds = (
        delta.days * 86_400_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )
    if canonical_recorded_at(milliseconds) != recorded_at:
        raise ValueError("recordedAt is not canonical UTC milliseconds")
    return milliseconds


def _jcs_string(value: str) -> str:
    try:
        value.encode("utf-8")
        value.encode("utf-16-be")
    except UnicodeEncodeError as error:
        raise ValueError("JCS strings must contain Unicode scalar values") from error
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _jcs_text(value: JsonValue) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        if abs(value) > MAX_SAFE_INTEGER:
            raise ValueError("JCS integer exceeds interoperable safe range")
        return str(value)
    if isinstance(value, float):
        raise ValueError("JCS submission subset forbids floating-point values")
    if isinstance(value, str):
        return _jcs_string(value)
    if isinstance(value, list):
        return "[" + ",".join(_jcs_text(item) for item in value) + "]"
    keys = sorted(value, key=lambda key: key.encode("utf-16-be"))
    return "{" + ",".join(
        f"{_jcs_string(key)}:{_jcs_text(value[key])}" for key in keys
    ) + "}"


def canonical_json_bytes(value: JsonValue) -> bytes:
    """Encode the closed Presentation JSON subset with RFC 8785 key ordering."""
    validated = _JSON_VALUE_ADAPTER.validate_python(value)
    return _jcs_text(validated).encode()


def event_jcs_bytes(event: ClosedAguiEvent) -> bytes:
    dumped = event.model_dump(mode="json", by_alias=True, exclude_none=True)
    encoded = canonical_json_bytes(dumped)
    if len(encoded) > MAX_OFFICIAL_EVENT_JSON_BYTES:
        raise ValueError("official event JCS exceeds byte limit")
    return encoded


def event_digest(event: ClosedAguiEvent) -> str:
    return f"sha256:{hashlib.sha256(event_jcs_bytes(event)).hexdigest()}"


def submission_identity(
    *,
    contract_revision: str,
    source: SubmissionSource,
    event_digest_value: str,
) -> str:
    route = source.route
    material = "\0".join(
        (
            contract_revision,
            route.internal_run_ref,
            route.internal_thread_ref,
            route.internal_message_ref or "",
            source.source_event_ref,
            source.event_ordinal,
            source.recorded_at,
            event_digest_value,
        )
    ).encode()
    return f"presentation.submission:sha256:{hashlib.sha256(material).hexdigest()}"


_MessageScopedEvent: TypeAlias = (
    ClosedTextStartEvent
    | ClosedTextContentEvent
    | ClosedTextEndEvent
    | ClosedActivityEvent
)


class PresentationSubmission(_SubmissionModel):
    """Canonical Agent-to-Session input; Session remains admission owner."""

    contract_revision: Annotated[
        str, Field(pattern=r"^kokoro\.presentation\.submission\.v1$")
    ]
    submission_ref: _SubmissionRef
    source: SubmissionSource
    event_digest: _Sha256Digest
    event: ClosedAguiEvent

    @model_validator(mode="after")
    def validate_material(self) -> PresentationSubmission:
        if self.contract_revision != PRESENTATION_SUBMISSION_CONTRACT_REVISION:
            raise ValueError("PRESENTATION_SUBMISSION_REVISION_INVALID")
        if recorded_at_milliseconds(self.source.recorded_at) != self.event.timestamp:
            raise ValueError("PRESENTATION_SUBMISSION_TIMESTAMP_INVALID")
        route = self.source.route
        if isinstance(self.event, ClosedRunStartedEvent | ClosedRunFinishedEvent):
            if (
                self.event.run_id != route.internal_run_ref
                or self.event.thread_id != route.internal_thread_ref
            ):
                raise ValueError("PRESENTATION_SUBMISSION_ROUTE_INVALID")
            if route.internal_message_ref is not None:
                raise ValueError("PRESENTATION_SUBMISSION_SEGMENT_INVALID")
        elif isinstance(
            self.event,
            (ClosedTextStartEvent, ClosedTextContentEvent, ClosedTextEndEvent),
        ) or isinstance(self.event, CLOSED_ACTIVITY_CLASSES):
            message_event: _MessageScopedEvent = self.event
            if message_event.message_id != route.internal_message_ref:
                raise ValueError("PRESENTATION_SUBMISSION_SEGMENT_INVALID")
        elif route.internal_message_ref is not None:
            raise ValueError("PRESENTATION_SUBMISSION_SEGMENT_INVALID")
        if event_digest(self.event) != self.event_digest:
            raise ValueError("PRESENTATION_SUBMISSION_EVENT_DIGEST_INVALID")
        expected = submission_identity(
            contract_revision=self.contract_revision,
            source=self.source,
            event_digest_value=self.event_digest,
        )
        if self.submission_ref != expected:
            raise ValueError("PRESENTATION_SUBMISSION_REF_INVALID")
        return self

    def envelope_bytes(self) -> bytes:
        return canonical_json_bytes(
            self.model_dump(mode="json", by_alias=True, exclude_none=True)
        )


__all__ = [
    "MAX_SAFE_INTEGER",
    "MAX_UINT64",
    "PRESENTATION_SUBMISSION_CONTRACT_REVISION",
    "PresentationSubmission",
    "SubmissionRoute",
    "SubmissionSource",
    "canonical_json_bytes",
    "canonical_recorded_at",
    "event_digest",
    "event_jcs_bytes",
    "recorded_at_milliseconds",
    "submission_identity",
]
