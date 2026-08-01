"""Strict immutable envelope for one internal Agent AG-UI event candidate."""

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
    AGUI_CANDIDATE_PROFILE_REVISION,
    ClosedActivityBase,
    ClosedAguiEvent,
    ClosedRunFinishedEvent,
    ClosedRunStartedEvent,
    ClosedTextContentEvent,
    ClosedTextEndEvent,
    ClosedTextStartEvent,
    MAX_OFFICIAL_EVENT_JSON_BYTES,
)

_Id = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]
_Sha256Digest = Annotated[
    str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")
]
_CandidateRef = Annotated[
    str,
    StringConstraints(pattern=r"^agui_candidate:sha256:[0-9a-f]{64}$"),
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

MAX_UINT64 = (1 << 64) - 1
MAX_SAFE_INTEGER = (1 << 53) - 1
_JSON_VALUE_ADAPTER: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class _StrictCandidateModel(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        alias_generator=to_camel,
        populate_by_name=True,
    )


class AgentAguiCandidateRoute(_StrictCandidateModel):
    internal_run_ref: _Id
    internal_thread_ref: _Id
    internal_message_ref: _Id | None = None

    @model_validator(mode="after")
    def reject_explicit_null_message_ref(self) -> AgentAguiCandidateRoute:
        if (
            self.internal_message_ref is None
            and "internal_message_ref" in self.model_fields_set
        ):
            raise ValueError("internalMessageRef must be absent rather than null")
        return self


class AgentAguiCandidateSource(_StrictCandidateModel):
    """Caller-owned durable source coordinates; the adapter invents none of them."""

    source_event_ref: _Id
    source_ordinal: _Uint64Decimal
    recorded_at: _CanonicalUtcMilliseconds
    route: AgentAguiCandidateRoute

    @model_validator(mode="after")
    def validate_source(self) -> AgentAguiCandidateSource:
        if int(self.source_ordinal) > MAX_UINT64:
            raise ValueError("source ordinal exceeds uint64")
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
        raise ValueError("JCS candidate subset forbids floating-point values")
    if isinstance(value, str):
        return _jcs_string(value)
    if isinstance(value, list):
        return "[" + ",".join(_jcs_text(item) for item in value) + "]"
    keys = sorted(value, key=lambda key: key.encode("utf-16-be"))
    return "{" + ",".join(
        f"{_jcs_string(key)}:{_jcs_text(value[key])}" for key in keys
    ) + "}"


def event_jcs_bytes(event: ClosedAguiEvent) -> bytes:
    dumped = event.model_dump(mode="json", by_alias=True, exclude_none=True)
    value = _JSON_VALUE_ADAPTER.validate_python(dumped)
    encoded = _jcs_text(value).encode("utf-8")
    if len(encoded) > MAX_OFFICIAL_EVENT_JSON_BYTES:
        raise ValueError("official event JCS exceeds byte limit")
    return encoded


def event_digest(event: ClosedAguiEvent) -> str:
    return f"sha256:{hashlib.sha256(event_jcs_bytes(event)).hexdigest()}"


def candidate_identity(
    *,
    source: AgentAguiCandidateSource,
    event_digest_value: str,
) -> str:
    route = source.route
    material = "\0".join(
        (
            AGUI_CANDIDATE_PROFILE_REVISION,
            route.internal_run_ref,
            route.internal_thread_ref,
            route.internal_message_ref or "",
            source.source_event_ref,
            source.source_ordinal,
            source.recorded_at,
            event_digest_value,
        )
    ).encode("utf-8")
    return f"agui_candidate:sha256:{hashlib.sha256(material).hexdigest()}"


_MessageScopedEvent: TypeAlias = (
    ClosedTextStartEvent | ClosedTextContentEvent | ClosedTextEndEvent | ClosedActivityBase
)


class AgentAguiEventCandidate(_StrictCandidateModel):
    """Agent-internal candidate; Session remains projection/cursor/SSE owner."""

    profile_revision: Annotated[
        str,
        Field(pattern=r"^kokoro-agent-agui-candidate\.v1$"),
    ]
    candidate_ref: _CandidateRef
    source: AgentAguiCandidateSource
    event_digest: _Sha256Digest
    event: ClosedAguiEvent

    @model_validator(mode="after")
    def validate_material(self) -> AgentAguiEventCandidate:
        if self.profile_revision != AGUI_CANDIDATE_PROFILE_REVISION:
            raise ValueError("candidate profile revision mismatch")
        event_timestamp = self.event.timestamp
        if recorded_at_milliseconds(self.source.recorded_at) != event_timestamp:
            raise ValueError("recordedAt does not equal event timestamp")
        route = self.source.route
        if isinstance(self.event, ClosedRunStartedEvent | ClosedRunFinishedEvent):
            if (
                self.event.run_id != route.internal_run_ref
                or self.event.thread_id != route.internal_thread_ref
            ):
                raise ValueError("official event route mismatch")
            if route.internal_message_ref is not None:
                raise ValueError("run event must not have internal message ref")
        elif isinstance(
            self.event,
            (
                ClosedTextStartEvent,
                ClosedTextContentEvent,
                ClosedTextEndEvent,
            ),
        ) or isinstance(self.event, ClosedActivityBase):
            message_event: _MessageScopedEvent = self.event
            if message_event.message_id != route.internal_message_ref:
                raise ValueError("official event message route mismatch")
        elif route.internal_message_ref is not None:
            raise ValueError("unscoped event must not have internal message ref")
        expected_digest = event_digest(self.event)
        if expected_digest != self.event_digest:
            raise ValueError("official event digest mismatch")
        expected_ref = candidate_identity(
            source=self.source,
            event_digest_value=self.event_digest,
        )
        if expected_ref != self.candidate_ref:
            raise ValueError("candidate identity mismatch")
        return self


__all__ = [
    "AgentAguiCandidateRoute",
    "AgentAguiCandidateSource",
    "AgentAguiEventCandidate",
    "MAX_SAFE_INTEGER",
    "MAX_UINT64",
    "candidate_identity",
    "canonical_recorded_at",
    "event_digest",
    "event_jcs_bytes",
    "recorded_at_milliseconds",
]
