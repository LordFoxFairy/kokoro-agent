"""Strict, pure decoding and immutable revision checks for attempt evidence."""

from __future__ import annotations

import hashlib
import json

import rfc8785
from pydantic import JsonValue, TypeAdapter

from kokoro_agent.protocol.model_usage_generated import (
    MAX_JSON_BYTES,
    MAX_JSON_DEPTH,
    ModelUsageEvidence,
    NormalizedUsage,
    ReportedCount,
    TokenUsageV1,
)

_OBJECT = TypeAdapter(dict[str, JsonValue])
_MAPPING = TypeAdapter(dict[str, object])
_ARRAY = TypeAdapter(list[object])
_COUNTS = (
    "input_total",
    "output_total",
    "total",
    "input_cache_read",
    "input_cache_write",
    "output_reasoning",
)
_DOMAIN = b"kokoro-agent:model-usage-evidence:v1\n"


def _members(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate evidence JSON member")
        result[key] = value
    return result


def _constant(value: str) -> object:
    raise ValueError("nonfinite evidence JSON value")


def _tree(value: object, depth: int = 1) -> None:
    if depth > MAX_JSON_DEPTH:
        raise ValueError("evidence nesting exceeds its bound")
    if isinstance(value, str):
        value.encode("utf-8", errors="strict")
        if any(ord(character) < 32 or ord(character) == 127 for character in value):
            raise ValueError("evidence contains a control character")
    elif isinstance(value, dict):
        for key, child in _MAPPING.validate_python(value, strict=True).items():
            _tree(key, depth)
            _tree(child, depth + 1)
    elif isinstance(value, list):
        for child in _ARRAY.validate_python(value, strict=True):
            _tree(child, depth + 1)


def _count(usage: TokenUsageV1, name: str) -> int | None:
    observation = getattr(usage, name)
    return int(observation.count) if isinstance(observation, ReportedCount) else None


def _validate_usage(usage: TokenUsageV1) -> None:
    if any(getattr(usage, name).state == "not_applicable" for name in _COUNTS):
        raise ValueError("this source profile has no not-applicable count")
    unsupported = usage.profile_assessment.state == "unsupported"
    if usage.input_cache_write.state != "unreported":
        raise ValueError("cache-write is not verified by this profile")
    if usage.source_protocol == "ollama_openai_chat_completions" and (
        usage.input_cache_read.state != "unreported"
        or usage.output_reasoning.state != "unreported"
    ):
        raise ValueError("Ollama detail counts are not verified by this profile")
    input_total = _count(usage, "input_total")
    output_total = _count(usage, "output_total")
    total = _count(usage, "total")
    if input_total is not None and output_total is not None and total is not None:
        if input_total + output_total != total:
            raise ValueError("token total does not equal input plus output")
        state = "known_zero" if total == 0 else "known_nonzero"
    else:
        state = "unknown"
    if usage.totals_state != state:
        raise ValueError("token knowledge disagrees with reported observations")
    for child, parent in (
        (_count(usage, "input_cache_read"), input_total),
        (_count(usage, "output_reasoning"), output_total),
    ):
        if child is not None and parent is not None and child > parent:
            raise ValueError("token subset exceeds its inclusive total")
    classification = "unknown" if unsupported or state == "unknown" else "partial"
    if usage.classification_state != classification:
        raise ValueError("classification disagrees with source knowledge")


def parse_evidence(raw: bytes) -> ModelUsageEvidence:
    """Validate original bytes, exact generated fields, semantics and domain digest."""
    if len(raw) > MAX_JSON_BYTES:
        raise ValueError("evidence JSON exceeds its byte budget")
    try:
        text = raw.decode("utf-8", errors="strict")
        decoded: object = json.loads(
            text, object_pairs_hook=_members, parse_constant=_constant
        )
        _tree(decoded)
        document = _OBJECT.validate_python(decoded, strict=True)
        evidence = ModelUsageEvidence.model_validate(document, strict=True)
    except (UnicodeError, RecursionError) as error:
        raise ValueError("evidence is not bounded valid UTF-8 JSON") from error
    if evidence.revision == "1":
        if evidence.predecessor is not None:
            raise ValueError("first revision cannot have a predecessor")
    elif evidence.predecessor is None:
        raise ValueError("later revision requires a predecessor")
    if evidence.predecessor is not None and (
        evidence.predecessor.event_id == evidence.identity.event_id
        or evidence.predecessor.digest == evidence.digest
    ):
        raise ValueError("evidence predecessor cannot refer to itself")
    dispatched = evidence.execution.dispatch_started_at is not None
    if dispatched and evidence.authorization_observation.state != "granted":
        raise ValueError("dispatched attempt requires its granted observation")
    if evidence.execution.outcome == "not_dispatched" and dispatched:
        raise ValueError("not-dispatched outcome contradicts dispatch observation")
    if evidence.execution.outcome in {"completed", "failed"} and not dispatched:
        raise ValueError("executed outcome requires a dispatch observation")
    if isinstance(evidence.usage, NormalizedUsage):
        _validate_usage(evidence.usage.value)
        if not dispatched:
            raise ValueError(
                "provider usage cannot be attached to an undispatched attempt"
            )
    canonical = dict(document)
    canonical.pop("digest")
    expected = hashlib.sha256(_DOMAIN + rfc8785.dumps(canonical)).hexdigest()
    if evidence.digest != expected:
        raise ValueError("evidence digest does not bind its canonical facts")
    return evidence


def _knowledge(previous: ModelUsageEvidence, incoming: ModelUsageEvidence) -> None:
    if previous.usage.raw_usage_digest is not None and (
        previous.usage.raw_usage_digest != incoming.usage.raw_usage_digest
    ):
        raise ValueError("observed raw usage digest cannot be replaced or discarded")
    if previous.actual.state == "verified" and incoming.actual != previous.actual:
        raise ValueError("verified attribution cannot be replaced")
    if previous.execution.outcome != "unknown" and (
        previous.execution.outcome != incoming.execution.outcome
    ):
        raise ValueError("known execution outcome cannot be replaced")
    if previous.execution.prepared_at != incoming.execution.prepared_at:
        raise ValueError("original preparation time is immutable")
    if previous.execution.dispatch_started_at is not None and (
        previous.execution.dispatch_started_at != incoming.execution.dispatch_started_at
    ):
        raise ValueError("observed dispatch cannot be replaced")
    if previous.execution.seal and not incoming.execution.seal:
        raise ValueError("successor cannot unseal an observation")
    before_auth = previous.authorization_observation
    after_auth = incoming.authorization_observation
    if before_auth.state in {"granted", "denied"} and before_auth != after_auth:
        raise ValueError("known authorization observation cannot be replaced")
    if before_auth.state == "requested_unknown":
        if (
            after_auth.state == "not_requested"
            or before_auth.request_ref != after_auth.request_ref
        ):
            raise ValueError("authorization request identity cannot be replaced")
    if isinstance(previous.usage, NormalizedUsage):
        if not isinstance(incoming.usage, NormalizedUsage):
            raise ValueError("reported token observations cannot be discarded")
        before = previous.usage.value
        after = incoming.usage.value
        if before.profile_assessment != after.profile_assessment:
            raise ValueError("known source profile assessment cannot be replaced")
        if (
            before.profile_id != after.profile_id
            or before.source_protocol != after.source_protocol
            or before.aggregation_mode != after.aggregation_mode
            or before.unit != after.unit
        ):
            raise ValueError("source usage interpretation is immutable")
        for name in _COUNTS:
            prior = getattr(before, name)
            if isinstance(prior, ReportedCount) and prior != getattr(after, name):
                raise ValueError("known token observation conflicts with successor")


def validate_successor(
    previous: ModelUsageEvidence, incoming: ModelUsageEvidence
) -> None:
    """Validate a replay or adjacent revision; never perform I/O or latest-wins."""
    # Public boundary values must still carry valid digests after caller mutation.
    parse_evidence(previous.model_dump_json().encode())
    parse_evidence(incoming.model_dump_json().encode())
    previous_identity = previous.identity.model_dump(exclude={"event_id"})
    incoming_identity = incoming.identity.model_dump(exclude={"event_id"})
    if previous_identity != incoming_identity or previous.planned != incoming.planned:
        raise ValueError("usage revision changed its immutable binding")
    if incoming.revision == previous.revision:
        if incoming != previous:
            raise ValueError("usage revision replay conflicts")
        return
    if int(incoming.revision) != int(previous.revision) + 1:
        raise ValueError("usage revision is not contiguous")
    predecessor = incoming.predecessor
    if (
        predecessor is None
        or predecessor.event_id != previous.identity.event_id
        or predecessor.digest != previous.digest
        or incoming.identity.event_id == previous.identity.event_id
    ):
        raise ValueError("usage revision predecessor does not match")
    _knowledge(previous, incoming)
