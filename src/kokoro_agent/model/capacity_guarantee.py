"""One provider-documented, Agent-enforced text capacity profile; never a grant."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from collections.abc import Sequence

import rfc8785
from pydantic import JsonValue, TypeAdapter, ValidationError

from kokoro_agent.clients.system import ResolvedModel
from kokoro_agent.distribution_assets import read_capacity_guarantee_assets
from kokoro_agent.model.call_models import ModelCallInputInvalid
from kokoro_agent.model.request_profile import validate_request_profile
from kokoro_agent.protocol.model_call_capacity_guarantee_generated import (
    GUARANTEE_SHA256,
    ModelCallCapacityGuarantee,
    ModelCallCapacityEvidence,
)

MAX_GUARANTEE_BYTES = 65536
ENDPOINT = "https://api.openai.com/v1/chat/completions"
MODEL = "gpt-4o-2024-08-06"
_JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)
_OBJECT = TypeAdapter(dict[str, JsonValue])


class CapacityGuaranteeInvalid(ValueError):
    """Safe error; never includes supplied bytes, provider keys or parser causes."""


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise CapacityGuaranteeInvalid("capacity guarantee is invalid")
        result[key] = value
    return result


def _document(raw: bytes) -> JsonValue:
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_GUARANTEE_BYTES:
        raise CapacityGuaranteeInvalid("capacity guarantee is invalid")
    try:
        return _JSON.validate_python(
            json.loads(raw.decode("utf-8", errors="strict"), object_pairs_hook=_pairs),
            strict=True,
        )
    except (ValueError, UnicodeError, RecursionError):
        raise CapacityGuaranteeInvalid("capacity guarantee is invalid") from None


def parse_capacity_guarantee(raw: bytes) -> ModelCallCapacityGuarantee:
    """Decode only exact canonical bytes of the formally generated identity."""
    try:
        value = _document(raw)
        if (
            rfc8785.dumps(value) != raw
            or hashlib.sha256(raw).hexdigest() != GUARANTEE_SHA256
        ):
            raise CapacityGuaranteeInvalid("capacity guarantee is invalid")
        return ModelCallCapacityGuarantee.model_validate_json(raw, strict=True)
    except (ValueError, UnicodeError, RecursionError, ValidationError):
        raise CapacityGuaranteeInvalid("capacity guarantee is invalid") from None


def load_capacity_guarantee() -> ModelCallCapacityGuarantee:
    """Explicit per-use distribution read; no import-time I/O or fallback layout."""
    try:
        assets = read_capacity_guarantee_assets()
        schema = _document(assets["schema.json"])
        manifest = _document(assets["manifest.json"])
        if not isinstance(manifest, dict) or not isinstance(schema, dict):
            raise CapacityGuaranteeInvalid("capacity guarantee is invalid")
        sources = manifest.get("source_sha256")
        if not isinstance(sources, dict):
            raise CapacityGuaranteeInvalid("capacity guarantee is invalid")
        for name in ("schema.json", "guarantee.json", "vectors.json", "evidence.json"):
            key = f"contract/model-call-capacity-guarantee/v1/{name}"
            if sources.get(key) != hashlib.sha256(assets[name]).hexdigest():
                raise CapacityGuaranteeInvalid("capacity guarantee is invalid")
        guarantee = parse_capacity_guarantee(
            rfc8785.dumps(_document(assets["guarantee.json"]))
        )
        evidence_raw = rfc8785.dumps(_document(assets["evidence.json"]))
        if hashlib.sha256(evidence_raw).hexdigest() != guarantee.evidence_sha256:
            raise CapacityGuaranteeInvalid("capacity guarantee is invalid")
        ModelCallCapacityEvidence.model_validate_json(evidence_raw, strict=True)
        norm_raw = assets["normalization-manifest.json"]
        normalization = _OBJECT.validate_python(_document(norm_raw), strict=True)
        if (
            sources.get("contract/usage/v1/manifest.json")
            != hashlib.sha256(norm_raw).hexdigest()
            or normalization.get("owner") != guarantee.normalization_owner
            or normalization.get("artifact_version") != guarantee.normalization_version
            or normalization.get("aggregate_sha256")
            != guarantee.normalization_manifest_aggregate_sha256
        ):
            raise CapacityGuaranteeInvalid("capacity guarantee is invalid")
        return guarantee
    except (ValueError, OSError, UnicodeError, RecursionError):
        raise CapacityGuaranteeInvalid("capacity guarantee is invalid") from None


def matches_capacity_route(route: ResolvedModel) -> bool:
    """Only an authenticated exact direct route can select this narrow branch."""
    return (
        route.connection.protocol == "openai_chat_completions"
        and route.connection.connection_type == "direct"
        and route.connection.base_url == "https://api.openai.com/v1"
        and route.request_model == MODEL
        and route.provider_model_name == MODEL
        and route.context_window == 128000
        and 1 <= route.max_output_tokens <= 16384
    )


def require_capacity_time(guarantee: ModelCallCapacityGuarantee, now: datetime) -> None:
    """Caller supplies a trusted clock; later prepare uses its one DB instant."""
    sunset = datetime.fromisoformat(guarantee.sunset.replace("Z", "+00:00"))
    if now.tzinfo is None or now.astimezone(timezone.utc) >= sunset:
        raise CapacityGuaranteeInvalid("capacity guarantee is unavailable")


def validate_capacity_request(
    *,
    route: ResolvedModel,
    raw: bytes,
    method: str,
    endpoint: str,
    headers: Sequence[tuple[str, str]],
    guarantee: ModelCallCapacityGuarantee,
) -> int:
    """Return the bound's final output cap, not an authorization/send permit."""
    try:
        parse_capacity_guarantee(rfc8785.dumps(guarantee.model_dump(mode="json")))
        if not matches_capacity_route(route) or endpoint != guarantee.endpoint:
            raise ModelCallInputInvalid("model capacity request is invalid")
        validate_request_profile(
            raw=raw,
            method=method,
            endpoint=endpoint,
            expected_endpoint=guarantee.endpoint,
            headers=headers,
            model_name=guarantee.request_model,
            max_output_tokens=min(route.max_output_tokens, guarantee.max_output_tokens),
        )
        body = _OBJECT.validate_json(raw, strict=True)
        if set(body) != set(guarantee.request_fields):
            raise ModelCallInputInvalid("model capacity request is invalid")
        if (
            body["stream"] is not False
            or body["store"] is not False
            or body["service_tier"] != guarantee.service_tier
        ):
            raise ModelCallInputInvalid("model capacity request is invalid")
        messages = body["messages"]
        if not isinstance(messages, list) or not 1 <= len(messages) <= 16384:
            raise ModelCallInputInvalid("model capacity request is invalid")
        for message in messages:
            if (
                not isinstance(message, dict)
                or set(message) != {"role", "content"}
                or type(message["role"]) is not str
                or message["role"] not in guarantee.roles
                or type(message["content"]) is not str
            ):
                raise ModelCallInputInvalid("model capacity request is invalid")
        cap = body["max_completion_tokens"]
        assert type(cap) is int
        return cap
    except (ValueError, TypeError, KeyError, AssertionError):
        raise ModelCallInputInvalid("model capacity request is invalid") from None
