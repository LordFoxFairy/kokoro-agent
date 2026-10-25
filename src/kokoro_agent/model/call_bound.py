"""Canonical planned/execution-bound bytes for one immutable model attempt."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from uuid import UUID

import rfc8785
from pydantic import JsonValue, TypeAdapter

from kokoro_agent.distribution_assets import read_model_call_bound_assets
from kokoro_agent.model.capacity_guarantee import (
    load_capacity_guarantee,
    validate_capacity_request,
    require_capacity_time,
)
from kokoro_agent.model.call_artifacts import (
    encode_final_request_digest,
    encode_planned_artifact,
)
from kokoro_agent.model.call_models import ModelCallInputInvalid, PrepareModelCall
from kokoro_agent.protocol.model_call_bound_generated import (
    ModelCallBound,
    SOURCE_SHA256,
)
from kokoro_agent.protocol.model_call_capacity_guarantee_generated import (
    GUARANTEE_SHA256,
)
from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding

_JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate bound member")
        result[key] = value
    return result


def _depth(value: JsonValue, level: int = 0) -> None:
    if level > 16:
        raise ValueError("bound nesting exceeds limit")
    if isinstance(value, dict):
        for item in value.values():
            _depth(item, level + 1)
    elif isinstance(value, list):
        for item in value:
            _depth(item, level + 1)


def utc_millis(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ModelCallInputInvalid("bound clock is invalid")
    return value.astimezone(timezone.utc).replace(
        microsecond=value.microsecond // 1000 * 1000
    )


def instant_text(value: datetime) -> str:
    return utc_millis(value).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_bound(
    raw: bytes, *, planned_bytes: bytes, expected_digest: str | None = None
) -> ModelCallBound:
    """Validate original canonical bytes, then their closed joint identity."""
    try:
        if (
            type(raw) is not bytes
            or not 0 < len(raw) <= 65536
            or raw.startswith(b"\xef\xbb\xbf")
        ):
            raise ValueError
        value = _JSON.validate_python(
            json.loads(raw.decode("utf-8", errors="strict"), object_pairs_hook=_pairs),
            strict=True,
        )
        _depth(value)
        if rfc8785.dumps(value) != raw or (
            expected_digest is not None
            and hashlib.sha256(raw).hexdigest() != expected_digest
        ):
            raise ValueError
        bound = ModelCallBound.model_validate_json(raw, strict=True)
        # Literal validation can accept Python-equal bool/int values. Preserve
        # the original JSON types as well as their values before trusting it.
        if rfc8785.dumps(bound.model_dump(mode="json")) != raw:
            raise ValueError
        planned = PlannedBinding.model_validate_json(planned_bytes, strict=True)
        if rfc8785.dumps(planned.model_dump(mode="json")) != planned_bytes:
            raise ValueError
        attempt = UUID(bound.attempt_id)
        if (
            str(attempt) != bound.attempt_id
            or attempt.version != 5
            or bound.execution_bound_ref != "mcb_" + bound.attempt_id
            or bound.planned_binding_ref != "mpb_" + bound.attempt_id
            or hashlib.sha256(planned_bytes).hexdigest() != bound.planned_binding_digest
            or bound.capacity_guarantee_digest != GUARANTEE_SHA256
            or int(bound.total_tokens_max) != 128000 + int(bound.output_tokens_max)
            or planned.request_model != "gpt-4o-2024-08-06"
            or planned.provider_model_name != "gpt-4o-2024-08-06"
        ):
            raise ValueError
        sealed = datetime.fromisoformat(bound.sealed_at.replace("Z", "+00:00"))
        expiry = datetime.fromisoformat(bound.valid_until.replace("Z", "+00:00"))
        if expiry <= sealed or expiry > datetime(2027, 1, 14, tzinfo=timezone.utc):
            raise ValueError
        return bound
    except (ValueError, TypeError, RecursionError, UnicodeError, OverflowError):
        raise ModelCallInputInvalid("model-call execution bound is invalid") from None


def validate_prepare_bound(command: PrepareModelCall) -> int:
    """Revalidate actual final material, not a caller-supplied numeric cap."""
    try:
        m = command.capacity_request
        route = m.route
        for field in (
            "model_id",
            "provider_id",
            "revision_id",
            "provider_model_name",
            "request_model",
            "feature_key",
            "generation",
            "tenant_generation",
        ):
            planned_field = {"model_id": "model", "provider_id": "provider"}.get(
                field, field
            )
            if getattr(command.planned, planned_field) != getattr(route, field):
                raise ValueError
        if (
            command.planned.revision != str(route.revision)
            or command.planned.digest != route.digest
            or command.planned.label != route.label_key
        ):
            raise ValueError
        guarantee = load_capacity_guarantee()
        cap = validate_capacity_request(
            route=route,
            raw=m.raw,
            method=m.method,
            endpoint=m.endpoint,
            headers=m.headers,
            guarantee=guarantee,
        )
        _, pd = encode_planned_artifact(command.planned)
        if command.final_request_digest != encode_final_request_digest(
            endpoint=m.endpoint, planned_digest=pd, raw=m.raw
        ):
            raise ValueError
        return cap
    except (ValueError, TypeError, AttributeError):
        raise ModelCallInputInvalid(
            "model-call capacity preparation is invalid"
        ) from None


def build_bound(
    command: PrepareModelCall,
    *,
    ordinal: int,
    prepared_at: datetime,
    lease_expires_at: datetime,
) -> tuple[bytes, str, datetime]:
    cap = validate_prepare_bound(command)
    guarantee = load_capacity_guarantee()
    sealed = utc_millis(prepared_at)
    require_capacity_time(guarantee, sealed)
    expiry = min(
        utc_millis(lease_expires_at),
        datetime.fromisoformat(guarantee.sunset.replace("Z", "+00:00")),
    )
    if expiry <= sealed:
        raise ModelCallInputInvalid("model-call bound has no valid lifetime")
    _, pd = encode_planned_artifact(command.planned)
    # Complete System provenance comes from the installed finite schema.

    assets = read_model_call_bound_assets()
    if hashlib.sha256(assets["schema.json"]).hexdigest() != SOURCE_SHA256:
        raise ModelCallInputInvalid("model-call bound schema is invalid")
    schema = TypeAdapter(dict[str, JsonValue]).validate_json(
        assets["schema.json"], strict=True
    )
    properties = TypeAdapter(dict[str, dict[str, JsonValue]]).validate_python(
        schema["properties"], strict=True
    )
    value: dict[str, JsonValue] = {
        k: n["const"] for k, n in properties.items() if "const" in n
    }
    value.update(
        execution_bound_ref="mcb_" + str(command.attempt_id),
        planned_binding_ref="mpb_" + str(command.attempt_id),
        planned_binding_digest=pd,
        tenant_ref=command.request.execution_identity.tenant_ref,
        run_id=command.request.run_id,
        call_id=str(command.call_id),
        attempt_id=str(command.attempt_id),
        attempt_ordinal=str(ordinal),
        lease_owner=command.lease.owner,
        lease_generation=str(command.lease.generation),
        final_request_digest=command.final_request_digest,
        output_tokens_max=str(cap),
        total_tokens_max=str(128000 + cap),
        sealed_at=instant_text(sealed),
        valid_until=instant_text(expiry),
    )
    raw = rfc8785.dumps(value)
    digest = hashlib.sha256(raw).hexdigest()
    planned_raw, _ = encode_planned_artifact(command.planned)
    parse_bound(raw, planned_bytes=planned_raw, expected_digest=digest)
    return raw, digest, expiry
