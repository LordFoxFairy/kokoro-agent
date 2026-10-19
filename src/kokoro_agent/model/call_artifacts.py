"""Pure canonical artifacts and frozen recipe checks for durable model calls."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping

import rfc8785
from pydantic import JsonValue, TypeAdapter

from kokoro_agent.model.call_models import (
    AttemptBinding,
    ModelCallInputInvalid,
    PrepareModelCall,
)
from kokoro_agent.model.request_profile import REQUEST_PROFILE
from kokoro_agent.protocol import RunRequest
from kokoro_agent.protocol.model_usage_v2_generated import (
    AuthorizationGranted,
    AuthorizationNotRequested,
    AuthorizationRequested,
    IdentityRef,
    Lease,
    Opaque,
    PlannedBinding,
)

_RAW_OBJECT = TypeAdapter(dict[str, object])
_RAW_ARRAY = TypeAdapter(list[object])
_RAW_TUPLE = TypeAdapter(tuple[object, ...])


def _canonical(value: object) -> tuple[bytes, str]:
    """Return bounded RFC8785 bytes and their exact SHA-256 digest."""
    try:
        raw = rfc8785.dumps(_json_value(value))
    except (TypeError, ValueError) as error:
        raise ModelCallInputInvalid("model-call canonical value is invalid") from error
    if not 0 < len(raw) <= 65_536:
        raise ModelCallInputInvalid("model-call canonical value exceeds its bound")
    return raw, _digest(raw)


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def encode_planned_artifact(planned: PlannedBinding) -> tuple[bytes, str]:
    return _canonical(planned.model_dump(mode="json"))


def encode_final_request_digest(
    *, endpoint: str, planned_digest: str, raw: bytes
) -> str:
    """Bind already validated final bytes, not a reconstruction of model input."""
    envelope = {
        "version": 1,
        "request_profile": REQUEST_PROFILE,
        "method": "POST",
        "endpoint": endpoint,
        "planned_digest": planned_digest,
        "body_bytes": len(raw),
        "body_sha256": _digest(raw),
    }
    return _digest(b"kokoro-agent:final-http-request:v1\n" + rfc8785.dumps(envelope))


def encode_not_requested_artifact() -> tuple[bytes, str]:
    return _canonical(
        AuthorizationNotRequested(state="not_requested").model_dump(mode="json")
    )


def encode_requested_artifact(request_ref: Opaque) -> tuple[bytes, str]:
    return _canonical(
        AuthorizationRequested(
            state="requested_unknown", request_ref=request_ref
        ).model_dump(mode="json")
    )


def encode_granted_artifact(
    request_ref: Opaque, admission_ref: Opaque
) -> tuple[bytes, str]:
    return _canonical(
        AuthorizationGranted(
            state="granted", request_ref=request_ref, admission_ref=admission_ref
        ).model_dump(mode="json")
    )


def strict_granted_artifact(raw: bytes) -> tuple[AuthorizationGranted, str]:
    try:
        granted = AuthorizationGranted.model_validate_json(raw, strict=True)
    except ValueError as error:
        raise ModelCallInputInvalid(
            "granted authorization artifact is invalid"
        ) from error
    canonical, digest = _canonical(granted.model_dump(mode="json"))
    if canonical != raw:
        raise ModelCallInputInvalid("granted authorization artifact is not canonical")
    return granted, digest


def encode_attempt_binding(
    command: PrepareModelCall, request: RunRequest, ordinal: int
) -> tuple[bytes, str]:
    identity = request.execution_identity
    binding = AttemptBinding(
        tenant_ref=identity.tenant_ref,
        actor=IdentityRef(
            kind=identity.actor.kind, opaque_ref=identity.actor.opaque_ref
        ),
        subject=IdentityRef(
            kind=identity.subject.kind, opaque_ref=identity.subject.opaque_ref
        ),
        identity_assertion_ref=identity.identity_assertion_ref,
        run_id=request.run_id,
        session_id=request.session_id,
        call_id=command.call_id,
        attempt_id=command.attempt_id,
        attempt_ordinal=str(ordinal),
        lease=Lease(
            owner=command.lease.owner, generation=str(command.lease.generation)
        ),
        input_digest=_canonical(request.input.model_dump(mode="json"))[1],
        peer_path=command.peer_path,
    )
    return _canonical(binding.model_dump(mode="json"))


def recipe_feature_for_peer_path(recipe_bytes: bytes, path: tuple[str, ...]) -> str:
    """Prove that a peer path is declared by the frozen canonical recipe."""
    try:
        recipe = _json_object(_RAW_OBJECT.validate_json(recipe_bytes, strict=True))
        feature = _require_object(recipe, "feature")
        agent_items = _require_list(feature, "agents")
        agents = {
            _require_str(item, "key"): item
            for value in agent_items
            if (item := _as_object(value)) is not None
        }
        if len(agents) != len(agent_items):
            raise ValueError
        handoffs = {
            (_require_string(edge[0]), _require_string(edge[1]))
            for value in _require_list(feature, "handoffs")
            if len(edge := _require_array(value)) == 2
        }
        entry_agent = _require_str(feature, "entry_agent")
        if not path or path[0] != entry_agent or path[0] not in agents:
            raise ValueError
        current = path[0]
        for target in path[1:]:
            agent = agents.get(current)
            if agent is None:
                raise ValueError
            subagents = {
                _require_str(item, "name")
                for value in _require_list(agent, "subagents")
                if (item := _as_object(value)) is not None
            }
            if target in subagents:
                current = target
                continue
            if (current, target) not in handoffs or target not in agents:
                raise ValueError
            current = target
        return _require_str(feature, "key")
    except (KeyError, TypeError, ValueError) as error:
        raise ModelCallInputInvalid("peer path is outside the frozen recipe") from error


def _json_value(value: object, depth: int = 1) -> JsonValue:
    if depth > 16:
        raise ValueError("model-call JSON exceeds its depth bound")
    if value is None or isinstance(value, (bool, str, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("model-call JSON number is not finite")
        return value
    if isinstance(value, list):
        return [
            _json_value(item, depth + 1)
            for item in _RAW_ARRAY.validate_python(value, strict=True)
        ]
    if isinstance(value, tuple):
        return [
            _json_value(item, depth + 1)
            for item in _RAW_TUPLE.validate_python(value, strict=True)
        ]
    if isinstance(value, Mapping):
        return {
            key: _json_value(item, depth + 1)
            for key, item in _RAW_OBJECT.validate_python(value, strict=True).items()
        }
    raise ValueError("model-call value is not JSON")


def _json_object(value: Mapping[str, object]) -> dict[str, JsonValue]:
    normalized = _json_value(value)
    if not isinstance(normalized, dict):
        raise ValueError("model-call recipe root is not an object")
    return normalized


def _as_object(value: JsonValue) -> dict[str, JsonValue] | None:
    return value if isinstance(value, dict) else None


def _require_object(value: Mapping[str, JsonValue], key: str) -> dict[str, JsonValue]:
    result = _as_object(value.get(key))
    if result is None:
        raise ValueError
    return result


def _require_list(value: Mapping[str, JsonValue], key: str) -> list[JsonValue]:
    result = value.get(key)
    if not isinstance(result, list):
        raise ValueError
    return result


def _require_array(value: JsonValue) -> list[JsonValue]:
    if not isinstance(value, list):
        raise ValueError
    return value


def _require_string(value: JsonValue) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError
    return value


def _require_str(value: Mapping[str, JsonValue], key: str) -> str:
    return _require_string(value.get(key))


__all__ = [
    "encode_final_request_digest",
    "encode_attempt_binding",
    "encode_granted_artifact",
    "encode_not_requested_artifact",
    "encode_planned_artifact",
    "encode_requested_artifact",
    "recipe_feature_for_peer_path",
    "strict_granted_artifact",
]
