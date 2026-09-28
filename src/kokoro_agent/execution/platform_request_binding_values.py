"""Strict value-domain projection for pinned Platform request bindings."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import re
from typing import TypeAlias

from pydantic import JsonValue, TypeAdapter, ValidationError

from kokoro_agent.execution.execution_proof_profile import (
    MAX_SAFE_INTEGER,
    canonical_json_bytes,
)
from kokoro_agent.generated.kokoro.common.v1 import common_pb
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as platform_pb


BINDING_VERSION = "1.0.0"
_LOWERCASE_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$")
_SOURCE_REF_PATTERN = re.compile(r"^skill:[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$")
_OBJECT_LIST = TypeAdapter(list[object])
_ECMASCRIPT_TRIM_CHARACTERS = (
    "\u0009\u000a\u000b\u000c\u000d\u0020\u00a0\u1680"
    "\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a"
    "\u2028\u2029\u202f\u205f\u3000\ufeff"
)

PlatformIdentity: TypeAlias = (
    platform_pb.SkillSeriesId
    | platform_pb.SkillId
    | platform_pb.SkillInstallationId
    | platform_pb.SkillSourceRef
    | platform_pb.McpConnectorId
    | platform_pb.McpServerId
    | platform_pb.McpConnectionId
    | platform_pb.McpAuthorizationId
    | platform_pb.McpInvocationGrant
)


class PlatformRequestBindingError(ValueError):
    """A typed Platform request cannot satisfy the pinned binding profile."""


@dataclass(frozen=True, slots=True, kw_only=True)
class ProjectedPlatformRequest:
    """Generated projection result before the single reviewed JCS encoder."""

    operation: str
    fq_method: str
    request_id: str
    request: dict[str, JsonValue]


def _utf8(value: object, *, label: str, nonempty: bool = False) -> str:
    if type(value) is not str or (nonempty and not value):
        raise PlatformRequestBindingError(f"{label} must be a valid string")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        raise PlatformRequestBindingError(f"{label} must be valid UTF-8") from None
    return value


def project_string(value: object, *, label: str) -> str:
    return _utf8(value, label=label)


def project_nonempty_string(value: object, *, label: str) -> str:
    return _utf8(value, label=label, nonempty=True)


def project_boolean(value: object, *, label: str) -> bool:
    if type(value) is not bool:
        raise PlatformRequestBindingError(f"{label} must be a boolean")
    return value


def project_safe_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PlatformRequestBindingError(f"{label} must be a safe integer")
    projected = int(value)
    if not -MAX_SAFE_INTEGER <= projected <= MAX_SAFE_INTEGER:
        raise PlatformRequestBindingError(f"{label} must be a safe integer")
    return projected


def project_array(values: object, *, label: str) -> list[str]:
    try:
        items = _OBJECT_LIST.validate_python(values, strict=True)
    except ValidationError:
        raise PlatformRequestBindingError(f"{label} must be an array")
    return [project_string(value, label=f"{label} item") for value in items]


def project_set(values: object, *, label: str) -> list[str]:
    projected = project_array(values, label=label)
    if any(not value.strip(_ECMASCRIPT_TRIM_CHARACTERS) for value in projected):
        raise PlatformRequestBindingError(f"{label} must not contain blank values")
    if len(set(projected)) != len(projected):
        raise PlatformRequestBindingError(f"{label} must not contain duplicates")
    return sorted(projected, key=lambda value: value.encode("utf-8"))


def project_presence(value: JsonValue | None) -> dict[str, JsonValue]:
    if value is None:
        return {"present": False}
    return {"present": True, "value": value}


def project_optional_string(
    value: object | None, *, label: str
) -> dict[str, JsonValue]:
    if value is None:
        return project_presence(None)
    return project_presence(project_string(value, label=label))


def project_optional_boolean(
    value: object | None, *, label: str
) -> dict[str, JsonValue]:
    if value is None:
        return project_presence(None)
    return project_presence(project_boolean(value, label=label))


def project_owner_scope(
    value: platform_pb.OwnerScope | None, *, label: str
) -> dict[str, JsonValue]:
    if value is None:
        return project_presence(None)
    if type(value) is not platform_pb.OwnerScope:
        raise PlatformRequestBindingError(f"{label} must be an OwnerScope message")
    return project_presence(
        {
            "kind": project_nonempty_string(value.kind, label=f"{label}.kind"),
            "id": project_nonempty_string(value.id, label=f"{label}.id"),
        }
    )


def project_command(
    value: common_pb.CommandIdentity | None, *, label: str
) -> dict[str, JsonValue]:
    if value is None:
        return project_presence(None)
    if type(value) is not common_pb.CommandIdentity:
        raise PlatformRequestBindingError(f"{label} must be a CommandIdentity message")
    command_id = project_nonempty_string(value.command_id, label=f"{label}.command_id")
    request_digest = project_string(
        value.request_digest, label=f"{label}.request_digest"
    )
    if _LOWERCASE_SHA256.fullmatch(request_digest) is None:
        raise PlatformRequestBindingError(
            f"{label}.request_digest must be lowercase SHA-256"
        )
    return project_presence(
        {"command_id": command_id, "request_digest": request_digest}
    )


def project_page(
    value: common_pb.PageRequest | None, *, label: str
) -> dict[str, JsonValue]:
    if value is None:
        return project_presence(None)
    if type(value) is not common_pb.PageRequest:
        raise PlatformRequestBindingError(f"{label} must be a PageRequest message")
    cursor = value.cursor if value.has_field("cursor") else None
    return project_presence(
        {
            "limit": project_safe_integer(value.limit, label=f"{label}.limit"),
            "cursor": project_optional_string(cursor, label=f"{label}.cursor"),
        }
    )


def project_identity(
    value: PlatformIdentity | None, *, wrapper: str, label: str
) -> dict[str, JsonValue]:
    if value is None:
        return project_presence(None)
    if not _identity_type_matches_wrapper(value, wrapper=wrapper):
        raise PlatformRequestBindingError(f"{label} must be a {wrapper} message")
    raw = project_nonempty_string(value.value, label=label)
    length = len(raw.encode("utf-8"))
    if wrapper == "SkillSourceRef":
        valid = 7 <= length <= 197 and _SOURCE_REF_PATTERN.fullmatch(raw) is not None
    elif wrapper == "McpInvocationGrant":
        valid = (
            length == 46
            and re.fullmatch(
                r"mcp-grant:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                raw,
            )
            is not None
        )
    else:
        valid = 1 <= length <= 191 and _IDENTITY_PATTERN.fullmatch(raw) is not None
    if not valid:
        raise PlatformRequestBindingError(f"{label} is outside the {wrapper} domain")
    return project_presence(raw)


def _identity_type_matches_wrapper(value: object, *, wrapper: str) -> bool:
    return (
        (wrapper == "SkillSeriesId" and type(value) is platform_pb.SkillSeriesId)
        or (wrapper == "SkillId" and type(value) is platform_pb.SkillId)
        or (
            wrapper == "SkillInstallationId"
            and type(value) is platform_pb.SkillInstallationId
        )
        or (wrapper == "SkillSourceRef" and type(value) is platform_pb.SkillSourceRef)
        or (wrapper == "McpConnectorId" and type(value) is platform_pb.McpConnectorId)
        or (wrapper == "McpServerId" and type(value) is platform_pb.McpServerId)
        or (wrapper == "McpConnectionId" and type(value) is platform_pb.McpConnectionId)
        or (
            wrapper == "McpAuthorizationId"
            and type(value) is platform_pb.McpAuthorizationId
        )
        or (
            wrapper == "McpInvocationGrant"
            and type(value) is platform_pb.McpInvocationGrant
        )
    )


def project_typed_arguments_digest(value: object, *, label: str) -> str:
    if type(value) is not bytes:
        raise PlatformRequestBindingError(f"{label} must be raw bytes")
    return hashlib.sha256(value).hexdigest()


def canonical_binding_bytes(value: dict[str, JsonValue]) -> bytes:
    """Use the Agent's one RFC 8785 encoder after strict projection."""

    try:
        return canonical_json_bytes(value)
    except ValueError as error:
        raise PlatformRequestBindingError(
            "request binding is not canonical JSON"
        ) from error


def project_metadata_bytes(value: bytes) -> str:
    """Expose the owner's no-pad byte projection for generated command reuse."""

    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")
