"""Exact runtime values and canonical encoding for execution-proof V1."""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
import re

from pydantic import JsonValue, TypeAdapter, ValidationError
import rfc8785

from kokoro_agent.protocol import IdentityRef


CONTRACT_VERSION = "1.0.0"
TYPE = "kokoro-agent-execution+jwt"
ALGORITHM = "EdDSA"
AUDIENCE = "https://kokoro.dev/resources/iam-execution-authorization"
MAX_SAFE_INTEGER = 9_007_199_254_740_991
MAX_COMPACT_JWS_BYTES = 16 * 1024
HEADER_FIELDS = frozenset({"typ", "alg", "kid"})
CLAIM_FIELDS = frozenset(
    {
        "contract_version",
        "iss",
        "aud",
        "tenant_ref",
        "actor",
        "subject",
        "run_id",
        "execution_session_id",
        "lease_generation",
        "operation",
        "request_binding_sha256",
        "iat",
        "exp",
        "jti",
    }
)
_OPERATION = re.compile(r"^[a-z][a-z0-9_.]{0,127}$")
_BINDING = re.compile(r"^[0-9a-f]{64}$")
_BASE64URL = re.compile(rb"^[A-Za-z0-9_-]+$")
_JSON_VALUE: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


class ExecutionProofProfileError(ValueError):
    """The proposed dynamic claims do not satisfy the exact V1 profile."""


def _non_empty(value: object) -> bool:
    return type(value) is str and bool(value)


def _safe_integer(value: object, *, minimum: int) -> bool:
    return type(value) is int and minimum <= value <= MAX_SAFE_INTEGER


def _identity_snapshot(value: object) -> IdentityRef:
    if type(value) is not IdentityRef:
        raise ExecutionProofProfileError("execution proof identity is invalid")
    kind = value.kind
    opaque_ref = value.opaque_ref
    if type(kind) is not str or not _non_empty(opaque_ref):
        raise ExecutionProofProfileError("execution proof identity is invalid")
    if kind == "user":
        return IdentityRef(kind="user", opaque_ref=opaque_ref)
    if kind == "project":
        return IdentityRef(kind="project", opaque_ref=opaque_ref)
    if kind == "service":
        return IdentityRef(kind="service", opaque_ref=opaque_ref)
    raise ExecutionProofProfileError("execution proof identity is invalid")


def _operation_is_valid(value: object) -> bool:
    return type(value) is str and _OPERATION.fullmatch(value) is not None


def _binding_is_valid(value: object) -> bool:
    return type(value) is str and _BINDING.fullmatch(value) is not None


def _jti_bytes(value: object) -> bytes:
    if type(value) is not str:
        raise ExecutionProofProfileError("execution proof jti is invalid")
    return decode_base64url(value)


def encode_base64url(value: bytes) -> str:
    """Encode bytes as canonical unpadded base64url."""

    return encode_base64url_bytes(value).decode("ascii")


def encode_base64url_bytes(value: bytes) -> bytes:
    """Encode bytes as canonical unpadded ASCII base64url bytes."""

    return base64.urlsafe_b64encode(value).rstrip(b"=")


def decode_base64url(value: str) -> bytes:
    """Decode only canonical unpadded base64url."""

    if type(value) is not str:
        raise ExecutionProofProfileError("execution proof base64url is not canonical")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        raise ExecutionProofProfileError(
            "execution proof base64url is not canonical"
        ) from None
    return decode_base64url_bytes(encoded)


def decode_base64url_bytes(value: bytes) -> bytes:
    """Decode only canonical unpadded base64url ASCII bytes."""

    if (
        type(value) is not bytes
        or not value
        or b"=" in value
        or _BASE64URL.fullmatch(value) is None
    ):
        raise ExecutionProofProfileError("execution proof base64url is not canonical")
    try:
        decoded = base64.b64decode(
            value + b"=" * (-len(value) % 4), altchars=b"-_", validate=True
        )
    except (binascii.Error, ValueError):
        raise ExecutionProofProfileError(
            "execution proof base64url is not canonical"
        ) from None
    if encode_base64url_bytes(decoded) != value:
        raise ExecutionProofProfileError("execution proof base64url is not canonical")
    return decoded


def canonical_json_bytes(value: object) -> bytes:
    """Return JCS UTF-8 bytes without normalizing string values."""

    try:
        json_value = _JSON_VALUE.validate_python(value, strict=True)
        return rfc8785.dumps(json_value)
    except (ValidationError, rfc8785.CanonicalizationError, UnicodeError):
        raise ExecutionProofProfileError(
            "execution proof value is not canonical JSON"
        ) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionProofInput:
    """Dynamic trusted values supplied later by the run-scoped supplier."""

    tenant_ref: str
    actor: IdentityRef
    subject: IdentityRef
    run_id: str
    execution_session_id: str
    lease_generation: int
    operation: str
    request_binding_sha256: str
    iat: int
    exp: int
    jti: str

    def __post_init__(self) -> None:
        strings = (self.tenant_ref, self.run_id, self.execution_session_id)
        if not all(_non_empty(value) for value in strings):
            raise ExecutionProofProfileError("execution proof identity is invalid")
        actor = _identity_snapshot(self.actor)
        subject = _identity_snapshot(self.subject)
        if not _safe_integer(self.lease_generation, minimum=1):
            raise ExecutionProofProfileError("execution proof numeric claim is invalid")
        if not _safe_integer(self.iat, minimum=0) or not _safe_integer(
            self.exp, minimum=0
        ):
            raise ExecutionProofProfileError("execution proof numeric claim is invalid")
        if not 0 < self.exp - self.iat <= 60:
            raise ExecutionProofProfileError("execution proof time claims are invalid")
        if not _operation_is_valid(self.operation):
            raise ExecutionProofProfileError("execution proof operation is invalid")
        if not _binding_is_valid(self.request_binding_sha256):
            raise ExecutionProofProfileError(
                "execution proof request binding is invalid"
            )
        try:
            decoded_jti = _jti_bytes(self.jti)
        except ExecutionProofProfileError:
            raise ExecutionProofProfileError("execution proof jti is invalid") from None
        if len(decoded_jti) != 16:
            raise ExecutionProofProfileError("execution proof jti is invalid")
        object.__setattr__(self, "actor", actor)
        object.__setattr__(self, "subject", subject)


def snapshot_execution_proof_input(issue: object) -> ExecutionProofInput:
    """Rebuild and revalidate an exact immutable signing-input snapshot."""

    if type(issue) is not ExecutionProofInput:
        raise ExecutionProofProfileError("execution proof input is invalid")
    return ExecutionProofInput(
        tenant_ref=issue.tenant_ref,
        actor=_identity_snapshot(issue.actor),
        subject=_identity_snapshot(issue.subject),
        run_id=issue.run_id,
        execution_session_id=issue.execution_session_id,
        lease_generation=issue.lease_generation,
        operation=issue.operation,
        request_binding_sha256=issue.request_binding_sha256,
        iat=issue.iat,
        exp=issue.exp,
        jti=issue.jti,
    )


def build_protected_header(kid: str) -> dict[str, JsonValue]:
    if not _non_empty(kid):
        raise ExecutionProofProfileError("execution proof key identifier is invalid")
    return {"typ": TYPE, "alg": ALGORITHM, "kid": kid}


def build_claims(issue: object, issuer: str) -> dict[str, JsonValue]:
    if not _non_empty(issuer):
        raise ExecutionProofProfileError("execution proof signing identity is invalid")
    snapshot = snapshot_execution_proof_input(issue)
    claims: dict[str, JsonValue] = {
        "contract_version": CONTRACT_VERSION,
        "iss": issuer,
        "aud": AUDIENCE,
        "tenant_ref": snapshot.tenant_ref,
        "actor": {"kind": snapshot.actor.kind, "opaque_ref": snapshot.actor.opaque_ref},
        "subject": {
            "kind": snapshot.subject.kind,
            "opaque_ref": snapshot.subject.opaque_ref,
        },
        "run_id": snapshot.run_id,
        "execution_session_id": snapshot.execution_session_id,
        "lease_generation": snapshot.lease_generation,
        "operation": snapshot.operation,
        "request_binding_sha256": snapshot.request_binding_sha256,
        "iat": snapshot.iat,
        "exp": snapshot.exp,
        "jti": snapshot.jti,
    }
    if frozenset(claims) != CLAIM_FIELDS:
        raise ExecutionProofProfileError("execution proof claim shape is invalid")
    return claims
