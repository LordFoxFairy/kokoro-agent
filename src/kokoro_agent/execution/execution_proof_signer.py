"""Ed25519 signer with fail-closed post-sign checks for execution-proof V1."""

from __future__ import annotations

from dataclasses import dataclass, field
import json

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import jwt

from kokoro_agent.execution.execution_proof_profile import (
    ALGORITHM,
    MAX_COMPACT_JWS_BYTES,
    ExecutionProofInput,
    build_claims,
    build_protected_header,
    canonical_json_bytes,
    decode_base64url_bytes,
    encode_base64url_bytes,
)


class ExecutionProofSigningError(RuntimeError):
    """Execution-proof signing or post-sign validation failed closed."""


class _CanonicalJsonEncoder(json.JSONEncoder):
    def encode(self, o: object) -> str:
        return canonical_json_bytes(o).decode("utf-8")


def _config_is_valid(issuer: object, kid: object, private_key: object) -> bool:
    return (
        type(issuer) is str
        and bool(issuer)
        and type(kid) is str
        and bool(kid)
        and isinstance(private_key, Ed25519PrivateKey)
    )


def _config_snapshot(value: object) -> ExecutionProofSignerConfig:
    if type(value) is not ExecutionProofSignerConfig or not _config_is_valid(
        value.issuer, value.kid, value.private_key
    ):
        raise ExecutionProofSigningError(
            "execution proof signer configuration is invalid"
        )
    return ExecutionProofSignerConfig(
        issuer=value.issuer,
        kid=value.kid,
        private_key=value.private_key,
    )


def _compact_bytes(value: object) -> bytes:
    if type(value) is not str:
        raise ExecutionProofSigningError("execution proof signing failed")
    try:
        return value.encode("ascii")
    except UnicodeEncodeError:
        raise ExecutionProofSigningError("execution proof signing failed") from None


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionProofSignerConfig:
    """Process-lifetime owner-controlled signer values."""

    issuer: str
    kid: str
    private_key: Ed25519PrivateKey = field(repr=False)

    def __post_init__(self) -> None:
        if not _config_is_valid(self.issuer, self.kid, self.private_key):
            raise ExecutionProofSigningError(
                "execution proof signer configuration is invalid"
            )


class ExecutionProofSigner:
    """Sign one exact compact proof from already trusted dynamic values."""

    __slots__ = ("_config",)

    def __init__(self, config: ExecutionProofSignerConfig) -> None:
        self._config = _config_snapshot(config)

    def issue_execution_proof(self, issue: ExecutionProofInput) -> str:
        """Build, sign, and independently verify one compact execution proof."""

        proof: str | None = None
        try:
            config = _config_snapshot(self._config)
            header = build_protected_header(config.kid)
            claims = build_claims(issue, config.issuer)
            header_bytes = canonical_json_bytes(header)
            claims_bytes = canonical_json_bytes(claims)
            compact = jwt.encode(
                claims,
                key=config.private_key,
                algorithm=ALGORITHM,
                headers=header,
                json_encoder=_CanonicalJsonEncoder,
            )
            proof = self._verify_signed_output(
                compact, header_bytes, claims_bytes, config.private_key
            )
        except Exception:
            pass
        if proof is None:
            raise ExecutionProofSigningError("execution proof signing failed")
        return proof

    def _verify_signed_output(
        self,
        compact: object,
        header_bytes: bytes,
        claims_bytes: bytes,
        private_key: Ed25519PrivateKey,
    ) -> str:
        compact_bytes = _compact_bytes(compact)
        if len(compact_bytes) > MAX_COMPACT_JWS_BYTES:
            raise ExecutionProofSigningError("execution proof signing failed")
        segments = compact_bytes.split(b".")
        if len(segments) != 3 or not all(segments):
            raise ExecutionProofSigningError("execution proof signing failed")
        protected_segment, claims_segment, signature_segment = segments
        protected = decode_base64url_bytes(protected_segment)
        payload = decode_base64url_bytes(claims_segment)
        signature = decode_base64url_bytes(signature_segment)
        if protected != header_bytes or payload != claims_bytes or len(signature) != 64:
            raise ExecutionProofSigningError("execution proof signing failed")
        if (
            encode_base64url_bytes(protected) != protected_segment
            or encode_base64url_bytes(payload) != claims_segment
            or encode_base64url_bytes(signature) != signature_segment
        ):
            raise ExecutionProofSigningError("execution proof signing failed")
        signing_input = protected_segment + b"." + claims_segment
        try:
            private_key.public_key().verify(signature, signing_input)
        except InvalidSignature:
            raise ExecutionProofSigningError("execution proof signing failed") from None
        return compact_bytes.decode("ascii")
