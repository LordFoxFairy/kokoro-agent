"""Worker-only private Ed25519 material loading for execution-proof signing."""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
import hashlib
import hmac
import os
import stat

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import rfc8785

from kokoro_agent.execution.execution_proof_signer import (
    ExecutionProofSigner,
    ExecutionProofSignerConfig,
)

_MAX_PRIVATE_BYTES = 16_384
_ALLOWED_OUTER = b"\x09\x0a\x0d\x20"
_BEGIN = b"-----BEGIN PRIVATE KEY-----"
_END = b"-----END PRIVATE KEY-----"
_CHALLENGE = b"kokoro-agent-execution-proof-key-check-v1"


class ExecutionProofKeyError(RuntimeError):
    """Private execution-proof material could not be loaded safely."""


def _exact_text(value: object) -> bool:
    if type(value) is not str or not value:
        return False
    try:
        rfc8785.dumps(value)
    except (rfc8785.CanonicalizationError, UnicodeError):
        return False
    return True


def _canonical_absolute_path(value: object) -> bool:
    if type(value) is not str or not value.startswith("/") or value == "/":
        return False
    if "\x00" in value or "//" in value:
        return False
    return all(part not in {"", ".", ".."} for part in value[1:].split("/"))


def _decode_digest(value: object) -> bytes | None:
    if type(value) is not str or not value or "=" in value:
        return None
    try:
        encoded = value.encode("ascii")
        decoded = base64.b64decode(
            encoded + b"=" * (-len(encoded) % 4), altchars=b"-_", validate=True
        )
    except (UnicodeEncodeError, ValueError):
        return None
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=")
    return decoded if len(decoded) == 32 and canonical == encoded else None


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerExecutionProofConfig:
    """Strict process-lifetime descriptor; every field is deliberately secret in repr."""

    issuer: str = field(repr=False)
    private_key_file: str = field(repr=False)
    active_kid: str = field(repr=False)
    active_jwk_thumbprint_sha256: str = field(repr=False)

    def __post_init__(self) -> None:
        if not _worker_values_valid(
            self.issuer,
            self.private_key_file,
            self.active_kid,
            self.active_jwk_thumbprint_sha256,
        ):
            raise ExecutionProofKeyError(
                "execution proof private key configuration is invalid"
            )


def _worker_values_valid(
    issuer: object, path: object, kid: object, thumbprint: object
) -> bool:
    return (
        _exact_text(issuer)
        and _canonical_absolute_path(path)
        and _exact_text(kid)
        and _decode_digest(thumbprint) is not None
    )


def _snapshot(value: object) -> tuple[str, str, str, bytes]:
    if type(value) is not WorkerExecutionProofConfig:
        raise ValueError
    issuer = value.issuer
    path = value.private_key_file
    kid = value.active_kid
    thumbprint = value.active_jwk_thumbprint_sha256
    digest = _decode_digest(thumbprint)
    if not _worker_values_valid(issuer, path, kid, thumbprint) or digest is None:
        raise ValueError
    return issuer, path, kid, digest


def _open_flags() -> int:
    values: list[int] = []
    for name in ("O_RDONLY", "O_CLOEXEC", "O_NOFOLLOW", "O_NONBLOCK"):
        value = getattr(os, name, None)
        if type(value) is not int:
            raise ValueError
        values.append(value)
    flags = 0
    for value in values:
        flags |= value
    return flags


def _stat_tuple(value: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_uid,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _valid_private_stat(value: os.stat_result) -> bool:
    return (
        stat.S_ISREG(value.st_mode)
        and value.st_uid == os.geteuid()
        and stat.S_IMODE(value.st_mode) in {0o400, 0o600}
        and 1 <= value.st_size <= _MAX_PRIVATE_BYTES
    )


def _read_bounded(fd: int, limit: int) -> bytes:
    chunks: list[bytes] = []
    remaining = limit
    while remaining:
        chunk = os.read(fd, remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _pem_body(value: bytes) -> bytes:
    stripped = value.strip(_ALLOWED_OUTER)
    if value[: len(value) - len(value.lstrip(_ALLOWED_OUTER))].strip(_ALLOWED_OUTER):
        raise ValueError
    if value[len(value.rstrip(_ALLOWED_OUTER)) :].strip(_ALLOWED_OUTER):
        raise ValueError
    if stripped.count(_BEGIN) != 1 or stripped.count(_END) != 1:
        raise ValueError
    if not stripped.startswith(_BEGIN) or not stripped.endswith(_END):
        raise ValueError
    if any(byte not in _ALLOWED_OUTER for byte in value[: value.find(_BEGIN)]):
        raise ValueError
    after = value.find(_END) + len(_END)
    if any(byte not in _ALLOWED_OUTER for byte in value[after:]):
        raise ValueError
    return stripped


def _thumbprint(private_key: Ed25519PrivateKey) -> bytes:
    raw = private_key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )
    x = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    canonical = rfc8785.dumps({"crv": "Ed25519", "kty": "OKP", "x": x})
    return hashlib.sha256(canonical).digest()


def _load(config: object) -> ExecutionProofSigner:
    issuer, path, kid, configured_digest = _snapshot(config)
    flags = _open_flags()
    fd: int | None = None
    try:
        fd = os.open(path, flags)
        before = os.fstat(fd)
        if not _valid_private_stat(before):
            raise ValueError
        raw = _read_bounded(fd, _MAX_PRIVATE_BYTES + 1)
        after = os.fstat(fd)
        if (
            len(raw) != before.st_size
            or len(raw) > _MAX_PRIVATE_BYTES
            or _stat_tuple(before) != _stat_tuple(after)
        ):
            raise ValueError
    finally:
        if fd is not None:
            os.close(fd)
    pem = _pem_body(raw)
    key = serialization.load_pem_private_key(pem, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError
    derived_digest = _thumbprint(key)
    if not hmac.compare_digest(configured_digest, derived_digest):
        raise ValueError
    signature = key.sign(_CHALLENGE)
    key.public_key().verify(signature, _CHALLENGE)
    return ExecutionProofSigner(
        ExecutionProofSignerConfig(issuer=issuer, kid=kid, private_key=key)
    )


def load_execution_proof_signer(
    config: WorkerExecutionProofConfig,
) -> ExecutionProofSigner:
    """Load one immutable signer, replacing every boundary error with a stable error."""

    signer: ExecutionProofSigner | None = None
    failed = False
    try:
        signer = _load(config)
    except Exception:
        failed = True
    if failed or signer is None:
        raise ExecutionProofKeyError(
            "execution proof private key is unavailable"
        ) from None
    return signer


__all__ = [
    "ExecutionProofKeyError",
    "WorkerExecutionProofConfig",
    "load_execution_proof_signer",
]
