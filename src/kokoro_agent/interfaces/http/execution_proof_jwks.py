"""Immutable, fail-closed public Ed25519 JWK-set snapshot loading."""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
import hashlib
import hmac
import json
import os
import stat

import rfc8785
from pydantic import JsonValue, TypeAdapter

_MAX_PUBLIC_BYTES = 65_536
_JWK_FIELDS = frozenset({"kty", "crv", "use", "alg", "kid", "x"})
_FINAL_MODES = {0o400, 0o440, 0o444, 0o600, 0o640, 0o644}
_JSON_VALUE: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)
_JSON_OBJECT: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(dict[str, JsonValue])
_JSON_ARRAY: TypeAdapter[list[JsonValue]] = TypeAdapter(list[JsonValue])
_OPEN_SUPPORTS_DIR_FD = os.open in os.supports_dir_fd
_STAT_SUPPORTS_DIR_FD = os.stat in os.supports_dir_fd
_STAT_SUPPORTS_NOFOLLOW = os.stat in os.supports_follow_symlinks


class ExecutionProofJwksError(RuntimeError):
    """A public execution-proof descriptor is invalid."""


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


def _decode_base64url(value: object, *, length: int) -> bytes | None:
    if type(value) is not str or not value or "=" in value:
        return None
    try:
        encoded = value.encode("ascii")
        decoded = base64.b64decode(
            encoded + b"=" * (-len(encoded) % 4), altchars=b"-_", validate=True
        )
    except (UnicodeEncodeError, ValueError):
        return None
    return (
        decoded
        if len(decoded) == length
        and base64.urlsafe_b64encode(decoded).rstrip(b"=") == encoded
        else None
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class HttpExecutionProofConfig:
    public_jwks_file: str = field(repr=False)
    active_kid: str = field(repr=False)
    active_jwk_thumbprint_sha256: str = field(repr=False)

    def __post_init__(self) -> None:
        if not _http_values_valid(
            self.public_jwks_file,
            self.active_kid,
            self.active_jwk_thumbprint_sha256,
        ):
            raise ExecutionProofJwksError(
                "execution proof public configuration is invalid"
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionProofJwksState:
    available: bool
    body: bytes | None = field(default=None, repr=False)
    content_type: str = "application/jwk-set+json"

    def __post_init__(self) -> None:
        body_valid = False
        if type(self.body) is bytes and 1 <= len(self.body) <= _MAX_PUBLIC_BYTES:
            try:
                body_valid = _canonical_body(self.body) == self.body
            except Exception:
                body_valid = False
        valid = (
            type(self.available) is bool
            and type(self.content_type) is str
            and self.content_type == "application/jwk-set+json"
            and (
                (self.available and body_valid)
                or (not self.available and self.body is None)
            )
        )
        if not valid:
            raise ExecutionProofJwksError("execution proof JWKS state is invalid")

    @classmethod
    def unavailable(cls) -> ExecutionProofJwksState:
        return cls(available=False)


def _http_values_valid(path: object, kid: object, thumbprint: object) -> bool:
    return (
        _canonical_absolute_path(path)
        and _exact_text(kid)
        and _decode_base64url(thumbprint, length=32) is not None
    )


def _snapshot(value: object) -> tuple[str, str, bytes]:
    if type(value) is not HttpExecutionProofConfig:
        raise ValueError
    path = value.public_jwks_file
    kid = value.active_kid
    thumbprint = value.active_jwk_thumbprint_sha256
    digest = _decode_base64url(thumbprint, length=32)
    if not _http_values_valid(path, kid, thumbprint) or digest is None:
        raise ValueError
    return path, kid, digest


def _native_flags() -> tuple[int, int]:
    values: dict[str, int] = {}
    for name in ("O_RDONLY", "O_CLOEXEC", "O_NOFOLLOW", "O_NONBLOCK", "O_DIRECTORY"):
        value = getattr(os, name, None)
        if type(value) is not int:
            raise ValueError
        values[name] = value
    if not _OPEN_SUPPORTS_DIR_FD or not _STAT_SUPPORTS_DIR_FD:
        raise ValueError
    if not _STAT_SUPPORTS_NOFOLLOW:
        raise ValueError
    directory = (
        values["O_RDONLY"]
        | values["O_CLOEXEC"]
        | values["O_NOFOLLOW"]
        | values["O_DIRECTORY"]
    )
    final = (
        values["O_RDONLY"]
        | values["O_CLOEXEC"]
        | values["O_NOFOLLOW"]
        | values["O_NONBLOCK"]
    )
    return directory, final


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


def _safe_directory(value: os.stat_result, euid: int) -> bool:
    return (
        stat.S_ISDIR(value.st_mode)
        and value.st_uid in {0, euid}
        and stat.S_IMODE(value.st_mode) & 0o022 == 0
    )


def _safe_file(value: os.stat_result, euid: int) -> bool:
    return (
        stat.S_ISREG(value.st_mode)
        and value.st_uid in {0, euid}
        and stat.S_IMODE(value.st_mode) in _FINAL_MODES
        and 1 <= value.st_size <= _MAX_PUBLIC_BYTES
    )


def _read_bounded(fd: int) -> bytes:
    chunks: list[bytes] = []
    remaining = _MAX_PUBLIC_BYTES + 1
    while remaining:
        value = os.read(fd, remaining)
        if not value:
            break
        chunks.append(value)
        remaining -= len(value)
    return b"".join(chunks)


def _reject_duplicate_members(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _strings_are_unicode_scalars(value: JsonValue) -> bool:
    if type(value) is str:
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            return False
        return True
    if type(value) is list:
        return all(_strings_are_unicode_scalars(item) for item in value)
    if type(value) is dict:
        return all(
            _strings_are_unicode_scalars(key) and _strings_are_unicode_scalars(item)
            for key, item in value.items()
        )
    return True


def _jwk(value: JsonValue) -> dict[str, str]:
    document = _JSON_OBJECT.validate_python(value, strict=True)
    if frozenset(document) != _JWK_FIELDS:
        raise ValueError
    kty = document["kty"]
    crv = document["crv"]
    use = document["use"]
    alg = document["alg"]
    kid = document["kid"]
    x = document["x"]
    if (
        type(kty) is not str
        or type(crv) is not str
        or type(use) is not str
        or type(alg) is not str
        or type(kid) is not str
        or type(x) is not str
    ):
        raise ValueError
    if (
        kty != "OKP"
        or crv != "Ed25519"
        or use != "sig"
        or alg != "EdDSA"
        or not _exact_text(kid)
        or _decode_base64url(x, length=32) is None
    ):
        raise ValueError
    return {"kty": kty, "crv": crv, "use": use, "alg": alg, "kid": kid, "x": x}


def _jwk_thumbprint(value: dict[str, str]) -> bytes:
    canonical = rfc8785.dumps(
        {"crv": value["crv"], "kty": value["kty"], "x": value["x"]}
    )
    return hashlib.sha256(canonical).digest()


def _parse_keys(raw: bytes) -> list[dict[str, str]]:
    if raw.startswith(b"\xef\xbb\xbf"):
        raise ValueError
    text = raw.decode("utf-8", errors="strict")
    parsed = json.loads(text, object_pairs_hook=_reject_duplicate_members)
    document = _JSON_OBJECT.validate_python(
        _JSON_VALUE.validate_python(parsed, strict=True), strict=True
    )
    if not _strings_are_unicode_scalars(document) or frozenset(document) != frozenset(
        {"keys"}
    ):
        raise ValueError
    values = _JSON_ARRAY.validate_python(document["keys"], strict=True)
    if not values:
        raise ValueError
    keys = [_jwk(value) for value in values]
    kids = [key["kid"] for key in keys]
    if len(set(kids)) != len(kids):
        raise ValueError
    return keys


def _canonical_body(raw: bytes) -> bytes:
    keys = _parse_keys(raw)
    keys.sort(key=lambda key: key["kid"].encode("utf-8"))
    body = rfc8785.dumps({"keys": keys})
    if len(body) > _MAX_PUBLIC_BYTES:
        raise ValueError
    return body


def _parse(raw: bytes, active_kid: str, active_digest: bytes) -> bytes:
    keys = _parse_keys(raw)
    active = [key for key in keys if key["kid"] == active_kid]
    if len(active) != 1 or not hmac.compare_digest(
        _jwk_thumbprint(active[0]), active_digest
    ):
        raise ValueError
    return _canonical_body(raw)


def _load(value: object) -> ExecutionProofJwksState:
    path, active_kid, active_digest = _snapshot(value)
    directory_flags, file_flags = _native_flags()
    components = path[1:].split("/")
    euid = os.geteuid()
    descriptors: list[int] = []
    close_failed = False
    try:
        current = os.open("/", directory_flags)
        descriptors.append(current)
        if not _safe_directory(os.fstat(current), euid):
            raise ValueError
        for component in components[:-1]:
            current = os.open(component, directory_flags, dir_fd=current)
            descriptors.append(current)
            if not _safe_directory(os.fstat(current), euid):
                raise ValueError
        parent = current
        final = os.open(components[-1], file_flags, dir_fd=parent)
        descriptors.append(final)
        before = os.fstat(final)
        if not _safe_file(before, euid):
            raise ValueError
        raw = _read_bounded(final)
        after = os.fstat(final)
        named = os.stat(components[-1], dir_fd=parent, follow_symlinks=False)
        if (
            len(raw) != before.st_size
            or len(raw) > _MAX_PUBLIC_BYTES
            or _stat_tuple(before) != _stat_tuple(after)
            or (named.st_dev, named.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise ValueError
    finally:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except Exception:
                close_failed = True
    if close_failed:
        raise ValueError
    return ExecutionProofJwksState(
        available=True, body=_parse(raw, active_kid, active_digest)
    )


def load_execution_proof_jwks(
    config: HttpExecutionProofConfig,
) -> ExecutionProofJwksState:
    """Return an all-or-nothing immutable public ring snapshot."""

    state: ExecutionProofJwksState | None = None
    failed = False
    try:
        state = _load(config)
    except Exception:
        failed = True
    if failed or state is None:
        return ExecutionProofJwksState.unavailable()
    return state


__all__ = [
    "ExecutionProofJwksError",
    "ExecutionProofJwksState",
    "HttpExecutionProofConfig",
    "load_execution_proof_jwks",
]
