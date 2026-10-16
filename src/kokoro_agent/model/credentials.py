"""Worker-owned private model credentials, bound to exact System provider handles."""

from __future__ import annotations

import json
import os
import stat
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StringConstraints,
    field_validator,
)

_LIMIT = 131_072
_Reference = Annotated[
    str,
    StringConstraints(
        min_length=1, max_length=255, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$"
    ),
]


class ModelCredential(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    provider_id: Annotated[str, StringConstraints(min_length=1, max_length=255)] = (
        Field(repr=False)
    )
    secret_handle_ref: _Reference = Field(repr=False)
    api_key: SecretStr = Field(min_length=1, max_length=4096, repr=False)

    @field_validator("api_key")
    @classmethod
    def validate_key(cls, value: SecretStr) -> SecretStr:
        if any(not 33 <= ord(c) <= 126 for c in value.get_secret_value()):
            raise ValueError("invalid model credential")
        return value


class _CredentialSnapshot(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    version: Literal["1.0.0"]
    credentials: tuple[ModelCredential, ...] = Field(
        min_length=1, max_length=128, repr=False
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _reject_constant(_: str) -> object:
    raise ValueError


def _fingerprint(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def read_model_credentials(path: str) -> tuple[ModelCredential, ...]:
    """Read one bounded, protected startup snapshot; never retain input errors."""
    try:
        if not os.path.isabs(path) or os.path.normpath(path) != path:
            raise ValueError
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
        try:
            before = os.fstat(fd)
            if not (
                stat.S_ISREG(before.st_mode)
                and before.st_uid == os.geteuid()
                and stat.S_IMODE(before.st_mode) in {0o400, 0o600}
                and before.st_nlink == 1
                and 0 < before.st_size <= _LIMIT
            ):
                raise ValueError
            raw = bytearray()
            while len(raw) <= _LIMIT:
                chunk = os.read(fd, min(65536, _LIMIT + 1 - len(raw)))
                if not chunk:
                    break
                raw.extend(chunk)
            if (
                len(raw) != before.st_size
                or _fingerprint(before) != _fingerprint(os.fstat(fd))
                or _fingerprint(before)
                != _fingerprint(os.stat(path, follow_symlinks=False))
            ):
                raise ValueError
        finally:
            os.close(fd)
        json.loads(
            raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant
        )
        snapshot = _CredentialSnapshot.model_validate_json(raw, strict=True)
        seen: set[str] = set()
        for credential in snapshot.credentials:
            key = credential.api_key.get_secret_value()
            if credential.secret_handle_ref in seen or any(
                not 33 <= ord(c) <= 126 for c in key
            ):
                raise ValueError
            seen.add(credential.secret_handle_ref)
        return snapshot.credentials
    except (OSError, ValueError, UnicodeError, RecursionError):
        pass
    raise ValueError("model credentials unavailable")


def credential_for_provider(
    credentials: tuple[ModelCredential, ...], provider_id: str, secret_handle_ref: str
) -> SecretStr:
    """No implicit account, environment key, topology or handle fallback."""
    matches = [
        credential
        for credential in credentials
        if credential.provider_id == provider_id
        and credential.secret_handle_ref == secret_handle_ref
    ]
    if len(matches) != 1:
        raise ValueError("model credentials unavailable")
    return matches[0].api_key
