"""Worker-only current Platform service credentials; no exchange or cache."""

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
)

_LIMIT = 1_048_576
_Text = Annotated[str, StringConstraints(min_length=1)]


class CredentialError(RuntimeError):
    """Credentials are unavailable; messages never contain file or secret data."""


class PlatformCredential(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    tenant_id: _Text = Field(repr=False)
    credential_id: Annotated[
        str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    ] = Field(repr=False)
    caller: Literal["kokoro-agent"]
    surfaces: list[Literal["tenant_execution"]] = Field(min_length=1, max_length=1)
    secret: SecretStr = Field(repr=False)


class _CredentialDocument(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    version: int = Field(ge=1, le=1)
    entries: list[PlatformCredential] = Field(min_length=1)


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


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


class CredentialFile:
    """Synchronous bounded reads are atomic with respect to event-loop callers.

    Parent directories are a deployment-trusted secret-mount boundary, as with
    the private signer. Atomic rename is supported; mutation during read is not.
    The complete current mount is checked on every call, never a cached secret.
    """

    def __init__(self, path: str) -> None:
        if not os.path.isabs(path) or os.path.normpath(path) != path:
            raise CredentialError("platform credentials unavailable")
        self._path = path

    def snapshot(self, tenant_id: str) -> PlatformCredential:
        try:
            return self._read()[tenant_id]
        except Exception:
            pass
        raise CredentialError("platform credentials unavailable") from None

    async def credential(self, tenant_id: str) -> str:
        """Read immediately before dispatch; no token issuance or expiry lifecycle."""
        return self.snapshot(tenant_id).secret.get_secret_value()

    def validate(self) -> None:
        """Validate the entire mount at startup without selecting a default tenant."""
        try:
            self._read()
        except Exception:
            failed = True
        else:
            failed = False
        if failed:
            raise CredentialError("platform credentials unavailable") from None

    def _read(self) -> dict[str, PlatformCredential]:
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
        fd = os.open(self._path, flags)
        try:
            before = os.fstat(fd)
            if not (
                stat.S_ISREG(before.st_mode)
                and before.st_uid == os.geteuid()
                and stat.S_IMODE(before.st_mode) in {0o400, 0o600}
                and 0 < before.st_size <= _LIMIT
            ):
                raise ValueError
            raw = bytearray()
            while len(raw) <= _LIMIT:
                chunk = os.read(fd, min(65536, _LIMIT + 1 - len(raw)))
                if not chunk:
                    break
                raw.extend(chunk)
            if len(raw) != before.st_size or _fingerprint(before) != _fingerprint(
                os.fstat(fd)
            ):
                raise ValueError
            # Reject replacement before read completion, not just fd mutation.
            if _fingerprint(before) != _fingerprint(
                os.stat(self._path, follow_symlinks=False)
            ):
                raise ValueError
        finally:
            os.close(fd)
        value: object = json.loads(raw, object_pairs_hook=_object)
        document = _CredentialDocument.model_validate(value)
        result: dict[str, PlatformCredential] = {}
        ids: set[str] = set()
        secrets: set[str] = set()
        for credential in document.entries:
            secret = credential.secret.get_secret_value()
            tenant = credential.tenant_id
            if (
                not 32 <= len(secret) <= 4096
                or any(not 0x21 <= ord(char) <= 0x7E for char in secret)
                or len(tenant.encode("utf-16-le")) // 2 > 255
                or any(
                    char.isspace()
                    or char == "\ufeff"
                    or ord(char) < 32
                    or ord(char) == 127
                    for char in tenant
                )
                or tenant in result
                or credential.credential_id in ids
                or secret in secrets
            ):
                raise ValueError
            result[tenant] = credential
            ids.add(credential.credential_id)
            secrets.add(secret)
        return result
