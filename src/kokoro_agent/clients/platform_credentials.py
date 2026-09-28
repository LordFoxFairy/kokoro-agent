"""Worker-only, generation-fenced Platform workload credential snapshots."""

from __future__ import annotations

import json
import os
import stat
from typing import Annotated

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StringConstraints,
    TypeAdapter,
)

_RESOURCE = "https://kokoro.dev/resources/platform-internal"
_SCOPE = "platform:execution.invoke"
_LIMIT = 1_048_576
_Text = Annotated[str, StringConstraints(min_length=1)]


class CredentialError(RuntimeError):
    """Credentials are unavailable; messages never contain file or secret data."""


class PlatformCredential(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    tenant_id: _Text = Field(repr=False)
    generation: int = Field(ge=1, le=9_007_199_254_740_991)
    credential_ref_version: _Text = Field(repr=False)
    client_id: _Text = Field(repr=False)
    client_secret: SecretStr = Field(repr=False)
    resource: _Text = Field(repr=False)
    scope: _Text = Field(repr=False)


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
    High-water marks survive removal, preventing a deleted tenant's rollback.
    """

    def __init__(self, path: str) -> None:
        if not os.path.isabs(path) or os.path.normpath(path) != path:
            raise CredentialError("platform credentials unavailable")
        self._path = path
        self._observed: dict[str, PlatformCredential] = {}

    def snapshot(self, tenant_id: str) -> PlatformCredential:
        try:
            snapshots = self._read()
            for tenant, current in snapshots.items():
                previous = self._observed.get(tenant)
                if previous is not None and (
                    current.generation < previous.generation
                    or (
                        current.generation == previous.generation
                        and current != previous
                    )
                ):
                    raise ValueError
            self._observed.update(snapshots)
            return snapshots[tenant_id]
        except Exception:
            pass
        raise CredentialError("platform credentials unavailable") from None

    def validate(self) -> None:
        """Validate the entire mount at startup without selecting a default tenant."""
        try:
            self._observed = self._read()
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
        if not isinstance(value, list) or not value:
            raise ValueError
        result: dict[str, PlatformCredential] = {}
        clients: set[str] = set()
        for entry in TypeAdapter(list[object]).validate_python(value):
            credential = PlatformCredential.model_validate(entry)
            secret = credential.client_secret.get_secret_value()
            if (
                credential.resource != _RESOURCE
                or credential.scope != _SCOPE
                or not secret
                or ":" in credential.client_id
                or credential.tenant_id in result
                or credential.client_id in clients
            ):
                raise ValueError
            for text in (
                credential.tenant_id,
                credential.client_id,
                secret,
                credential.credential_ref_version,
            ):
                if not text.strip() or any(
                    ord(char) < 32 or 0xD800 <= ord(char) <= 0xDFFF for char in text
                ):
                    raise ValueError
            result[credential.tenant_id] = credential
            clients.add(credential.client_id)
        return result
