"""Current direct service credentials: no exchanged token or stale cache."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from kokoro_agent.clients.platform_credentials import CredentialFile, CredentialError


def entry(tenant: str = "tenant", secret: str = "s" * 32) -> dict[str, object]:
    return dict(
        credential_id=tenant,
        caller="kokoro-agent",
        surfaces=["tenant_execution"],
        tenant_id=tenant,
        secret=secret,
    )


def write_credentials(
    path: Path, entries: list[dict[str, object]] | None = None
) -> None:
    path.write_text(json.dumps(dict(version=1, entries=entries or [entry()])))
    path.chmod(0o600)


async def test_current_service_secret_and_atomic_rotation_without_token_exchange(
    tmp_path: Path,
) -> None:
    path = tmp_path / "credentials.json"
    write_credentials(path)
    reader = CredentialFile(str(path))
    reader.validate()
    assert await reader.credential("tenant") == "s" * 32
    assert "s" * 32 not in repr(reader.snapshot("tenant"))
    replacement = tmp_path / "replacement.json"
    write_credentials(replacement, [entry(secret="r" * 32)])
    replacement.replace(path)
    assert await reader.credential("tenant") == "r" * 32
    path.unlink()
    with pytest.raises(CredentialError):
        await reader.credential("tenant")


async def test_tenants_never_share_or_default_credentials(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    write_credentials(path, [entry(), entry("second", "r" * 32)])
    reader = CredentialFile(str(path))
    assert await reader.credential("second") == "r" * 32
    with pytest.raises(CredentialError):
        await reader.credential("unknown")


@pytest.mark.parametrize(
    "mutation",
    [
        "caller",
        "surface",
        "extra_surface",
        "unknown",
        "duplicate_tenant",
        "duplicate_id",
        "duplicate_secret",
        "short_secret",
        "long_secret",
        "control_secret",
        "blank_tenant",
        "control_tenant",
        "long_tenant",
        "bad_id",
        "old_format",
        "version_bool",
        "unknown_root",
        "empty",
        "unrelated_invalid",
    ],
)
def test_entire_current_file_is_strict_and_legacy_is_rejected(
    tmp_path: Path, mutation: str
) -> None:
    path = tmp_path / "credentials.json"
    write_credentials(path)
    data = json.loads(path.read_text())
    first = data["entries"][0]
    changes = {
        "caller": ("caller", "kokoro-bff"),
        "surface": ("surfaces", ["skills_admin"]),
        "extra_surface": ("surfaces", ["tenant_execution", "tenant_execution"]),
        "unknown": ("client_id", "old-oauth"),
        "short_secret": ("secret", "s" * 31),
        "long_secret": ("secret", "s" * 4097),
        "control_secret": ("secret", "s" * 32 + "\r\n"),
        "blank_tenant": ("tenant_id", "a b"),
        "control_tenant": ("tenant_id", "a\x7f"),
        "long_tenant": ("tenant_id", "a" * 256),
        "bad_id": ("credential_id", "/bad"),
    }
    if mutation in changes:
        field, value = changes[mutation]
        first[field] = value
    elif mutation.startswith("duplicate_"):
        second = entry("second", "r" * 32)
        field = {
            "duplicate_tenant": "tenant_id",
            "duplicate_id": "credential_id",
            "duplicate_secret": "secret",
        }[mutation]
        second[field] = first[field]
        data["entries"].append(second)
    elif mutation == "old_format":
        data = [
            dict(
                tenant_id="tenant",
                generation=1,
                client_id="agent",
                client_secret="secret",
                credential_ref_version="v1",
                resource="https://kokoro.dev/resources/platform-internal",
                scope="platform:execution.invoke",
            )
        ]
    elif mutation == "version_bool":
        data["version"] = True
    elif mutation == "unknown_root":
        data["extra"] = "value"
    elif mutation == "empty":
        data["entries"] = []
    else:
        data["entries"].append(dict(entry("second", "r" * 32), caller="kokoro-system"))
    path.write_text(json.dumps(data))
    with pytest.raises(CredentialError) as captured:
        CredentialFile(str(path)).snapshot("tenant")
    assert str(path) not in str(captured.value)
    assert "s" * 32 not in str(captured.value)


def test_duplicate_json_key_and_symlink_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    write_credentials(path)
    alias = tmp_path / "alias"
    alias.symlink_to(path)
    with pytest.raises(CredentialError):
        CredentialFile(str(alias)).snapshot("tenant")
    path.write_text('{"version":1,"version":1,"entries":[]}')
    with pytest.raises(CredentialError):
        CredentialFile(str(path)).snapshot("tenant")


@pytest.mark.parametrize("mode", [0o644, 0o660, 0o700])
def test_owner_only_mode_rechecked_after_valid_read(tmp_path: Path, mode: int) -> None:
    path = tmp_path / "credentials.json"
    write_credentials(path)
    reader = CredentialFile(str(path))
    reader.validate()
    path.chmod(mode)
    with pytest.raises(CredentialError):
        reader.snapshot("tenant")


def test_fifo_and_read_time_mutation_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "credentials.json"
    os.mkfifo(path, mode=0o600)
    with pytest.raises(CredentialError):
        CredentialFile(str(path)).snapshot("tenant")
    path.unlink()
    write_credentials(path)
    real_read = os.read

    def changed(fd: int, length: int) -> bytes:
        data = real_read(fd, length)
        path.chmod(0o644)
        return data

    monkeypatch.setattr(os, "read", changed)
    with pytest.raises(CredentialError):
        CredentialFile(str(path)).snapshot("tenant")
