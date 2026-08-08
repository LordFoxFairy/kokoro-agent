from __future__ import annotations

import os
from pathlib import Path

import pytest

from kokoro_agent.security import read_secure_tls_material


def _atomic_writer_secret(root: Path, generation: str, name: str, content: bytes) -> Path:
    generation_dir = root / generation
    generation_dir.mkdir()
    target = generation_dir / name
    target.write_bytes(content)
    target.chmod(0o644)
    data_link = root / "..data"
    data_link.symlink_to(generation)
    exposed = root / name
    exposed.symlink_to(Path("..data") / name)
    return exposed


def test_loader_accepts_kubernetes_atomic_writer_symlinks_and_0644_key(
    tmp_path: Path,
) -> None:
    exposed = _atomic_writer_secret(
        tmp_path,
        "..2026_08_08_00_00_00.000000001",
        "tls.key",
        b"-----BEGIN PRIVATE KEY-----\nfixture\n-----END PRIVATE KEY-----\n",
    )

    material = read_secure_tls_material(
        str(exposed), error_code="TLS_KEY_INVALID", private=True
    )

    assert material.startswith(b"-----BEGIN PRIVATE KEY-----")


def test_loader_observes_atomic_writer_rotation_on_next_read(tmp_path: Path) -> None:
    exposed = _atomic_writer_secret(tmp_path, "..generation-a", "ca.pem", b"generation-a")
    assert read_secure_tls_material(str(exposed), error_code="CA_INVALID") == b"generation-a"

    second = tmp_path / "..generation-b"
    second.mkdir()
    (second / "ca.pem").write_bytes(b"generation-b")
    replacement = tmp_path / "..data-new"
    replacement.symlink_to("..generation-b")
    os.replace(replacement, tmp_path / "..data")

    assert read_secure_tls_material(str(exposed), error_code="CA_INVALID") == b"generation-b"


def test_loader_rejects_atomic_writer_escape_outside_mount_parent(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    mount.mkdir()
    outside = tmp_path / "outside.key"
    outside.write_bytes(b"secret")
    exposed = mount / "tls.key"
    exposed.symlink_to(outside)

    with pytest.raises(ValueError, match="TLS_KEY_INVALID"):
        read_secure_tls_material(str(exposed), error_code="TLS_KEY_INVALID", private=True)


def test_loader_rejects_excessive_symlink_hops(tmp_path: Path) -> None:
    target = tmp_path / "target.pem"
    target.write_bytes(b"certificate")
    current = target
    for index in range(10):
        link = tmp_path / f"link-{index}.pem"
        link.symlink_to(current.name)
        current = link

    with pytest.raises(ValueError, match="TLS_CERT_INVALID"):
        read_secure_tls_material(str(current), error_code="TLS_CERT_INVALID")


def test_loader_rejects_mutable_or_oversized_material(tmp_path: Path) -> None:
    mutable = tmp_path / "mutable.key"
    mutable.write_bytes(b"secret")
    mutable.chmod(0o666)
    oversized = tmp_path / "oversized.pem"
    oversized.write_bytes(b"x" * (256 * 1024 + 1))

    with pytest.raises(ValueError, match="TLS_KEY_INVALID"):
        read_secure_tls_material(str(mutable), error_code="TLS_KEY_INVALID", private=True)
    with pytest.raises(ValueError, match="TLS_CERT_INVALID"):
        read_secure_tls_material(str(oversized), error_code="TLS_CERT_INVALID")
