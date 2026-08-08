"""Rotation-safe TLS material reads, including Kubernetes AtomicWriter mounts."""

from __future__ import annotations

import os
from pathlib import Path
import stat

_MAX_BYTES = 256 * 1024
_MAX_SYMLINK_HOPS = 8
_ROTATION_ATTEMPTS = 3


def read_secure_tls_material(
    value: str,
    *,
    error_code: str,
    private: bool = False,
) -> bytes:
    """Read one bounded regular file without allowing a secret-mount escape.

    Kubernetes AtomicWriter exposes ``name -> ..data/name -> ..generation/name``. Those
    symlinks are accepted only while every resolved component remains beneath the exposed
    file's mount parent. Opening the resolved generation file with ``O_NOFOLLOW`` and matching
    ``lstat`` to ``fstat`` closes the final-file swap window. A rotation race retries from the
    public link; error details never contain paths or material.
    """

    del private  # trust comes from the closed mount root, not host-style 0600 assumptions
    path = Path(value)
    if not path.is_absolute():
        raise ValueError(error_code)
    for _attempt in range(_ROTATION_ATTEMPTS):
        try:
            mount_root = path.parent.resolve(strict=True)
            resolved = _resolve_beneath_mount(path.name, mount_root)
            before = resolved.lstat()
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_size < 1
                or before.st_size > _MAX_BYTES
                or before.st_mode & 0o022 != 0
            ):
                raise OSError("invalid material metadata")
            descriptor = os.open(
                resolved,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
            )
            try:
                opened = os.fstat(descriptor)
                if (
                    (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
                    or not stat.S_ISREG(opened.st_mode)
                    or opened.st_size != before.st_size
                ):
                    raise OSError("material changed")
                material = _read_bounded(descriptor)
            finally:
                os.close(descriptor)
            if len(material) != before.st_size:
                raise OSError("material changed")
            return material
        except (OSError, RuntimeError, ValueError):
            continue
    raise ValueError(error_code) from None


def _resolve_beneath_mount(name: str, mount_root: Path) -> Path:
    pending = [name]
    current = mount_root
    hops = 0
    while pending:
        part = pending.pop(0)
        if part in ("", ".", ".."):
            raise OSError("invalid path component")
        candidate = current / part
        metadata = candidate.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            hops += 1
            if hops > _MAX_SYMLINK_HOPS:
                raise OSError("too many symbolic links")
            target = Path(os.readlink(candidate))
            combined = target if target.is_absolute() else candidate.parent / target
            normalized = Path(os.path.normpath(combined))
            try:
                relative = normalized.relative_to(mount_root)
            except ValueError as error:
                raise OSError("symbolic link escaped mount") from error
            current = mount_root
            pending = [*relative.parts, *pending]
            continue
        if pending and not stat.S_ISDIR(metadata.st_mode):
            raise OSError("non-directory path component")
        current = candidate
    try:
        current.relative_to(mount_root)
    except ValueError as error:
        raise OSError("resolved path escaped mount") from error
    return current


def _read_bounded(descriptor: int) -> bytes:
    chunks: list[bytes] = []
    remaining = _MAX_BYTES + 1
    while remaining:
        chunk = os.read(descriptor, min(64 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    material = b"".join(chunks)
    if len(material) > _MAX_BYTES:
        raise OSError("material exceeds byte budget")
    return material


__all__ = ["read_secure_tls_material"]
