"""Private execution-proof key loading boundary."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import subprocess
import sys
import stat
from types import SimpleNamespace
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, ed448, rsa, x25519
import pytest
from pydantic import BaseModel

from kokoro_agent.execution.execution_proof_keys import (
    ExecutionProofKeyError,
    WorkerExecutionProofConfig,
    load_execution_proof_signer,
)
import kokoro_agent.execution.execution_proof_keys as key_loader

SEED = bytes.fromhex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
THUMBPRINT = "kPrK_qmxVWaYVA9wwBF6Iuo3vVzz7TxHCTwXBygrS4k"


def _pem(
    key: ed25519.Ed25519PrivateKey
    | rsa.RSAPrivateKey
    | x25519.X25519PrivateKey
    | None = None,
) -> bytes:
    private = key or ed25519.Ed25519PrivateKey.from_private_bytes(SEED)
    return private.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _file(tmp_path: Path, data: bytes | None = None, mode: int = 0o600) -> Path:
    path = tmp_path / "proof-private.pem"
    path.write_bytes(_pem() if data is None else data)
    path.chmod(mode)
    return path


def _config(path: Path, **values: Any) -> WorkerExecutionProofConfig:
    fields: dict[str, Any] = {
        "issuer": "urn:kokoro:agent:test",
        "private_key_file": str(path),
        "active_kid": "agent-key-a",
        "active_jwk_thumbprint_sha256": THUMBPRINT,
    }
    fields.update(values)
    return WorkerExecutionProofConfig(**fields)


def _assert_sanitized(error: BaseException, sentinels: tuple[str, ...]) -> None:
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        text = f"{current!s} {current!r}"
        assert all(sentinel not in text for sentinel in sentinels)
        current = current.__cause__ or current.__context__
    assert error.__cause__ is None
    assert error.__context__ is None


def test_loads_exact_rfc8032_key_and_constructs_signer(tmp_path: Path) -> None:
    path = _file(tmp_path)
    signer = load_execution_proof_signer(_config(path))
    assert signer is not None
    assert "PRIVATE" not in repr(signer)


def test_valid_different_kid_constructs_same_key(tmp_path: Path) -> None:
    path = _file(tmp_path)
    assert load_execution_proof_signer(_config(path, active_kid="rotated-key"))


@pytest.mark.parametrize(
    "field,value",
    [
        ("issuer", ""),
        ("issuer", "bad\ud800"),
        ("private_key_file", "relative.pem"),
        ("private_key_file", "/tmp/../key.pem"),
        ("private_key_file", "/tmp//key.pem"),
        ("private_key_file", "/tmp/./key.pem"),
        ("private_key_file", "/tmp/key\x00.pem"),
        ("active_kid", ""),
        ("active_kid", "bad\ud800"),
        ("active_jwk_thumbprint_sha256", "bad="),
        ("active_jwk_thumbprint_sha256", "a" * 43),
    ],
)
def test_config_rejects_invalid_exact_values(
    field: str, value: object, tmp_path: Path
) -> None:
    with pytest.raises(ExecutionProofKeyError):
        _config(_file(tmp_path), **{field: value})


def test_loader_rejects_duck_subclass_and_post_construction_mutation_before_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    config = _config(path)
    object.__setattr__(config, "issuer", "")
    calls = 0

    def fail_open(*args: object, **kwargs: object) -> int:
        nonlocal calls
        calls += 1
        raise AssertionError("open must not run")

    monkeypatch.setattr(os, "open", fail_open)
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(config)
    with pytest.raises(ExecutionProofKeyError):
        invalid: Any = object()
        load_execution_proof_signer(invalid)
    assert calls == 0


def test_pydantic_model_copy_duck_is_rejected_before_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class CopyableWorkerConfig(BaseModel):
        issuer: str
        private_key_file: str
        active_kid: str
        active_jwk_thumbprint_sha256: str

    config = _config(_file(tmp_path))
    assert not hasattr(config, "model_copy")
    duck = CopyableWorkerConfig(
        issuer=config.issuer,
        private_key_file=config.private_key_file,
        active_kid=config.active_kid,
        active_jwk_thumbprint_sha256=config.active_jwk_thumbprint_sha256,
    ).model_copy(update={"issuer": ""})
    calls = 0

    def fail_open(*args: object, **kwargs: object) -> int:
        nonlocal calls
        calls += 1
        raise AssertionError("open must not run")

    monkeypatch.setattr(os, "open", fail_open)
    with pytest.raises(ExecutionProofKeyError):
        invalid: Any = duck
        load_execution_proof_signer(invalid)
    assert calls == 0


@pytest.mark.parametrize("mode", [0o000, 0o400, 0o644, 0o660])
def test_file_mode_is_exactly_0400_or_0600(tmp_path: Path, mode: int) -> None:
    path = _file(tmp_path, mode=mode)
    if mode == 0o400:
        assert load_execution_proof_signer(_config(path))
    else:
        with pytest.raises(ExecutionProofKeyError):
            load_execution_proof_signer(_config(path))


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"x" * 16385,
        _pem() + _pem(),
        b"junk" + _pem(),
        b"\v" + _pem(),
        _pem().replace(b"PRIVATE KEY", b"ENCRYPTED PRIVATE KEY"),
    ],
)
def test_rejects_size_and_pem_lexical_violations(tmp_path: Path, data: bytes) -> None:
    path = _file(tmp_path, data=data)
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(_config(path))


def test_accepts_exact_16384_byte_file_with_allowed_outer_whitespace(
    tmp_path: Path,
) -> None:
    raw = _pem()
    path = _file(tmp_path, b" " * (16384 - len(raw)) + raw)
    assert path.stat().st_size == 16384
    assert load_execution_proof_signer(_config(path))


@pytest.mark.parametrize(
    "key",
    [
        rsa.generate_private_key(public_exponent=65537, key_size=2048),
        x25519.X25519PrivateKey.generate(),
    ],
)
def test_rejects_non_ed25519_pkcs8_keys(
    tmp_path: Path,
    key: rsa.RSAPrivateKey | x25519.X25519PrivateKey,
) -> None:
    path = _file(tmp_path, _pem(key))
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(_config(path))


def test_rejects_symlink_directory_and_thumbprint_mismatch(tmp_path: Path) -> None:
    path = _file(tmp_path)
    link = tmp_path / "link.pem"
    link.symlink_to(path)
    for candidate in (link, tmp_path):
        with pytest.raises(ExecutionProofKeyError):
            load_execution_proof_signer(_config(candidate))
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(
            _config(
                path,
                active_jwk_thumbprint_sha256=base64.urlsafe_b64encode(b"z" * 32)
                .rstrip(b"=")
                .decode(),
            )
        )


def test_config_and_failures_are_recursively_sanitized(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    sentinels = ("ISSUER_SENTINEL", "PATH_SENTINEL", "KID_SENTINEL", "THUMB_SENTINEL")
    config = WorkerExecutionProofConfig.__new__(WorkerExecutionProofConfig)
    for name, value in zip(
        ("issuer", "private_key_file", "active_kid", "active_jwk_thumbprint_sha256"),
        sentinels,
        strict=True,
    ):
        object.__setattr__(config, name, value)
    assert all(value not in repr(config) for value in sentinels)
    with (
        caplog.at_level(logging.DEBUG),
        pytest.raises(ExecutionProofKeyError) as caught,
    ):
        load_execution_proof_signer(config)
    _assert_sanitized(caught.value, sentinels)
    assert all(value not in caplog.text for value in sentinels)


def test_required_native_open_flags_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    monkeypatch.delattr(os, "O_NOFOLLOW")
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(_config(path))


def test_loader_uses_required_flags_and_closes_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    original_open = os.open
    original_close = os.close
    opened: list[int] = []
    closed: list[int] = []
    flags_seen = 0

    def tracking_open(
        name: str, flags: int, mode: int = 0o777, *, dir_fd: int | None = None
    ) -> int:
        nonlocal flags_seen
        flags_seen = flags
        fd = original_open(name, flags, mode, dir_fd=dir_fd)
        opened.append(fd)
        return fd

    def tracking_close(fd: int) -> None:
        closed.append(fd)
        original_close(fd)

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "close", tracking_close)
    assert load_execution_proof_signer(_config(path))
    required = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
    assert flags_seen & required == required
    assert opened == closed


def test_rejects_config_subclass_and_str_subclass(tmp_path: Path) -> None:
    path = _file(tmp_path)

    class Text(str):
        pass

    with pytest.raises(ExecutionProofKeyError):
        _config(path, issuer=Text("urn:test"))

    @dataclass(frozen=True, slots=True, kw_only=True)
    class Derived(WorkerExecutionProofConfig):
        pass

    derived = Derived(
        issuer="urn:test",
        private_key_file=str(path),
        active_kid="kid",
        active_jwk_thumbprint_sha256=THUMBPRINT,
    )
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(derived)


def test_rejects_real_encrypted_legacy_ec_and_ed448_keys(tmp_path: Path) -> None:
    encrypted = ed25519.Ed25519PrivateKey.generate().private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.BestAvailableEncryption(b"password"),
    )
    legacy = ec.generate_private_key(ec.SECP256R1()).private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    wrong_curve = ed448.Ed448PrivateKey.generate().private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    for raw in (encrypted, legacy, wrong_curve):
        with pytest.raises(ExecutionProofKeyError):
            load_execution_proof_signer(_config(_file(tmp_path, raw)))


def test_private_fifo_fails_in_subprocess_and_nonblock_mutant_hangs(
    tmp_path: Path,
) -> None:
    fifo = tmp_path / "private.fifo"
    os.mkfifo(fifo, 0o600)
    script = f"""\nimport os\nfrom kokoro_agent.execution.execution_proof_keys import WorkerExecutionProofConfig, load_execution_proof_signer, ExecutionProofKeyError\nc=WorkerExecutionProofConfig(issuer="urn:test", private_key_file={str(fifo)!r}, active_kid="kid", active_jwk_thumbprint_sha256={THUMBPRINT!r})\ntry:\n    load_execution_proof_signer(c)\nexcept ExecutionProofKeyError:\n    raise SystemExit(0)\nraise SystemExit(1)\n"""
    completed = subprocess.run([sys.executable, "-c", script], timeout=1, check=False)
    assert completed.returncode == 0
    mutant = "import os; os.O_NONBLOCK=0\n" + script
    process = subprocess.Popen([sys.executable, "-c", mutant])
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            process.wait(timeout=0.4)
    finally:
        process.terminate()
        process.wait(timeout=1)


def test_private_metadata_change_during_read_fails_and_closes_fd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    original_fstat = os.fstat
    original_close = os.close
    calls = 0
    closed: list[int] = []

    def changing_fstat(fd: int) -> Any:
        nonlocal calls
        value = original_fstat(fd)
        calls += 1
        if calls == 2:
            return SimpleNamespace(
                st_dev=value.st_dev,
                st_ino=value.st_ino,
                st_uid=value.st_uid,
                st_mode=value.st_mode,
                st_size=value.st_size,
                st_mtime_ns=value.st_mtime_ns,
                st_ctime_ns=value.st_ctime_ns + 1,
            )
        return value

    def tracking_close(fd: int) -> None:
        closed.append(fd)
        original_close(fd)

    monkeypatch.setattr(os, "fstat", changing_fstat)
    monkeypatch.setattr(os, "close", tracking_close)
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(_config(path))
    assert len(closed) == 1


@pytest.mark.parametrize(
    "changed", ["growth", "truncation", "chmod", "same_size_overwrite"]
)
def test_each_private_in_read_metadata_mutation_fails_closed(
    changed: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    original_fstat = os.fstat
    calls = 0

    def changing(fd: int) -> Any:
        nonlocal calls
        value = original_fstat(fd)
        calls += 1
        if calls != 2:
            return value
        return SimpleNamespace(
            st_dev=value.st_dev,
            st_ino=value.st_ino,
            st_uid=value.st_uid,
            st_mode=value.st_mode ^ (stat.S_IWUSR if changed == "chmod" else 0),
            st_size=value.st_size
            + (1 if changed == "growth" else -1 if changed == "truncation" else 0),
            st_mtime_ns=value.st_mtime_ns
            + (1 if changed == "same_size_overwrite" else 0),
            st_ctime_ns=value.st_ctime_ns,
        )

    monkeypatch.setattr(os, "fstat", changing)
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(_config(path))


def test_private_wrong_effective_owner_fails_without_chown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    monkeypatch.setattr(os, "geteuid", lambda: path.stat().st_uid + 100_000)
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(_config(path))


@pytest.mark.parametrize("early", [b"", _pem()])
def test_stable_metadata_short_or_empty_early_eof_fails_closed(
    early: bytes, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pem = _pem()
    path = _file(tmp_path, pem + b"        ")

    def early_read(_fd: int, _limit: int) -> bytes:
        return early

    monkeypatch.setattr(key_loader, "_read_bounded", early_read)
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(_config(path))


def test_rejects_ec_pkcs8_key(tmp_path: Path) -> None:
    raw = ec.generate_private_key(ec.SECP256R1()).private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(_config(_file(tmp_path, raw)))


@pytest.mark.parametrize(
    ("field", "sentinel"),
    [
        ("issuer", "ISSUER_SENTINEL\ud800"),
        ("private_key_file", "/PATH_SENTINEL/../key.pem"),
        ("active_kid", "KID_SENTINEL\ud800"),
        ("active_jwk_thumbprint_sha256", "THUMB_SENTINEL"),
    ],
)
def test_each_invalid_descriptor_field_reaches_its_own_pre_open_gate_sanitized(
    field: str,
    sentinel: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = _file(tmp_path)
    values = {
        "issuer": "urn:kokoro:agent:test",
        "private_key_file": str(path),
        "active_kid": "agent-key-a",
        "active_jwk_thumbprint_sha256": THUMBPRINT,
    }
    values[field] = sentinel
    config = WorkerExecutionProofConfig.__new__(WorkerExecutionProofConfig)
    for name, value in values.items():
        object.__setattr__(config, name, value)
    calls = 0

    def bomb_open(*_args: object, **_kwargs: object) -> int:
        nonlocal calls
        calls += 1
        raise AssertionError("open must not run")

    monkeypatch.setattr(os, "open", bomb_open)
    with (
        caplog.at_level(logging.DEBUG),
        pytest.raises(ExecutionProofKeyError) as caught,
    ):
        load_execution_proof_signer(config)
    assert calls == 0
    assert sentinel not in repr(config)
    _assert_sanitized(caught.value, (sentinel,))
    assert sentinel not in caplog.text


@pytest.mark.parametrize("flag", ["O_RDONLY", "O_CLOEXEC", "O_NOFOLLOW", "O_NONBLOCK"])
def test_each_required_private_open_flag_fails_closed_when_absent(
    flag: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    monkeypatch.delattr(os, flag)
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(_config(path))


@pytest.mark.parametrize("stage", ["stat", "read", "pem", "crypto"])
def test_private_descriptor_closes_on_each_failure_stage(
    stage: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = _pem()
    if stage == "pem":
        data = b"not a private key"
    elif stage == "crypto":
        data = _pem().replace(b"MC4CAQ", b"XXXXXX", 1)
    path = _file(tmp_path, data)
    original_open, original_close = os.open, os.close
    opened: list[int] = []
    closed: list[int] = []

    def tracking_open(*args: Any, **kwargs: Any) -> int:
        fd = original_open(*args, **kwargs)
        opened.append(fd)
        return fd

    def tracking_close(fd: int) -> None:
        closed.append(fd)
        original_close(fd)

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "close", tracking_close)
    if stage == "stat":

        def fail_stat(_fd: int) -> os.stat_result:
            raise OSError("x")

        monkeypatch.setattr(os, "fstat", fail_stat)
    elif stage == "read":

        def fail_read(_fd: int, _size: int) -> bytes:
            raise OSError("x")

        monkeypatch.setattr(os, "read", fail_read)
    with pytest.raises(ExecutionProofKeyError):
        load_execution_proof_signer(_config(path))
    assert opened == closed
