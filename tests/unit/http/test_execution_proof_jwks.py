"""Strict public execution-proof JWK ring snapshots."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace
from collections.abc import Callable
from typing import Any, cast

import pytest
import rfc8785
from pydantic import BaseModel

from kokoro_agent.interfaces.http.execution_proof_jwks import (
    ExecutionProofJwksError,
    ExecutionProofJwksState,
    HttpExecutionProofConfig,
    load_execution_proof_jwks,
)
import kokoro_agent.interfaces.http.execution_proof_jwks as jwks_loader

X = "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"
THUMBPRINT = "kPrK_qmxVWaYVA9wwBF6Iuo3vVzz7TxHCTwXBygrS4k"


def _remove_created_test_directory(
    path: Path, *, parent: Path, prefix: str, identity: tuple[int, int, int, int]
) -> None:
    current = os.lstat(path)
    if (
        path.parent != parent
        or not path.name.startswith(prefix)
        or stat.S_ISLNK(current.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or (current.st_dev, current.st_ino, current.st_uid, current.st_mode) != identity
        or shutil.rmtree.avoids_symlink_attacks is not True
    ):
        raise RuntimeError("unsafe public-ring test cleanup")
    shutil.rmtree(path)
    if os.path.lexists(path):
        raise RuntimeError("public-ring test cleanup failed")


def test_public_ring_positive_runs_from_nonrepo_cwd_without_dot_tmp() -> None:
    parent = Path(__file__).resolve().parent
    prefix = "a2b-nonrepo-cwd-"
    cwd = Path(tempfile.mkdtemp(prefix=prefix, dir=parent))
    value = os.lstat(cwd)
    identity = (value.st_dev, value.st_ino, value.st_uid, value.st_mode)
    try:
        assert not (cwd / ".tmp").exists()
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                f"{Path(__file__).resolve()}::test_loads_a1_jwk_and_precomputes_exact_jcs",
            ],
            cwd=cwd,
            timeout=10,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr
    finally:
        _remove_created_test_directory(
            cwd, parent=parent, prefix=prefix, identity=identity
        )


@pytest.fixture
def tmp_path() -> Any:
    """Create only under the test module's existing verified-safe parent chain."""
    base = Path(__file__).resolve().parent
    chain = [base, *base.parents]
    before = {
        item: (value.st_dev, value.st_ino, value.st_uid, value.st_mode)
        for item in chain
        for value in (os.stat(item, follow_symlinks=False),)
    }
    assert all(
        stat.S_ISDIR(mode)
        and uid in {0, os.geteuid()}
        and not stat.S_IMODE(mode) & 0o022
        for _dev, _ino, uid, mode in before.values()
    )
    prefix = "a2b-public-ring-"
    root = Path(tempfile.mkdtemp(prefix=prefix, dir=base))
    root.chmod(0o700)
    root_value = os.lstat(root)
    root_identity = (
        root_value.st_dev,
        root_value.st_ino,
        root_value.st_uid,
        root_value.st_mode,
    )
    try:
        yield root
    finally:
        _remove_created_test_directory(
            root, parent=base, prefix=prefix, identity=root_identity
        )
        after = {
            item: (value.st_dev, value.st_ino, value.st_uid, value.st_mode)
            for item in chain
            for value in (os.stat(item, follow_symlinks=False),)
        }
        assert after == before


def _jwk(kid: str = "agent-key-a", **extra: object) -> dict[str, object]:
    value: dict[str, object] = {
        "kty": "OKP",
        "crv": "Ed25519",
        "use": "sig",
        "alg": "EdDSA",
        "kid": kid,
        "x": X,
    }
    value.update(extra)
    return value


def _safe_dir(tmp_path: Path) -> Path:
    path = tmp_path / "safe"
    path.mkdir(mode=0o700)
    path.chmod(0o700)
    return path


def _file(tmp_path: Path, value: object | None = None, *, mode: int = 0o644) -> Path:
    directory = _safe_dir(tmp_path)
    path = directory / "jwks.json"
    payload = {"keys": [_jwk()]} if value is None else value
    raw = (
        payload
        if isinstance(payload, bytes)
        else json.dumps(payload, ensure_ascii=False).encode()
    )
    path.write_bytes(raw)
    path.chmod(mode)
    return path


def _config(path: Path, **values: Any) -> HttpExecutionProofConfig:
    fields: dict[str, Any] = {
        "public_jwks_file": str(path),
        "active_kid": "agent-key-a",
        "active_jwk_thumbprint_sha256": THUMBPRINT,
    }
    fields.update(values)
    return HttpExecutionProofConfig(**fields)


def test_loads_a1_jwk_and_precomputes_exact_jcs(tmp_path: Path) -> None:
    path = _file(tmp_path)
    state = load_execution_proof_jwks(_config(path))
    assert state.available is True
    expected: Any = {"keys": [_jwk()]}
    assert state.body == rfc8785.dumps(expected)
    assert state.content_type == "application/jwk-set+json"


def test_keys_sort_by_utf8_without_unicode_normalization(tmp_path: Path) -> None:
    keys = [_jwk("é"), _jwk("e\u0301")]
    path = _file(tmp_path, {"keys": keys})
    config = _config(path, active_kid="é")
    state = load_execution_proof_jwks(config)
    assert state.available
    decoded = json.loads(state.body or b"")
    assert [key["kid"] for key in decoded["keys"]] == ["e\u0301", "é"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("public_jwks_file", "relative.json"),
        ("public_jwks_file", "/tmp/../ring.json"),
        ("public_jwks_file", "/tmp//ring.json"),
        ("public_jwks_file", "/tmp/ring\x00.json"),
        ("active_kid", ""),
        ("active_kid", "bad\ud800"),
        ("active_jwk_thumbprint_sha256", "bad="),
    ],
)
def test_config_is_strict_and_canonical(
    field: str, value: object, tmp_path: Path
) -> None:
    with pytest.raises(ExecutionProofJwksError):
        _config(_file(tmp_path), **{field: value})


def test_mutated_and_duck_config_fail_before_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(_file(tmp_path))
    object.__setattr__(config, "active_kid", "")
    calls = 0

    def fail_open(*args: object, **kwargs: object) -> int:
        nonlocal calls
        calls += 1
        raise AssertionError

    monkeypatch.setattr(os, "open", fail_open)
    assert not load_execution_proof_jwks(config).available
    invalid: Any = object()
    assert not load_execution_proof_jwks(invalid).available
    assert calls == 0


def test_pydantic_model_copy_duck_is_rejected_before_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class CopyableHttpConfig(BaseModel):
        public_jwks_file: str
        active_kid: str
        active_jwk_thumbprint_sha256: str

    config = _config(_file(tmp_path))
    assert not hasattr(config, "model_copy")
    duck = CopyableHttpConfig(
        public_jwks_file=config.public_jwks_file,
        active_kid=config.active_kid,
        active_jwk_thumbprint_sha256=config.active_jwk_thumbprint_sha256,
    ).model_copy(update={"active_kid": ""})
    calls = 0

    def fail_open(*args: object, **kwargs: object) -> int:
        nonlocal calls
        calls += 1
        raise AssertionError("open must not run")

    monkeypatch.setattr(os, "open", fail_open)
    invalid: Any = duck
    assert not load_execution_proof_jwks(invalid).available
    assert calls == 0


@pytest.mark.parametrize(
    "mode,valid",
    [
        (0o400, True),
        (0o440, True),
        (0o444, True),
        (0o600, True),
        (0o640, True),
        (0o644, True),
        (0o000, False),
        (0o666, False),
    ],
)
def test_final_file_modes_are_exact(tmp_path: Path, mode: int, valid: bool) -> None:
    state = load_execution_proof_jwks(_config(_file(tmp_path, mode=mode)))
    assert state.available is valid


@pytest.mark.parametrize(
    "value",
    [
        b"",
        b"\xef\xbb\xbf{}",
        b"\xff",
        b'{"keys":[],"keys":[]}',
        {},
        {"keys": []},
        {"keys": [_jwk(extra="x")]},
        {"keys": [{key: value for key, value in _jwk().items() if key != "use"}]},
        {"keys": [_jwk(d="private")]},
        {"keys": [_jwk(x=X + "=")]},
        {"keys": [_jwk(x="AA")]},
        {"keys": [_jwk(), _jwk()]},
    ],
)
def test_strict_json_and_exact_jwk_shape_fail_closed(
    tmp_path: Path, value: object
) -> None:
    assert not load_execution_proof_jwks(_config(_file(tmp_path, value))).available


def test_rejects_lone_surrogate_from_json_escape(tmp_path: Path) -> None:
    raw = (
        b'{"keys":[{"alg":"EdDSA","crv":"Ed25519","kid":"\\ud800","kty":"OKP","use":"sig","x":"'
        + X.encode()
        + b'"}]}'
    )
    assert not load_execution_proof_jwks(_config(_file(tmp_path, raw))).available


def test_rejects_active_descriptor_mismatch_without_partial_state(
    tmp_path: Path,
) -> None:
    path = _file(tmp_path)
    state = load_execution_proof_jwks(_config(path, active_kid="missing"))
    assert state == ExecutionProofJwksState.unavailable()
    assert state.body is None
    assert "missing" not in repr(state)


def test_rejects_symlink_directory_and_unsafe_parent(tmp_path: Path) -> None:
    path = _file(tmp_path)
    link = path.parent / "link.json"
    link.symlink_to(path)
    assert not load_execution_proof_jwks(_config(link)).available
    assert not load_execution_proof_jwks(_config(path.parent)).available
    path.parent.chmod(0o777)
    try:
        assert not load_execution_proof_jwks(_config(path)).available
    finally:
        path.parent.chmod(0o700)


def test_input_and_output_bounds(tmp_path: Path) -> None:
    path = _file(tmp_path, b"x" * 65537)
    assert not load_execution_proof_jwks(_config(path)).available


def test_required_native_flags_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(_file(tmp_path))
    monkeypatch.delattr(os, "O_DIRECTORY")
    assert not load_execution_proof_jwks(config).available


def test_state_and_config_repr_do_not_reveal_descriptor(tmp_path: Path) -> None:
    path = _file(tmp_path)
    config = _config(path)
    state = load_execution_proof_jwks(config)
    for sentinel in (str(path), "agent-key-a", THUMBPRINT, X):
        assert sentinel not in repr(config)
        assert sentinel not in repr(state)


@pytest.mark.parametrize(
    "body",
    [
        b"{}",
        b'{"keys":[]}',
        b'{"keys":[{"alg":"EdDSA","crv":"Ed25519","kid":"agent-key-a","kty":"OKP","use":"sig","x":"'
        + X.encode()
        + b'"}]} ',
        b'{"keys":[{"kty":"OKP","crv":"Ed25519","use":"sig","alg":"EdDSA","kid":"agent-key-a","x":"'
        + X.encode()
        + b'"}]}',
        b"x" * 65_537,
    ],
)
def test_available_state_rejects_invalid_or_noncanonical_body(body: bytes) -> None:
    with pytest.raises(ExecutionProofJwksError):
        ExecutionProofJwksState(available=True, body=body)


def test_available_state_rejects_bytes_subclass() -> None:
    class DerivedBytes(bytes):
        pass

    with pytest.raises(ExecutionProofJwksError):
        ExecutionProofJwksState(available=True, body=DerivedBytes(b"{}"))


def test_rejects_config_subclass_and_string_subclass(tmp_path: Path) -> None:
    path = _file(tmp_path)

    class Text(str):
        pass

    with pytest.raises(ExecutionProofJwksError):
        _config(path, active_kid=Text("kid"))

    @dataclass(frozen=True, slots=True, kw_only=True)
    class Derived(HttpExecutionProofConfig):
        pass

    derived = Derived(
        public_jwks_file=str(path),
        active_kid="agent-key-a",
        active_jwk_thumbprint_sha256=THUMBPRINT,
    )
    assert not load_execution_proof_jwks(derived).available


def test_accepts_exact_65536_byte_input_but_output_remains_bounded(
    tmp_path: Path,
) -> None:
    canonical = json.dumps({"keys": [_jwk()]}, separators=(",", ":")).encode()
    path = _file(tmp_path, canonical + b" " * (65536 - len(canonical)))
    assert path.stat().st_size == 65536
    state = load_execution_proof_jwks(_config(path))
    assert state.available
    assert state.body is not None and len(state.body) <= 65536


def test_public_fifo_fails_in_subprocess_and_nonblock_mutant_hangs(
    tmp_path: Path,
) -> None:
    directory = _safe_dir(tmp_path)
    fifo = directory / "public.fifo"
    os.mkfifo(fifo, 0o644)
    script = f"""\nimport os\nfrom kokoro_agent.interfaces.http.execution_proof_jwks import HttpExecutionProofConfig, load_execution_proof_jwks\nc=HttpExecutionProofConfig(public_jwks_file={str(fifo)!r}, active_kid="agent-key-a", active_jwk_thumbprint_sha256={THUMBPRINT!r})\nraise SystemExit(0 if not load_execution_proof_jwks(c).available else 1)\n"""
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


def test_public_loader_closes_every_opened_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    original_open = os.open
    original_close = os.close
    opened: list[int] = []
    closed: list[int] = []

    def tracking_open(
        name: str, flags: int, mode: int = 0o777, *, dir_fd: int | None = None
    ) -> int:
        fd = original_open(name, flags, mode, dir_fd=dir_fd)
        opened.append(fd)
        return fd

    def tracking_close(fd: int) -> None:
        closed.append(fd)
        original_close(fd)

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "close", tracking_close)
    assert load_execution_proof_jwks(_config(path)).available
    assert sorted(opened) == sorted(closed)


@pytest.mark.parametrize("failure_attempt", [1, 2])
def test_close_fault_still_attempts_every_opened_descriptor_once_and_fails_closed(
    failure_attempt: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    original_open, original_close = os.open, os.close
    opened: list[int] = []
    attempts: list[int] = []

    def tracking_open(*args: Any, **kwargs: Any) -> int:
        descriptor = original_open(*args, **kwargs)
        opened.append(descriptor)
        return descriptor

    def faulting_close(descriptor: int) -> None:
        attempts.append(descriptor)
        original_close(descriptor)
        if len(attempts) == failure_attempt:
            raise OSError("CLOSE_SECRET_SENTINEL")

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "close", faulting_close)
    state = load_execution_proof_jwks(_config(path))
    assert not state.available
    assert len(attempts) == len(opened)
    assert set(attempts) == set(opened)
    assert len(attempts) == len(set(attempts))
    assert "CLOSE_SECRET_SENTINEL" not in repr(state)


def test_public_wrong_final_owner_reaches_final_fstat_and_fails_without_chown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    original_fstat = os.fstat
    reached_final = False

    def wrong_final(fd: int) -> Any:
        nonlocal reached_final
        value = original_fstat(fd)
        if stat.S_ISREG(value.st_mode):
            reached_final = True
            return SimpleNamespace(
                st_dev=value.st_dev,
                st_ino=value.st_ino,
                st_uid=os.geteuid() + 100_000,
                st_mode=value.st_mode,
                st_size=value.st_size,
                st_mtime_ns=value.st_mtime_ns,
                st_ctime_ns=value.st_ctime_ns,
            )
        return value

    monkeypatch.setattr(os, "fstat", wrong_final)
    assert not load_execution_proof_jwks(_config(path)).available
    assert reached_final


def test_public_wrong_ancestor_owner_fails_without_chown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    original_fstat = os.fstat
    calls = 0

    def wrong_ancestor(fd: int) -> Any:
        nonlocal calls
        value = original_fstat(fd)
        calls += 1
        if calls == 2:
            return SimpleNamespace(st_mode=value.st_mode, st_uid=os.geteuid() + 100_000)
        return value

    monkeypatch.setattr(os, "fstat", wrong_ancestor)
    assert not load_execution_proof_jwks(_config(path)).available


def test_public_atomic_rename_at_final_path_check_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    replacement = path.parent / "replacement.json"
    replacement.write_bytes(path.read_bytes())
    replacement.chmod(0o644)
    original_stat = os.stat
    replaced = False

    def replacing_stat(*args: Any, **kwargs: Any) -> os.stat_result:
        nonlocal replaced
        if kwargs.get("dir_fd") is not None and not replaced:
            os.replace(replacement, path)
            replaced = True
        return original_stat(*args, **kwargs)

    monkeypatch.setattr(os, "stat", replacing_stat)
    assert not load_execution_proof_jwks(_config(path)).available
    assert replaced


@pytest.mark.parametrize("empty", [False, True])
def test_stable_metadata_short_or_empty_early_eof_fails_closed(
    empty: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    canonical = json.dumps({"keys": [_jwk()]}, separators=(",", ":")).encode()
    path = _file(tmp_path, canonical + b"        ")

    def early_read(_fd: int) -> bytes:
        return b"" if empty else canonical

    monkeypatch.setattr(jwks_loader, "_read_bounded", early_read)
    assert not load_execution_proof_jwks(_config(path)).available


def test_ancestor_symlink_fails_closed(tmp_path: Path) -> None:
    real = _safe_dir(tmp_path)
    path = real / "jwks.json"
    path.write_text(json.dumps({"keys": [_jwk()]}), encoding="utf-8")
    path.chmod(0o644)
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)
    assert not load_execution_proof_jwks(_config(linked / "jwks.json")).available


def test_same_inode_in_place_mutation_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    original = cast(Callable[[int], bytes], getattr(jwks_loader, "_read_bounded"))

    def mutate(fd: int) -> bytes:
        raw = original(fd)
        replacement = raw.replace(b"agent-key-a", b"agent-key-b")
        assert len(replacement) == len(raw)
        path.write_bytes(replacement)
        return raw

    monkeypatch.setattr(jwks_loader, "_read_bounded", mutate)
    assert not load_execution_proof_jwks(_config(path)).available


@pytest.mark.parametrize(
    "flag", ["O_RDONLY", "O_CLOEXEC", "O_NOFOLLOW", "O_NONBLOCK", "O_DIRECTORY"]
)
def test_each_required_public_open_flag_fails_closed_when_absent(
    flag: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(_file(tmp_path))
    monkeypatch.delattr(os, flag)
    assert not load_execution_proof_jwks(config).available


@pytest.mark.parametrize(
    "capability",
    ["_OPEN_SUPPORTS_DIR_FD", "_STAT_SUPPORTS_DIR_FD", "_STAT_SUPPORTS_NOFOLLOW"],
)
def test_each_required_public_syscall_capability_fails_closed(
    capability: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(_file(tmp_path))
    monkeypatch.setattr(jwks_loader, capability, False)
    assert not load_execution_proof_jwks(config).available


@pytest.mark.parametrize("member", ["d", "x5c", "x5u", "jku", "jwk", "crit"])
def test_each_forbidden_jwk_member_fails_closed(member: str, tmp_path: Path) -> None:
    assert not load_execution_proof_jwks(
        _config(_file(tmp_path, {"keys": [_jwk(**{member: "forbidden"})]}))
    ).available


@pytest.mark.parametrize("failure_call", [1, 2, 3])
def test_public_descriptor_closes_every_fd_on_early_walk_failures(
    failure_call: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path)
    original_open, original_close = os.open, os.close
    opened: list[int] = []
    closed: list[int] = []
    calls = 0

    def tracking_open(*args: Any, **kwargs: Any) -> int:
        nonlocal calls
        fd = original_open(*args, **kwargs)
        opened.append(fd)
        calls += 1
        if calls == failure_call:
            original_close(fd)
            opened.pop()
            raise OSError("walk failed")
        return fd

    def tracking_close(fd: int) -> None:
        closed.append(fd)
        original_close(fd)

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "close", tracking_close)
    assert not load_execution_proof_jwks(_config(path)).available
    assert sorted(opened) == sorted(closed)


@pytest.mark.parametrize("stage", ["read", "path_stat", "parse"])
def test_public_descriptor_closes_every_fd_on_final_failure_stages(
    stage: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path, {} if stage == "parse" else None)
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
    if stage == "read":

        def fail_read(_fd: int, _size: int) -> bytes:
            raise OSError("read")

        monkeypatch.setattr(os, "read", fail_read)
    elif stage == "path_stat":
        original_stat = os.stat

        def fail_named_stat(*args: Any, **kwargs: Any) -> os.stat_result:
            if kwargs.get("dir_fd") is not None:
                raise OSError("stat")
            return original_stat(*args, **kwargs)

        monkeypatch.setattr(
            os,
            "stat",
            fail_named_stat,
        )
    assert not load_execution_proof_jwks(_config(path)).available
    assert sorted(opened) == sorted(closed)
