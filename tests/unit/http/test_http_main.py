"""HTTP-only configuration and composition root."""

from __future__ import annotations

import logging
import hashlib
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
from collections.abc import Callable, Iterator, Mapping

import pytest
from pydantic import SecretStr
from yaml.events import ScalarEvent

import kokoro_agent.config_file as config_file_module
import kokoro_agent.interfaces.http.main as http_main_module
from kokoro_agent.interfaces.http.main import (
    AgentHttpComposition,
    AgentHttpConfig,
    AgentHttpConfigError,
    build_http_composition,
    log_http_config_summary,
)
from kokoro_agent.interfaces.http.execution_proof_jwks import ExecutionProofJwksState

THUMBPRINT = "kPrK_qmxVWaYVA9wwBF6Iuo3vVzz7TxHCTwXBygrS4k"


def _workspace_dirty_hash(workspace: Path) -> str:
    digest = hashlib.sha256()
    digest.update(
        subprocess.check_output(
            ["git", "diff", "--binary", "HEAD", "--"], cwd=workspace
        )
    )
    for relative in sorted(
        subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard"],
            cwd=workspace,
            text=True,
        ).splitlines()
    ):
        digest.update(relative.encode())
        digest.update((workspace / relative).read_bytes())
    return digest.hexdigest()


def _temporary_base(source: dict[str, str], resolver: Callable[[Path], Path]) -> Path:
    candidate = source.get("TMPDIR") or tempfile.gettempdir()
    return resolver(Path(candidate))


def _cleanup_a2b_root(
    root_text: str,
    *,
    base: Path,
    workspace: Path,
    prefix: str,
    identity: tuple[int, int, int, int],
) -> None:
    if type(root_text) is not str or not root_text:
        raise RuntimeError("unsafe A2b cleanup target")
    target = Path(root_text)
    if (
        target == Path("/")
        or target == workspace
        or target in workspace.parents
        or workspace in target.parents
        or target.parent != base
        or not target.name.startswith(prefix)
    ):
        raise RuntimeError("unsafe A2b cleanup target")
    current = os.lstat(target)
    if (
        stat.S_ISLNK(current.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or (current.st_dev, current.st_ino, current.st_uid, current.st_mode) != identity
        or shutil.rmtree.avoids_symlink_attacks is not True
    ):
        raise RuntimeError("A2b temporary root identity changed")
    shutil.rmtree(target)
    if os.path.lexists(target):
        raise RuntimeError("A2b temporary root cleanup failed")


def _unlink_guarded_test_symlink(
    root_text: str,
    *,
    base: Path,
    prefix: str,
    identity: tuple[int, int, int, int],
    target: Path,
) -> None:
    link = Path(root_text)
    current = os.lstat(link)
    if (
        link.parent != base
        or not link.name.startswith(prefix)
        or not stat.S_ISLNK(current.st_mode)
        or (current.st_dev, current.st_ino, current.st_uid, current.st_mode) != identity
        or Path(os.readlink(link)) != target
    ):
        raise RuntimeError("unsafe A2b test symlink cleanup")
    os.unlink(link)
    if os.path.lexists(link):
        raise RuntimeError("A2b test symlink cleanup failed")


def _exercise_cleanup_harness_boundaries() -> None:
    workspace = Path.cwd().resolve(strict=True)
    before_hash = _workspace_dirty_hash(workspace)

    def resolve(path: Path) -> Path:
        return path.resolve(strict=True)

    def fail_resolve(_path: Path) -> Path:
        raise OSError("canonicalization")

    base = _temporary_base({}, resolve)
    assert base == Path(tempfile.gettempdir()).resolve(strict=True)
    with pytest.raises(OSError):
        _temporary_base({"TMPDIR": "/missing"}, fail_resolve)
    prefix = "kokoro-agent-a2b-test-"

    def create() -> tuple[str, tuple[int, int, int, int]]:
        text = tempfile.mkdtemp(prefix=prefix, dir=base)
        value = os.lstat(text)
        return text, (value.st_dev, value.st_ino, value.st_uid, value.st_mode)

    normal, identity = create()
    _cleanup_a2b_root(
        normal, base=base, workspace=workspace, prefix=prefix, identity=identity
    )
    assert not os.path.lexists(normal)

    dangerous = [
        "",
        "/",
        str(workspace),
        str(workspace.parent),
        str(workspace / "inside"),
    ]
    for target in dangerous:
        with pytest.raises(RuntimeError):
            _cleanup_a2b_root(
                target,
                base=base,
                workspace=workspace,
                prefix=prefix,
                identity=(0, 0, 0, 0),
            )

    other_prefix = "kokoro-agent-a2b-other-parent-"
    other_text = tempfile.mkdtemp(prefix=other_prefix, dir=base)
    other_parent = Path(other_text)
    other_stat = os.lstat(other_parent)
    other_identity = (
        other_stat.st_dev,
        other_stat.st_ino,
        other_stat.st_uid,
        other_stat.st_mode,
    )
    sentinel = other_parent / "PREEXISTING_SENTINEL"
    sentinel.write_text("preserve", encoding="utf-8")
    other = other_parent / f"{prefix}different"
    other.mkdir()
    try:
        value = os.lstat(other)
        with pytest.raises(RuntimeError):
            _cleanup_a2b_root(
                str(other),
                base=base,
                workspace=workspace,
                prefix=prefix,
                identity=(value.st_dev, value.st_ino, value.st_uid, value.st_mode),
            )
        assert sentinel.read_text(encoding="utf-8") == "preserve"
    finally:
        _cleanup_a2b_root(
            other_text,
            base=base,
            workspace=workspace,
            prefix=other_prefix,
            identity=other_identity,
        )

    changed, identity = create()
    replacement = os.lstat(changed)
    _cleanup_a2b_root(
        changed,
        base=base,
        workspace=workspace,
        prefix=prefix,
        identity=(
            replacement.st_dev,
            replacement.st_ino,
            replacement.st_uid,
            replacement.st_mode,
        ),
    )
    Path(changed).mkdir()
    with pytest.raises(RuntimeError):
        _cleanup_a2b_root(
            changed, base=base, workspace=workspace, prefix=prefix, identity=identity
        )
    replacement = os.lstat(changed)
    _cleanup_a2b_root(
        changed,
        base=base,
        workspace=workspace,
        prefix=prefix,
        identity=(
            replacement.st_dev,
            replacement.st_ino,
            replacement.st_uid,
            replacement.st_mode,
        ),
    )

    linked, identity = create()
    victim = Path(tempfile.mkdtemp(prefix="kokoro-agent-a2b-victim-", dir=base))
    (victim / "sentinel").write_text("preserve", encoding="utf-8")
    _cleanup_a2b_root(
        linked, base=base, workspace=workspace, prefix=prefix, identity=identity
    )
    os.symlink(victim, linked)
    link_stat = os.lstat(linked)
    victim_stat = os.lstat(victim)
    victim_identity = (
        victim_stat.st_dev,
        victim_stat.st_ino,
        victim_stat.st_uid,
        victim_stat.st_mode,
    )
    try:
        with pytest.raises(RuntimeError):
            _cleanup_a2b_root(
                linked, base=base, workspace=workspace, prefix=prefix, identity=identity
            )
        assert (victim / "sentinel").read_text(encoding="utf-8") == "preserve"
    finally:
        _unlink_guarded_test_symlink(
            linked,
            base=base,
            prefix=prefix,
            identity=(
                link_stat.st_dev,
                link_stat.st_ino,
                link_stat.st_uid,
                link_stat.st_mode,
            ),
            target=victim,
        )
        _cleanup_a2b_root(
            str(victim),
            base=base,
            workspace=workspace,
            prefix="kokoro-agent-a2b-victim-",
            identity=victim_identity,
        )

    guarded, identity = create()
    original = shutil.rmtree.avoids_symlink_attacks
    shutil.rmtree.avoids_symlink_attacks = False
    try:
        with pytest.raises(RuntimeError):
            _cleanup_a2b_root(
                guarded,
                base=base,
                workspace=workspace,
                prefix=prefix,
                identity=identity,
            )
    finally:
        shutil.rmtree.avoids_symlink_attacks = original
        _cleanup_a2b_root(
            guarded,
            base=base,
            workspace=workspace,
            prefix=prefix,
            identity=identity,
        )
    assert _workspace_dirty_hash(workspace) == before_hash


def test_http_config_has_exact_public_field_set_only() -> None:
    assert set(AgentHttpConfig.model_fields) == {
        "host",
        "port",
        "redis_url",
        "database_url",
        "database_schema",
        "lease_ttl_s",
        "internal_secret_agent",
        "execution_proof",
    }
    assert all(
        "private" not in name and "worker" not in name
        for name in AgentHttpConfig.model_fields
    )


def test_http_business_defaults_and_env_override() -> None:
    config = AgentHttpConfig.from_env({"KOKORO_AGENT_HTTP_PORT": "4510"})
    assert config.host == "127.0.0.1"
    assert config.port == 4510
    assert config.execution_proof is None


def test_http_business_env_overrides_yaml(tmp_path: Path) -> None:
    config_file = tmp_path / "agent.yaml"
    config_file.write_text(
        "stream:\n  redis_url: redis://yaml\ndatabase:\n  schema: from_yaml\n",
        encoding="utf-8",
    )
    config = AgentHttpConfig.from_env(
        {
            "KOKORO_AGENT_CONFIG": str(config_file),
            "KOKORO_REDIS_URL": "redis://env",
        }
    )
    assert config.redis_url == "redis://env"
    assert config.database_schema == "from_yaml"


def test_http_business_full_default_yaml_env_precedence(tmp_path: Path) -> None:
    config_file = tmp_path / "agent.yaml"
    config_file.write_text(
        "http:\n  host: 0.0.0.0\n  port: 4500\nstream:\n  redis_url: redis://yaml\n"
        "database:\n  url: postgres://yaml\n  schema: yaml_schema\n"
        "run_repository:\n  lease_ttl_s: 45\n",
        encoding="utf-8",
    )
    yaml = AgentHttpConfig.from_env({"KOKORO_AGENT_CONFIG": str(config_file)})
    assert (
        yaml.host,
        yaml.port,
        yaml.redis_url,
        yaml.database_url,
        yaml.database_schema,
        yaml.lease_ttl_s,
    ) == ("0.0.0.0", 4500, "redis://yaml", "postgres://yaml", "yaml_schema", 45)
    env = AgentHttpConfig.from_env(
        {
            "KOKORO_AGENT_CONFIG": str(config_file),
            "KOKORO_AGENT_HTTP_HOST": "127.1.2.3",
            "KOKORO_AGENT_HTTP_PORT": "4600",
            "KOKORO_REDIS_URL": "redis://env",
            "KOKORO_AGENT_DATABASE_URL": "postgres://env",
            "KOKORO_AGENT_DATABASE_SCHEMA": "env_schema",
            "KOKORO_LEASE_TTL_S": "46",
        }
    )
    assert (
        env.host,
        env.port,
        env.redis_url,
        env.database_url,
        env.database_schema,
        env.lease_ttl_s,
    ) == ("127.1.2.3", 4600, "redis://env", "postgres://env", "env_schema", 46)


def test_http_env_mapping_is_never_iterated_or_read_outside_exact_allowlist() -> None:
    allowed = {
        "KOKORO_AGENT_CONFIG",
        "KOKORO_AGENT_HTTP_HOST",
        "KOKORO_AGENT_HTTP_PORT",
        "KOKORO_REDIS_URL",
        "KOKORO_AGENT_DATABASE_URL",
        "KOKORO_AGENT_DATABASE_SCHEMA",
        "KOKORO_LEASE_TTL_S",
        "KOKORO_INTERNAL_SECRET_AGENT",
        "KOKORO_AGENT_EXECUTION_PROOF_PUBLIC_JWKS_FILE",
        "KOKORO_AGENT_EXECUTION_PROOF_HTTP_ACTIVE_KID",
        "KOKORO_AGENT_EXECUTION_PROOF_HTTP_ACTIVE_JWK_THUMBPRINT_SHA256",
    }

    class ExactSource(Mapping[str, str]):
        def __getitem__(self, key: str) -> str:
            if key not in allowed:
                raise AssertionError(f"non-HTTP key read: {key}")
            if key == "KOKORO_AGENT_HTTP_PORT":
                return "4512"
            raise KeyError(key)

        def __iter__(self) -> Iterator[str]:
            raise AssertionError("HTTP source must not be iterated")

        def __len__(self) -> int:
            raise AssertionError("HTTP source length must not be read")

    config = AgentHttpConfig.from_env(ExactSource())
    assert config.port == 4512


def test_worker_yaml_values_do_not_enter_http_projection_or_repr(
    tmp_path: Path,
) -> None:
    config_file = tmp_path / "agent.yaml"
    config_file.write_text(
        "http:\n  port: 4513\nmodel:\n  openai_base_url: MODEL_SENTINEL\n"
        "sandbox:\n  docker:\n    image: SANDBOX_SENTINEL\n"
        "mcp:\n  config: MCP_SENTINEL\n",
        encoding="utf-8",
    )
    config = AgentHttpConfig.from_env({"KOKORO_AGENT_CONFIG": str(config_file)})
    assert config.port == 4513
    rendered = repr(config)
    for sentinel in ("MODEL_SENTINEL", "SANDBOX_SENTINEL", "MCP_SENTINEL"):
        assert sentinel not in rendered


def test_http_yaml_constructs_only_exact_http_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "agent.yaml"
    config_path.write_text(
        "http:\n  port: 4514\nmodel:\n  openai_base_url: MODEL_SENTINEL\n"
        "  provider: PROVIDER_SENTINEL\n  private: PRIVATE_SENTINEL\n"
        "sandbox:\n  docker:\n    image: SANDBOX_SENTINEL\n"
        "mcp:\n  config: MCP_SENTINEL\n",
        encoding="utf-8",
    )
    constructed: list[str] = []
    original: Callable[[ScalarEvent], object] = getattr(
        config_file_module, "_http_scalar_value"
    )

    def tracking(event: ScalarEvent) -> object:
        value = original(event)
        constructed.append(repr(value))
        return value

    monkeypatch.setattr(config_file_module, "_http_scalar_value", tracking)
    config = AgentHttpConfig.from_env({"KOKORO_AGENT_CONFIG": str(config_path)})
    assert config.port == 4514
    assert all(
        sentinel not in "".join(constructed)
        for sentinel in (
            "MODEL_SENTINEL",
            "PROVIDER_SENTINEL",
            "PRIVATE_SENTINEL",
            "SANDBOX_SENTINEL",
            "MCP_SENTINEL",
        )
    )


def test_http_yaml_does_not_compose_full_worker_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "agent.yaml"
    config_path.write_text(
        "http:\n  port: 4515\nmodel:\n  openai_base_url: WORKER_VALUE_SENTINEL\n",
        encoding="utf-8",
    )

    def reject_compose(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("HTTP root must not compose the complete worker YAML tree")

    monkeypatch.setattr(config_file_module.yaml, "compose", reject_compose)
    assert (
        AgentHttpConfig.from_env({"KOKORO_AGENT_CONFIG": str(config_path)}).port == 4515
    )


def test_http_yaml_projection_accepts_quoted_string_keys(tmp_path: Path) -> None:
    config_path = tmp_path / "agent.yaml"
    config_path.write_text('"http":\n  "port": 4516\n', encoding="utf-8")
    assert (
        AgentHttpConfig.from_env({"KOKORO_AGENT_CONFIG": str(config_path)}).port == 4516
    )


def test_http_yaml_scalar_tags_preserve_safe_loader_semantics(tmp_path: Path) -> None:
    config_path = tmp_path / "agent.yaml"
    config_path.write_text(
        "http:\n  host: !!str null\n  port: !!int '4517'\n"
        "stream:\n  redis_url: !!str false\n",
        encoding="utf-8",
    )
    projected = config_file_module.load_http_config_file(str(config_path))
    assert projected == {
        "KOKORO_AGENT_HTTP_HOST": "null",
        "KOKORO_AGENT_HTTP_PORT": 4517,
        "KOKORO_REDIS_URL": "false",
    }


def test_http_yaml_unsupported_explicit_tag_fails_closed(tmp_path: Path) -> None:
    config_path = tmp_path / "agent.yaml"
    config_path.write_text(
        "http:\n  host: !UNSUPPORTED_TAG_SENTINEL accepted\n", encoding="utf-8"
    )
    error_type = getattr(http_main_module, "AgentHttpConfigError")
    with pytest.raises(error_type) as caught:
        AgentHttpConfig.from_env({"KOKORO_AGENT_CONFIG": str(config_path)})
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert "UNSUPPORTED_TAG_SENTINEL" not in repr(caught.value)


@pytest.mark.parametrize("failure", ["numeric", "missing", "malformed"])
def test_http_config_errors_are_stable_and_drop_sensitive_exception_chain(
    tmp_path: Path, failure: str
) -> None:
    sentinel = f"{failure.upper()}_CONFIG_SENTINEL"
    if failure == "numeric":
        source = {"KOKORO_AGENT_HTTP_PORT": sentinel}
    elif failure == "missing":
        source = {"KOKORO_AGENT_CONFIG": str(tmp_path / sentinel)}
    else:
        path = tmp_path / "agent.yaml"
        path.write_text(f"http:\n  port: [{sentinel}\n", encoding="utf-8")
        source = {"KOKORO_AGENT_CONFIG": str(path)}
    error_type = getattr(http_main_module, "AgentHttpConfigError")
    with pytest.raises(error_type) as caught:
        AgentHttpConfig.from_env(source)
    error = caught.value
    assert str(error) == "Agent HTTP configuration is invalid"
    assert sentinel not in repr(error)
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize("failure", ["numeric", "missing", "malformed"])
def test_http_entrypoint_reports_only_sanitized_config_failure(
    tmp_path: Path, failure: str
) -> None:
    sentinel = f"{failure.upper()}_ENTRYPOINT_SENTINEL"
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"KOKORO_AGENT_CONFIG", "KOKORO_AGENT_HTTP_PORT"}
    }
    if failure == "numeric":
        environment["KOKORO_AGENT_HTTP_PORT"] = sentinel
    elif failure == "missing":
        environment["KOKORO_AGENT_CONFIG"] = str(tmp_path / sentinel)
    else:
        path = tmp_path / "agent.yaml"
        path.write_text(f"http:\n  port: [{sentinel}\n", encoding="utf-8")
        environment["KOKORO_AGENT_CONFIG"] = str(path)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from kokoro_agent.interfaces.http.main import main; main()",
        ],
        cwd=tmp_path,
        env=environment,
        text=True,
        capture_output=True,
        timeout=3,
    )
    exposed = result.stdout + result.stderr
    assert result.returncode != 0
    assert exposed.count("Agent HTTP configuration is invalid") == 1
    assert sentinel not in exposed
    assert "Traceback" not in exposed


def test_http_entrypoint_does_not_load_dotenv(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "A2B_HTTP_DOTENV_SENTINEL=DOTENV_VALUE_SENTINEL\n", encoding="utf-8"
    )
    environment = os.environ.copy()
    environment.pop("A2B_HTTP_DOTENV_SENTINEL", None)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from kokoro_agent.interfaces.http import main as m; "
            "m.build_http_composition=lambda source: "
            "(_ for _ in ()).throw(SystemExit(source.get('A2B_HTTP_DOTENV_SENTINEL','ABSENT'))); "
            "m.main()",
        ],
        cwd=tmp_path,
        env=environment,
        text=True,
        capture_output=True,
        timeout=3,
    )
    assert result.returncode != 0
    assert "ABSENT" in result.stderr
    assert "DOTENV_VALUE_SENTINEL" not in result.stderr


def test_public_proof_cannot_enter_through_yaml(tmp_path: Path) -> None:
    config_file = tmp_path / "agent.yaml"
    config_file.write_text(
        "execution_proof:\n  public_jwks_file: /ring.json\n", encoding="utf-8"
    )
    with pytest.raises(AgentHttpConfigError):
        AgentHttpConfig.from_env({"KOKORO_AGENT_CONFIG": str(config_file)})


def test_cleanup_harness_boundaries() -> None:
    # RED first references the test-only helper; production must not own cleanup verification.
    _exercise_cleanup_harness_boundaries()


@pytest.mark.parametrize("port", ["0", "65536", "nan"])
def test_invalid_business_config_fails_startup(port: str) -> None:
    with pytest.raises(AgentHttpConfigError):
        AgentHttpConfig.from_env({"KOKORO_AGENT_HTTP_PORT": port})


def test_public_proof_descriptor_is_env_only_and_missing_degrades() -> None:
    composition = build_http_composition({})
    assert composition.config.execution_proof is None
    assert composition.jwks.available is False


def test_complete_public_descriptor_constructs_but_bad_ring_degrades() -> None:
    source = {
        "KOKORO_AGENT_EXECUTION_PROOF_PUBLIC_JWKS_FILE": "/missing/ring.json",
        "KOKORO_AGENT_EXECUTION_PROOF_HTTP_ACTIVE_KID": "key-a",
        "KOKORO_AGENT_EXECUTION_PROOF_HTTP_ACTIVE_JWK_THUMBPRINT_SHA256": THUMBPRINT,
    }
    composition = build_http_composition(source)
    assert composition.config.execution_proof is not None
    assert composition.jwks.available is False


def test_partial_or_invalid_public_descriptor_degrades_without_exception() -> None:
    for source in (
        {"KOKORO_AGENT_EXECUTION_PROOF_PUBLIC_JWKS_FILE": "/ring.json"},
        {
            "KOKORO_AGENT_EXECUTION_PROOF_PUBLIC_JWKS_FILE": "relative",
            "KOKORO_AGENT_EXECUTION_PROOF_HTTP_ACTIVE_KID": "kid",
            "KOKORO_AGENT_EXECUTION_PROOF_HTTP_ACTIVE_JWK_THUMBPRINT_SHA256": THUMBPRINT,
        },
    ):
        composition = build_http_composition(source)
        assert composition.config.execution_proof is None
        assert not composition.jwks.available


def test_safe_config_log_is_allowlisted(caplog: pytest.LogCaptureFixture) -> None:
    source = {
        "KOKORO_REDIS_URL": "redis://REDIS_SENTINEL",
        "KOKORO_AGENT_DATABASE_URL": "postgres://DB_SENTINEL",
        "KOKORO_INTERNAL_SECRET_AGENT": "TOKEN_SENTINEL",
        "KOKORO_AGENT_EXECUTION_PROOF_PUBLIC_JWKS_FILE": "/PATH_SENTINEL",
        "KOKORO_AGENT_EXECUTION_PROOF_HTTP_ACTIVE_KID": "KID_SENTINEL",
        "KOKORO_AGENT_EXECUTION_PROOF_HTTP_ACTIVE_JWK_THUMBPRINT_SHA256": THUMBPRINT,
    }
    composition = build_http_composition(source)
    with caplog.at_level(logging.INFO):
        log_http_config_summary(composition, logging.getLogger("test"))
    assert "ring_available" in caplog.text
    for sentinel in (
        "REDIS_SENTINEL",
        "DB_SENTINEL",
        "TOKEN_SENTINEL",
        "PATH_SENTINEL",
        "KID_SENTINEL",
        THUMBPRINT,
    ):
        assert sentinel not in caplog.text
    assert "private_key" not in repr(composition.config)


def test_config_summary_classifies_bind_and_blank_secret_without_raw_host(
    caplog: pytest.LogCaptureFixture,
) -> None:
    composition = AgentHttpComposition(
        config=AgentHttpConfig(
            host="HOST_SENTINEL.example",
            internal_secret_agent=SecretStr("   "),
        ),
        jwks=ExecutionProofJwksState.unavailable(),
    )
    with caplog.at_level(logging.INFO):
        log_http_config_summary(composition, logging.getLogger("bind-summary-test"))
    assert "HOST_SENTINEL" not in caplog.text
    assert "bind_mode" in caplog.text
    assert "service_auth_configured': False" in caplog.text
