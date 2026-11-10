"""The isolated script's exact stopped container owns the only accepted bytes."""

import json
from pathlib import Path
import subprocess
from typing import Any

import pytest
from pydantic import SecretStr

from kokoro_agent.sandbox import docker_backend
from kokoro_agent.sandbox.docker_backend import DockerShellBackend


class Environment:
    async def acquire(
        self, *, deadline_monotonic: float
    ) -> tuple[tuple[str, SecretStr], ...]:
        return (("API_TOKEN", SecretStr("fixture-only-secret")),)


async def test_exact_stopped_docker_output_recovery_never_reexecutes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    normal = tmp_path / "run-workspace"
    normal.mkdir()
    key = "b" * 64
    container = "c" * 64
    original = "hello 🐦\r\n".encode()
    secret = "fixture-only-secret"
    name = "kokoro-output-" + key
    scratch = tmp_path / (".kokoro-output-" + key)
    state = "absent"
    starts = 0

    def run(*args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal state, starts
        assert secret not in str(args) and secret not in str(kwargs.get("env"))
        if args[0] == "inspect" and args[-1] == "owned-run-container":
            return subprocess.CompletedProcess(args, 0, "sha256:" + "a" * 64, "")
        if args[0] == "create":
            assert (normal / "outputs").is_dir() and scratch.is_dir()
            assert name in args and "kokoro-workspace-output=" + key in args
            assert f"type=bind,source={normal},target=/workspace,readonly" in args
            assert f"type=bind,source={scratch},target=/workspace/outputs" in args
            assert "--log-driver" in args and "none" in args
            assert "print-fixture" not in args
            state = "created"
            return subprocess.CompletedProcess(args, 0, container, "")
        if args[0] == "start":
            assert args[-1] == container and "--interactive" in args
            assert secret in kwargs["script"] and "print-fixture" in kwargs["script"]
            starts += 1
            (scratch / "report.md").write_bytes(original)
            state = "exited"
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "ps" and state == "absent":
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "inspect" and args[-1] == name:
            if state == "absent":
                return subprocess.CompletedProcess(args, 1, "", "")
            snapshot = {
                "id": container,
                "name": "/" + name,
                "key": key,
                "status": state,
                "exit": 0,
                "mounts": [
                    {
                        "Type": "bind",
                        "Source": str(normal),
                        "Destination": "/workspace",
                        "RW": False,
                        "Mode": "",
                        "Propagation": "rprivate",
                    },
                    {
                        "Type": "bind",
                        "Source": str(scratch),
                        "Destination": "/workspace/outputs",
                        "RW": True,
                        "Mode": "",
                        "Propagation": "rprivate",
                    },
                ],
            }
            return subprocess.CompletedProcess(args, 0, json.dumps(snapshot), "")
        if args[0] == "rm" and args[-1] == container:
            state = "absent"
            return subprocess.CompletedProcess(args, 0, "", "")
        raise AssertionError(args)

    monkeypatch.setattr(docker_backend, "_docker", run)
    backend = DockerShellBackend(
        root=normal,
        container_id="owned-run-container",
        timeout=4,
        max_output_bytes=128,
        environment_provider=Environment(),
        docker_cli_environment={},
    )

    capture = backend.aexecute_workspace_output
    assert await capture("print-fixture", "/outputs/report.md", key, 4) == original
    assert await capture("print-fixture", "/outputs/report.md", key, 4) is None
    assert (
        await capture("print-fixture", "/outputs/report.md", key, 4, recover=True)
        == original
    )
    state = "running"
    assert (
        await capture("print-fixture", "/outputs/report.md", key, 4, recover=True)
        is None
    )
    state = "exited"
    artifact = scratch / "report.md"
    artifact.write_bytes(secret.encode())
    assert (
        await capture("print-fixture", "/outputs/report.md", key, 4, recover=True)
        is None
    )
    artifact.unlink()
    ordinary_file = normal / "report.md"
    ordinary_file.write_bytes(b"not the captured output")
    artifact.symlink_to(ordinary_file)
    assert (
        await capture("print-fixture", "/outputs/report.md", key, 4, recover=True)
        is None
    )
    await backend.acleanup_workspace_output(key)
    assert state == "absent" and not scratch.exists()
    assert ordinary_file.read_bytes() == b"not the captured output"
    assert starts == 1
