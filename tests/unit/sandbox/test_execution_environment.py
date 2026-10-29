"""Run-local environment injection; external sandbox I/O is explicitly doubled."""

from __future__ import annotations

import asyncio
import os
import subprocess
import threading
from pathlib import Path

import pytest
from pydantic import SecretStr
from e2b import CommandExitException

from kokoro_agent.sandbox.docker_backend import DockerShellBackend
from kokoro_agent.sandbox.docker_backend import (
    DockerSettings,
    connect_docker_sandbox,
    destroy_docker_sandbox,
)
from kokoro_agent.sandbox.e2b_backend import E2BSandboxBackend
from kokoro_agent.sandbox.execution_environment import ExecutionEnvironmentRunner


class RotatingEnvironment:
    def __init__(self) -> None:
        self.calls = 0

    async def acquire(
        self, *, deadline_monotonic: float
    ) -> tuple[tuple[str, SecretStr], ...]:
        self.calls += 1
        return (("API_KEY", SecretStr(f"secret-value-{self.calls}")),)


@pytest.mark.asyncio
async def test_docker_reacquires_environment_without_putting_values_in_argv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[tuple[str, ...], dict[str, str]]] = []

    def run(
        *args: str, timeout: int = 30, env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        assert env is not None
        calls.append((args, dict(env)))
        return subprocess.CompletedProcess(args, 0, "ok " + env["API_KEY"], " err")

    monkeypatch.setattr("kokoro_agent.sandbox.docker_backend._docker", run)
    provider = RotatingEnvironment()
    backend = DockerShellBackend(
        root=tmp_path,
        container_id="test",
        timeout=5,
        max_output_bytes=1000,
        environment_provider=provider,
    )
    first = await backend.aexecute("command")
    second = await backend.aexecute("command")
    assert provider.calls == 2
    assert first.output == second.output == "ok [REDACTED] err"
    assert [env["API_KEY"] for _, env in calls] == ["secret-value-1", "secret-value-2"]
    assert all("--env" in args and "API_KEY" in args for args, _ in calls)
    assert all("secret-value" not in repr(args) for args, _ in calls)


class Environment:
    def __init__(self, *entries: tuple[str, str]) -> None:
        self.entries = tuple((name, SecretStr(value)) for name, value in entries)

    async def acquire(
        self, *, deadline_monotonic: float
    ) -> tuple[tuple[str, SecretStr], ...]:
        return self.entries


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entries",
    [
        (("API_KEY", "one"), ("API_KEY", "one")),
        (("PATH", "injected"),),
        (("LD_PRELOAD", "injected"),),
        (("PYTHONPATH", "injected"),),
        (("HTTPS_PROXY", "injected"),),
        (("BASH_ENV", "injected"),),
        (("DOCKER_HOST", "injected"),),
        (("LANG", "injected"),),
        (("JAVA_TOOL_OPTIONS", "injected"),),
        (("OPENSSL_CONF", "injected"),),
        (("NODE_EXTRA_CA_CERTS", "injected"),),
        (("SSLKEYLOGFILE", "injected"),),
        (("api_key", "injected"),),
        (("API_KEY", ""),),
        (("API_KEY", "bad\nvalue"),),
        (("API_KEY", "x" * 4097),),
    ],
)
async def test_invalid_or_conflicting_environment_never_executes(
    entries: tuple[tuple[str, str], ...],
) -> None:
    def execute(values: dict[str, str], timeout: int):
        pytest.fail("invalid environment reached the sandbox")

    result = await ExecutionEnvironmentRunner(
        Environment(*entries), max_output_bytes=100
    ).aexecute(5, execute)
    assert result.output == "sandbox_environment_unavailable"


@pytest.mark.asyncio
async def test_provider_failure_does_not_expose_exception_or_execute() -> None:
    class Failing:
        async def acquire(
            self, *, deadline_monotonic: float
        ) -> tuple[tuple[str, SecretStr], ...]:
            raise RuntimeError("private-provider-value")

    def execute(values: dict[str, str], timeout: int):
        pytest.fail("failed authorization reached the sandbox")

    result = await ExecutionEnvironmentRunner(Failing(), max_output_bytes=100).aexecute(
        5, execute
    )
    assert result.output == "sandbox_environment_unavailable"
    assert "private-provider-value" not in repr(result)


@pytest.mark.asyncio
async def test_cancelled_acquisition_never_dispatches() -> None:
    entered, cancelled = asyncio.Event(), asyncio.Event()

    class Pending:
        async def acquire(
            self, *, deadline_monotonic: float
        ) -> tuple[tuple[str, SecretStr], ...]:
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
            return ()

    def execute(values: dict[str, str], timeout: int):
        pytest.fail("cancelled authorization reached the sandbox")

    task = asyncio.create_task(
        ExecutionEnvironmentRunner(Pending(), max_output_bytes=100).aexecute(5, execute)
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_cancellation_owns_started_thread_until_completion() -> None:
    from deepagents.backends.protocol import ExecuteResponse

    entered = asyncio.Event()
    release = threading.Event()
    exited = threading.Event()
    loop = asyncio.get_running_loop()

    def execute(values: dict[str, str], timeout: int) -> ExecuteResponse:
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        exited.set()
        return ExecuteResponse(output=values["API_KEY"], exit_code=0, truncated=False)

    task = asyncio.create_task(
        ExecutionEnvironmentRunner(
            Environment(("API_KEY", "private-value")), max_output_bytes=100
        ).aexecute(5, execute)
    )
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert exited.is_set()


@pytest.mark.asyncio
async def test_concurrent_calls_do_not_share_environment_or_redaction_set() -> None:
    from deepagents.backends.protocol import ExecuteResponse

    barrier = threading.Barrier(2)
    observed: list[str] = []

    def execute(values: dict[str, str], timeout: int) -> ExecuteResponse:
        value = values["API_KEY"]
        observed.append(value)
        barrier.wait(timeout=5)
        return ExecuteResponse(output="result:" + value, exit_code=0, truncated=False)

    provider = RotatingEnvironment()
    runner = ExecutionEnvironmentRunner(provider, max_output_bytes=100)
    results = await asyncio.gather(
        runner.aexecute(5, execute), runner.aexecute(5, execute)
    )
    assert set(observed) == {"secret-value-1", "secret-value-2"}
    assert all(result.output == "result:[REDACTED]" for result in results)


@pytest.mark.asyncio
async def test_provider_rotation_does_not_change_inflight_redaction_material() -> None:
    from deepagents.backends.protocol import ExecuteResponse

    provider = Environment(("API_KEY", "original-private-value"))

    def execute(values: dict[str, str], timeout: int) -> ExecuteResponse:
        # SecretStr hides repr but is mutable: a provider-owned instance must
        # not also own the redaction material of an in-flight invocation.
        setattr(provider.entries[0][1], "_secret_value", "replacement-value")
        return ExecuteResponse(output=values["API_KEY"], exit_code=0, truncated=False)

    result = await ExecutionEnvironmentRunner(provider, max_output_bytes=100).aexecute(
        5, execute
    )
    assert result.output == "[REDACTED]"


@pytest.mark.asyncio
async def test_sync_thread_bridge_and_owner_loop_deadlock_guard() -> None:
    from deepagents.backends.protocol import ExecuteResponse

    owner = asyncio.get_running_loop()

    class Bound:
        async def acquire(
            self, *, deadline_monotonic: float
        ) -> tuple[tuple[str, SecretStr], ...]:
            assert asyncio.get_running_loop() is owner
            return (("API_KEY", SecretStr("private-value")),)

    def execute(values: dict[str, str], timeout: int) -> ExecuteResponse:
        return ExecuteResponse(output="plain output", exit_code=7, truncated=False)

    runner = ExecutionEnvironmentRunner(Bound(), max_output_bytes=100, owner_loop=owner)
    result = await asyncio.to_thread(runner.execute, 5, execute)
    assert (result.output, result.exit_code, result.truncated) == (
        "plain output",
        7,
        False,
    )
    assert runner.execute(5, execute).output == "sandbox_environment_sync_on_owner_loop"


class Result:
    def __init__(self, stdout: str, stderr: str, exit_code: int = 0) -> None:
        self.stdout, self.stderr, self.exit_code = stdout, stderr, exit_code


class Files:
    def write(self, path: str, data: str | bytes) -> object:
        return None

    def read(self, path: str, format: str) -> object:
        return b"file"


class Commands:
    def __init__(self, failure: str | None = None) -> None:
        self.failure = failure
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    def run(
        self,
        cmd: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        envs: dict[str, str] | None = None,
    ) -> Result:
        self.calls.append((cmd, dict(envs) if envs is not None else None))
        if envs is None:
            return Result("plain", " output")
        value = envs["API_KEY"]
        if self.failure == "exception":
            raise RuntimeError("SDK error " + value)
        if self.failure == "exit":
            raise CommandExitException(
                stdout="out " + value,
                stderr=" err " + value,
                exit_code=2,
                error="failed " + value,
            )
        return Result("out " + value, " err " + value)


class Sandbox:
    def __init__(self, failure: str | None = None) -> None:
        self.commands = Commands(failure)
        self.files = Files()
        self.sandbox_id = "fake"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "exit", "exception"])
async def test_e2b_envs_and_error_outputs_are_sanitized(failure: str | None) -> None:
    sandbox = Sandbox(failure)
    backend = E2BSandboxBackend(
        sandbox, environment_provider=Environment(("API_KEY", "private-value"))
    )
    result = await backend.aexecute("unchanged command", timeout=5)
    assert "private-value" not in repr(result)
    assert sandbox.commands.calls == [
        ("unchanged command", {"API_KEY": "private-value"})
    ]
    if failure == "exception":
        assert result.output == "sandbox_execution_failed"
    else:
        assert result.output == "out [REDACTED] err [REDACTED]"
        assert result.exit_code == (2 if failure == "exit" else 0)


@pytest.mark.asyncio
async def test_e2b_no_provider_is_unchanged() -> None:
    sandbox = Sandbox()
    result = await E2BSandboxBackend(sandbox).aexecute("plain command")
    assert result.output == "plain output"
    assert sandbox.commands.calls == [("plain command", None)]


@pytest.mark.asyncio
async def test_redaction_precedes_byte_clipping_and_hides_repr() -> None:
    from deepagents.backends.protocol import ExecuteResponse

    provider = Environment(
        ("API_KEY", "private-long-secret"), ("OTHER_KEY", "private-long-secret-suffix")
    )
    runner = ExecutionEnvironmentRunner(provider, max_output_bytes=12)
    result = await runner.aexecute(
        5,
        lambda values, timeout: ExecuteResponse(
            output="ok " + values["OTHER_KEY"], exit_code=0, truncated=False
        ),
    )
    assert result.output == "ok [REDACTED"
    assert result.truncated
    assert "private-long-secret" not in repr(provider.entries)


@pytest.mark.asyncio
async def test_docker_cli_has_isolated_child_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert args == [
            "/fake/docker",
            "exec",
            "--env",
            "API_KEY",
            "-w",
            "/workspace",
            "sandbox",
            "sh",
            "-lc",
            "true",
        ]
        assert kwargs["env"] == {
            "PATH": os.defpath,
            "API_KEY": "private-value",
        }
        return subprocess.CompletedProcess(args, 0, "ok", "")

    monkeypatch.setenv("WORKER_CREDENTIAL", "do-not-inherit")
    monkeypatch.setenv("API_KEY", "parent-unselected")

    def executable(name: str) -> str:
        return "/fake/docker"

    monkeypatch.setattr("kokoro_agent.sandbox.docker_backend.shutil.which", executable)
    monkeypatch.setattr("kokoro_agent.sandbox.docker_backend.subprocess.run", run)
    backend = DockerShellBackend(
        root=tmp_path,
        container_id="sandbox",
        timeout=5,
        max_output_bytes=100,
        environment_provider=Environment(("API_KEY", "private-value")),
    )
    assert (await backend.aexecute("true")).output == "ok"
    assert os.environ["API_KEY"] == "parent-unselected"
    assert os.environ["WORKER_CREDENTIAL"] == "do-not-inherit"


@pytest.mark.asyncio
async def test_docker_cli_context_is_explicit_and_not_injected_into_container(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert kwargs["env"] == {
            "PATH": os.defpath,
            "HOME": "/fixture/home",
            "DOCKER_CONTEXT": "fixture",
            "API_KEY": "private-value",
        }
        assert "HOME" not in args and "DOCKER_CONTEXT" not in args
        return subprocess.CompletedProcess(args, 0, "ok", "")

    def executable(name: str) -> str:
        return "/fake/docker"

    monkeypatch.setattr("kokoro_agent.sandbox.docker_backend.shutil.which", executable)
    monkeypatch.setattr("kokoro_agent.sandbox.docker_backend.subprocess.run", run)
    backend = DockerShellBackend(
        root=tmp_path,
        container_id="sandbox",
        timeout=5,
        max_output_bytes=100,
        environment_provider=Environment(("API_KEY", "private-value")),
        docker_cli_environment={"HOME": "/fixture/home", "DOCKER_CONTEXT": "fixture"},
    )
    assert (await backend.aexecute("true")).output == "ok"


@pytest.mark.parametrize(
    "context",
    [
        {"WORKER_SECRET": "private-value"},
        {"DOCKER_HOST": "tcp://user:private-value@host:2376"},
    ],
)
def test_docker_cli_context_rejects_credentials_with_static_error(
    tmp_path: Path, context: dict[str, str]
) -> None:
    with pytest.raises(ValueError, match="^invalid_docker_cli_environment$"):
        DockerShellBackend(
            root=tmp_path,
            container_id="sandbox",
            timeout=5,
            max_output_bytes=100,
            environment_provider=Environment(("API_KEY", "private-value")),
            docker_cli_environment=context,
        )


@pytest.mark.asyncio
async def test_expired_budget_does_not_acquire_or_execute() -> None:
    provider = RotatingEnvironment()

    def execute(values: dict[str, str], timeout: int):
        pytest.fail("expired budget reached the sandbox")

    result = await ExecutionEnvironmentRunner(provider, max_output_bytes=100).aexecute(
        0, execute
    )
    assert result.exit_code != 0
    assert provider.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["sdk", "timeout"])
async def test_docker_operation_exception_never_returns_raw_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    def run(
        *args: str, timeout: int = 30, env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        assert env is not None
        if failure == "timeout":
            raise subprocess.TimeoutExpired(
                args, timeout, output=env["API_KEY"], stderr=env["API_KEY"]
            )
        raise RuntimeError("failed with " + env["API_KEY"])

    monkeypatch.setattr("kokoro_agent.sandbox.docker_backend._docker", run)
    backend = DockerShellBackend(
        root=tmp_path,
        container_id="sandbox",
        timeout=5,
        max_output_bytes=100,
        environment_provider=Environment(("API_KEY", "private-value")),
    )
    result = await backend.aexecute("unchanged")
    assert result.output == "sandbox_execution_failed"
    assert "private-value" not in repr(result)


@pytest.mark.asyncio
async def test_docker_without_provider_preserves_output_and_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(*args: str, timeout: int = 30) -> subprocess.CompletedProcess[str]:
        if args[-1] == "timeout":
            raise subprocess.TimeoutExpired(args, timeout)
        return subprocess.CompletedProcess(args, 3, "plain", " output")

    monkeypatch.setattr("kokoro_agent.sandbox.docker_backend._docker", run)
    backend = DockerShellBackend(
        root=tmp_path, container_id="sandbox", timeout=5, max_output_bytes=100
    )
    result = await backend.aexecute("unchanged")
    assert (result.output, result.exit_code) == ("plain output", 3)
    timed_out = await backend.aexecute("timeout")
    assert (timed_out.output, timed_out.exit_code) == (
        "command timed out after 5s",
        124,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("prior", [None, "alive", "expired"])
@pytest.mark.parametrize("selected", [False, True])
async def test_docker_same_context_for_create_reuse_execute_and_destroy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    prior: str | None,
    selected: bool,
) -> None:
    context = {"HOME": "/fixture/home", "DOCKER_CONTEXT": "other-daemon"}
    calls: list[tuple[str, dict[str, str] | None]] = []

    def run(
        *args: str, timeout: int = 30, env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        calls.append((args[0], dict(env) if env is not None else None))
        if args[0] == "inspect":
            return subprocess.CompletedProcess(
                args, 0, "true" if prior == "alive" else "false", ""
            )
        if args[0] == "run":
            return subprocess.CompletedProcess(args, 0, "new-container", "")
        return subprocess.CompletedProcess(args, 0, "ok", "")

    monkeypatch.setattr("kokoro_agent.sandbox.docker_backend._docker", run)
    backend = connect_docker_sandbox(
        DockerSettings(image="fixture", ttl=30),
        root=tmp_path,
        container_id=prior,
        run_id="run",
        exec_timeout=5,
        max_output_bytes=100,
        environment_provider=Environment(("API_KEY", "private-value"))
        if selected
        else None,
        docker_cli_environment=context,
    )
    assert (await backend.aexecute("true")).output == "ok"
    assert calls[0][1] == context
    destroy_docker_sandbox(backend.container_id, docker_cli_environment=context)
    expected = (
        ([] if prior is None else ["inspect"])
        + ([] if prior == "alive" else ["run"])
        + ["exec", "rm"]
    )
    assert [kind for kind, _ in calls] == expected
    for kind, observed in calls:
        assert observed == (
            {**context, "API_KEY": "private-value"}
            if selected and kind == "exec"
            else context
        )


@pytest.mark.parametrize("operation", ["create", "reuse", "destroy"])
def test_invalid_docker_context_rejected_before_any_lifecycle_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    def run(*args: str, **kwargs: object):
        pytest.fail("invalid context reached Docker")

    monkeypatch.setattr("kokoro_agent.sandbox.docker_backend._docker", run)
    context = {"WORKER_SECRET": "private-value"}
    with pytest.raises(ValueError, match="^invalid_docker_cli_environment$"):
        if operation == "destroy":
            destroy_docker_sandbox("sandbox", docker_cli_environment=context)
        else:
            connect_docker_sandbox(
                DockerSettings(image="fixture", ttl=30),
                root=tmp_path,
                container_id="prior" if operation == "reuse" else None,
                run_id="run",
                exec_timeout=5,
                max_output_bytes=100,
                docker_cli_environment=context,
            )


@pytest.mark.parametrize("remaining", ["container-id", None])
def test_docker_destroy_failure_retains_cleanup_retry(
    monkeypatch: pytest.MonkeyPatch, remaining: str | None
) -> None:
    def run(*args: str, **kwargs: object) -> subprocess.CompletedProcess[str]:
        if args[0] == "rm":
            return subprocess.CompletedProcess(args, 1, "", "private daemon detail")
        return subprocess.CompletedProcess(
            args, 1 if remaining is None else 0, remaining or "", ""
        )

    monkeypatch.setattr("kokoro_agent.sandbox.docker_backend._docker", run)
    with pytest.raises(RuntimeError, match="^docker_sandbox_cleanup_failed$"):
        destroy_docker_sandbox("container-id", docker_cli_environment={})


def test_docker_destroy_confirmed_absent_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run(*args: str, **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 1 if args[0] == "rm" else 0, "", "")

    monkeypatch.setattr("kokoro_agent.sandbox.docker_backend._docker", run)
    destroy_docker_sandbox("container-id", docker_cli_environment={})
    assert calls[-1] == (
        "ps",
        "--all",
        "--quiet",
        "--no-trunc",
        "--filter",
        "id=container-id",
    )
