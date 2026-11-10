"""e2b backend 规格（ADR-009 1b）：生命周期编排（resume 重连/新建落账/keep-first）与
execute 映射。SDK 边界以 fake 注入验证我们的编排语义；SDK 真实行为待 key 真栈复核。
"""

from __future__ import annotations

from typing import Self
from collections.abc import AsyncIterator
from pathlib import Path
import subprocess
import sys
import asyncio
import time
import logging

import pytest
from e2b import CommandExitException, SandboxException
from e2b.connection_config import ConnectionConfig
from e2b.envd.process import process_connect, process_pb2
from pydantic import SecretStr

from support.fakes import FakeRunRepository, request
from support.file_egress import Environment
from kokoro_agent.sandbox.backend import SandboxSettings, make_backend_for_run
from kokoro_agent.sandbox.e2b_backend import (
    E2BSandboxBackend,
    E2BSettings,
    connect_e2b_sandbox,
)
from kokoro_agent.domain.run.repository import RunRepository
from kokoro_agent.sandbox.output_edit import capture_output_edit


def _e2b_settings(api_key: str | None = "e2b-key") -> E2BSettings:
    return E2BSettings(
        api_key=None if api_key is None else SecretStr(api_key),
        template=None,
        timeout=1800,
    )


def _dispatch_settings() -> SandboxSettings:
    return SandboxSettings.model_validate(
        {
            "local_shell_root": None,
            "local_shell_inherit_env": False,
            "local_shell_timeout": 30,
            "local_shell_max_output_bytes": 100000,
            "workspace": None,
            "workspace_s3_access_key": None,
            "workspace_s3_secret_key": None,
            "e2b": _e2b_settings(),
            "docker": {"image": None, "ttl": 1800},
            "custom": {"factory_ref": None, "config_path": None},
        }
    )


class FakeCommandResult:
    def __init__(self, stdout: str, stderr: str, exit_code: int) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.exit_code = exit_code


class FakeCommands:
    def __init__(self, owner: FakeSandbox) -> None:
        self._owner = owner

    def run(
        self,
        cmd: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        envs: dict[str, str] | None = None,
    ) -> FakeCommandResult:
        self._owner.commands_run.append((cmd, cwd))
        if self._owner.fail_next_command is not None:
            failure = self._owner.fail_next_command
            self._owner.fail_next_command = None
            raise failure
        return FakeCommandResult("ok\n", "", 0)


class FakeFiles:
    def write(self, path: str, data: str | bytes) -> object:
        return None

    def read(self, path: str, format: str = "text") -> object:
        raise SandboxException("not found")


class FakeSandbox:
    """镜像 e2b 2.30 调用面：create / 构造+connect / commands.run / sandbox_id。"""

    created: list[FakeSandbox] = []
    killed: list[str] = []
    connect_should_fail = False
    connect_failures_remaining = 0

    def __init__(
        self, sandbox_id: str = "sbx_new", api_key: str | None = None, **_: object
    ) -> None:
        self.sandbox_id = sandbox_id
        self.api_key = api_key
        self.commands_run: list[tuple[str, str | None]] = []
        self.fail_next_command: Exception | None = None
        self.commands = FakeCommands(self)
        self.files = FakeFiles()

    @classmethod
    def create(
        cls, template: str | None = None, timeout: int | None = None, **opts: object
    ) -> Self:
        instance = cls(
            sandbox_id=f"sbx_created_{len(cls.created)}",
            api_key=str(opts.get("api_key")),
        )
        cls.created.append(instance)
        return instance

    @classmethod
    def connect(
        cls, sandbox_id: str, timeout: int | None = None, **opts: object
    ) -> Self:
        # 镜像 e2b 类形态 connect：按 id 重连，paused 自动 resume。
        if cls.connect_failures_remaining > 0:
            cls.connect_failures_remaining -= 1
            raise SandboxException("sandbox gone")
        if cls.connect_should_fail:
            raise SandboxException("sandbox gone")
        return cls(sandbox_id=sandbox_id, api_key=str(opts.get("api_key")))

    @classmethod
    def kill(cls, sandbox_id: str, **_: object) -> None:
        cls.killed.append(sandbox_id)


@pytest.fixture(autouse=True)
def fake_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeSandbox.created = []
    FakeSandbox.killed = []
    FakeSandbox.connect_should_fail = False
    FakeSandbox.connect_failures_remaining = 0
    monkeypatch.setattr("kokoro_agent.sandbox.e2b_backend.Sandbox", FakeSandbox)


class TestLifecycle:
    def test_no_api_key_fail_loud(self) -> None:
        with pytest.raises(ValueError, match="KOKORO_E2B_API_KEY"):
            connect_e2b_sandbox(_e2b_settings(api_key=None), sandbox_id=None)

    def test_fresh_run_creates_sandbox_with_key(self) -> None:
        backend = connect_e2b_sandbox(_e2b_settings(), sandbox_id=None)
        assert backend.id == "sbx_created_0"
        assert FakeSandbox.created[0].api_key == "e2b-key"

    def test_resume_reconnects_existing_sandbox_not_create(self) -> None:
        backend = connect_e2b_sandbox(_e2b_settings(), sandbox_id="sbx_prior")
        assert backend.id == "sbx_prior"
        assert (
            FakeSandbox.created == []
        )  # 关键语义：resume 绝不新建（暂停期文件在箱内）

    def test_resume_falls_back_to_create_when_sandbox_gone(self) -> None:
        FakeSandbox.connect_should_fail = True
        backend = connect_e2b_sandbox(_e2b_settings(), sandbox_id="sbx_expired")
        assert backend.id == "sbx_created_0"

    @pytest.mark.asyncio
    async def test_run_scoped_binding_new_sandbox_and_reuse(
        self, run_repository: RunRepository
    ) -> None:
        # 生产路径：run 先被认领（建 run 文档），箱绑定才落账。
        lease = await run_repository.try_claim(request("run_1"), "owner")
        assert lease is not None
        first = await make_backend_for_run(
            "e2b",
            _dispatch_settings(),
            workspace="ns:s1",
            run_id="run_1",
            lease=lease,
            sandbox_store=run_repository,
        )
        assert isinstance(first, E2BSandboxBackend)
        assert await run_repository.get_sandbox_id("run_1") == first.id
        # HITL resume：重建 backend 走重连，箱不重建、绑定不被覆盖（keep-first）。
        second = await make_backend_for_run(
            "e2b",
            _dispatch_settings(),
            workspace="ns:s1",
            run_id="run_1",
            lease=lease,
            sandbox_store=run_repository,
        )
        assert isinstance(second, E2BSandboxBackend)
        assert second.id == first.id
        assert len(FakeSandbox.created) == 1

    @pytest.mark.asyncio
    async def test_dead_prior_replacement_becomes_authoritative(self) -> None:
        repository = FakeRunRepository()
        lease = await repository.try_claim(request("run_replacement"), "owner")
        assert lease is not None
        first = await make_backend_for_run(
            "e2b",
            _dispatch_settings(),
            workspace="ns:s1",
            run_id="run_replacement",
            lease=lease,
            sandbox_store=repository,
        )
        assert isinstance(first, E2BSandboxBackend)
        assert await repository.pause("run_replacement", lease) is True
        replacement_lease = await repository.adopt("run_replacement", "owner")
        assert replacement_lease is not None
        FakeSandbox.connect_should_fail = True

        replacement = await make_backend_for_run(
            "e2b",
            _dispatch_settings(),
            workspace="ns:s1",
            run_id="run_replacement",
            lease=replacement_lease,
            sandbox_store=repository,
        )

        assert isinstance(replacement, E2BSandboxBackend)
        assert replacement.id != first.id
        assert await repository.get_sandbox_id("run_replacement") == replacement.id

    @pytest.mark.asyncio
    async def test_same_generation_cannot_replace_confirmed_sandbox(self) -> None:
        repository = FakeRunRepository()
        lease = await repository.try_claim(request("run_competing"), "owner")
        assert lease is not None
        first = await make_backend_for_run(
            "e2b",
            _dispatch_settings(),
            workspace="ns:s1",
            run_id="run_competing",
            lease=lease,
            sandbox_store=repository,
        )
        assert isinstance(first, E2BSandboxBackend)
        FakeSandbox.connect_failures_remaining = 1

        competing = await make_backend_for_run(
            "e2b",
            _dispatch_settings(),
            workspace="ns:s1",
            run_id="run_competing",
            lease=lease,
            sandbox_store=repository,
        )

        assert isinstance(competing, E2BSandboxBackend)
        assert competing.id == first.id
        assert await repository.get_sandbox_id("run_competing") == first.id
        assert FakeSandbox.killed == ["sbx_created_1"]


class TestExecuteMapping:
    def test_zero_exit_maps_stdout(self) -> None:
        backend = E2BSandboxBackend(FakeSandbox(sandbox_id="sbx_x"))
        result = backend.execute("echo ok")
        assert (result.output, result.exit_code, result.truncated) == ("ok\n", 0, False)
        assert backend.id == "sbx_x"

    def test_nonzero_exit_is_result_not_exception(self) -> None:
        fake = FakeSandbox(sandbox_id="sbx_x")
        fake.fail_next_command = CommandExitException(
            stdout="partial", stderr="boom", exit_code=2, error=""
        )
        result = E2BSandboxBackend(fake).execute("false")
        assert result.exit_code == 2
        assert "boom" in result.output

    def test_download_missing_file_maps_error(self) -> None:
        response = E2BSandboxBackend(FakeSandbox()).download_files(["/ghost.md"])[0]
        assert (response.path, response.content, response.error) == (
            "/ghost.md",
            None,
            "file_not_found",
        )


class MetadataHTTPServer:
    """Real loopback HTTP only; the official cloud service is not contacted."""

    def __init__(self, phase: str) -> None:
        self.phase = phase
        self.server: asyncio.Server | None = None
        self.tasks: set[asyncio.Task[None]] = set()
        self.entered = asyncio.Event()
        self.closed = asyncio.Event()
        self.requests: list[bytes] = []

    async def start(self) -> E2BSandboxBackend:
        from e2b.connection_config import ConnectionConfig
        from support.file_egress import Environment

        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]

        class LocalSandbox(FakeSandbox):
            envd_api_url = f"http://127.0.0.1:{port}"
            connection_config = ConnectionConfig(
                api_key="e2b_" + "0" * 40,
                extra_sandbox_headers={"X-Access-Token": "private-http-token"},
            )

        class DeadlineMetadataBackend(E2BSandboxBackend):
            def __init__(self) -> None:
                super().__init__(LocalSandbox(), environment_provider=Environment())
                # Shorten only this test fixture's inherited file budget.
                assert self._file_egress is not None
                self._file_egress.timeout = 1

        return DeadlineMetadataBackend()

    async def handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.tasks.add(task)
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            size = next(
                int(row.split(b":", 1)[1])
                for row in headers.split(b"\r\n")
                if row.lower().startswith(b"content-length:")
            )
            request = await reader.readexactly(size)
            self.requests.append(request)
            self.entered.set()
            if self.phase == "headers":
                assert await reader.read(1) == b""
                return
            if self.phase == "redirect":
                writer.write(
                    b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: /never\r\n\r\n"
                )
            elif self.phase in {"body", "drip"}:
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/proto\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n"
                )
                await writer.drain()
                if self.phase == "drip":
                    for _ in range(20):
                        await asyncio.sleep(0.1)
                        writer.write(b"1\r\nx\r\n")
                        await writer.drain()
            elif self.phase == "chunked-oversize":
                from kokoro_agent.sandbox.e2b_metadata import MAX_METADATA_BODY_BYTES

                payload = b"x" * (MAX_METADATA_BODY_BYTES + 1)
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/proto\r\nTransfer-Encoding: chunked\r\n\r\n"
                    + format(len(payload), "x").encode()
                    + b"\r\n"
                    + payload
                    + b"\r\n0\r\n\r\n"
                )
            else:
                from kokoro_agent.sandbox.e2b_metadata import MAX_METADATA_BODY_BYTES

                payload = (
                    b"x" * (MAX_METADATA_BODY_BYTES + 1)
                    if self.phase == "oversize"
                    else b"private-rpc-error"
                )
                status = (
                    b"500 Internal Server Error" if self.phase == "error" else b"200 OK"
                )
                declared = len(payload) + (1 if self.phase == "incomplete" else 0)
                writer.write(
                    b"HTTP/1.1 "
                    + status
                    + b"\r\nContent-Type: application/proto\r\nContent-Length: "
                    + str(declared).encode()
                    + b"\r\n\r\n"
                    + payload
                )
                if self.phase == "incomplete":
                    await writer.drain()
                    return
            await writer.drain()
            assert await reader.read(1) == b""
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            self.tasks.discard(task)
            self.closed.set()

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase",
    [
        "headers",
        "body",
        "drip",
        "oversize",
        "chunked-oversize",
        "error",
        "redirect",
        "incomplete",
        "malformed",
    ],
)
async def test_e2b_metadata_real_http_is_bounded_and_closes(
    phase: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = MetadataHTTPServer(phase)
    instance = await server.start()
    caplog.set_level(logging.DEBUG)
    started = time.monotonic()
    try:
        denied = await instance.als("/")
        assert denied.error == "sandbox_file_unavailable"
        assert denied.entries is None
        assert time.monotonic() - started < 1.5
        if phase != "drip":
            await asyncio.wait_for(server.closed.wait(), 0.4)
        assert len(server.requests) == 1
        assert "private-rpc-error" not in caplog.text
        assert "private-http-token" not in caplog.text
        assert "filesystem.Filesystem" not in caplog.text
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_e2b_metadata_cancel_closes_real_http_without_late_result() -> None:
    server = MetadataHTTPServer("body")
    instance = await server.start()
    task = asyncio.create_task(instance.als("/"))
    try:
        await asyncio.wait_for(server.entered.wait(), 1)
        task.cancel()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 0.7)
        await asyncio.wait_for(server.closed.wait(), 0.4)
        assert len(server.requests) == 1
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_e2b_without_api_keeps_original_native_file_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deepagents.backends.protocol import GlobResult, GrepResult, LsResult
    from deepagents.backends.sandbox import BaseSandbox

    def listed(self: BaseSandbox, path: str) -> LsResult:
        assert path == "/native"
        return LsResult(entries=[{"path": "/native/file"}])

    def globbed(self: BaseSandbox, pattern: str, path: str = "/") -> GlobResult:
        assert (pattern, path) == ("*.txt", "/native")
        return GlobResult(matches=[{"path": "/native/file.txt"}])

    def searched(
        self: BaseSandbox,
        pattern: str,
        path: str | None = None,
        glob: str | None = None,
    ) -> GrepResult:
        assert (pattern, path, glob) == ("needle", "/native", None)
        return GrepResult(
            matches=[{"path": "/native/file", "line": 1, "text": "needle"}]
        )

    monkeypatch.setattr(BaseSandbox, "ls", listed)
    monkeypatch.setattr(BaseSandbox, "glob", globbed)
    monkeypatch.setattr(BaseSandbox, "grep", searched)
    instance = E2BSandboxBackend(FakeSandbox())
    assert instance.ls("/native").entries == [{"path": "/native/file"}]
    assert (await instance.als("/native")).entries == [{"path": "/native/file"}]
    assert instance.glob("*.txt", "/native").matches == [{"path": "/native/file.txt"}]
    assert (await instance.aglob("*.txt", "/native")).matches == [
        {"path": "/native/file.txt"}
    ]
    assert instance.grep("needle", "/native").matches == [
        {"path": "/native/file", "line": 1, "text": "needle"}
    ]
    assert (await instance.agrep("needle", "/native")).matches == [
        {"path": "/native/file", "line": 1, "text": "needle"}
    ]


async def test_protected_e2b_write_and_edit_own_exact_bytes_without_path_readback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class LocalSandbox(FakeSandbox):
        envd_api_url = "https://envd.fixture.invalid"
        connection_config = ConnectionConfig(api_key="fixture")

    tamper = False

    class Process:
        def __init__(self, origin: str, **_: object) -> None:
            assert origin == LocalSandbox.envd_api_url
            self.data = bytearray()

        async def astart(
            self, request: process_pb2.StartRequest, **_: object
        ) -> AsyncIterator[process_pb2.StartResponse]:
            config = request.process
            assert config.cwd == "/home/user" and request.stdin
            assert request.tag.startswith("kokoro-workspace-write-")
            self.tag = request.tag
            assert config.cmd == "/usr/bin/python3" and not config.envs
            assert list(config.args)[:3] == ["-I", "-S", "-c"]
            assert "原文" not in "".join(config.args)
            yield process_pb2.StartResponse(
                event=process_pb2.ProcessEvent(
                    start=process_pb2.ProcessEvent.StartEvent(pid=73)
                )
            )
            args = list(config.args)
            if tamper:
                args[3] = """
import os
def tamper_install(real):
    def install(src, dst, *, src_dir_fd, **options):
        fd = os.open(src, os.O_WRONLY | os.O_NOFOLLOW, dir_fd=src_dir_fd)
        try:
            info = os.fstat(fd)
            os.write(fd, b"X" * info.st_size)
            os.utime(fd, ns=(info.st_atime_ns, info.st_mtime_ns))
        finally:
            os.close(fd)
        real(src, dst, src_dir_fd=src_dir_fd, **options)
    return install
os.link = tamper_install(os.link)
os.replace = tamper_install(os.replace)
""" + args[3]
            result = subprocess.run(
                [sys.executable, *args],
                cwd=tmp_path,
                input=bytes(self.data),
                capture_output=True,
                timeout=5,
                check=False,
            )
            assert not result.stderr
            if result.stdout:
                yield process_pb2.StartResponse(
                    event=process_pb2.ProcessEvent(
                        data=process_pb2.ProcessEvent.DataEvent(stdout=result.stdout)
                    )
                )
            yield process_pb2.StartResponse(
                event=process_pb2.ProcessEvent(
                    end=process_pb2.ProcessEvent.EndEvent(
                        exited=True, exit_code=result.returncode
                    )
                )
            )

        async def asend_input(
            self, request: process_pb2.SendInputRequest, **_: object
        ) -> process_pb2.SendInputResponse:
            assert request.process.tag == self.tag
            self.data.extend(request.input.stdin)
            return process_pb2.SendInputResponse()

    monkeypatch.setattr(process_connect, "ProcessClient", Process)
    sandbox = LocalSandbox()
    environment = Environment()
    backend = E2BSandboxBackend(
        sandbox,
        environment_provider=environment,
        environment_loop=asyncio.get_running_loop(),
    )
    text = "原文🦉\r\n"
    path = "/outputs/proof.md"
    file = tmp_path / "outputs" / "proof.md"
    result = await backend.awrite(path, text)
    assert result.error is None and result.path == path
    assert file.read_bytes() == text.encode()
    assert (await backend.awrite(path, "replace")).error
    assert file.read_bytes() == text.encode()

    outside = tmp_path / "outside"
    outside.mkdir()
    (file.parent / "alias").symlink_to(outside, target_is_directory=True)
    assert (await backend.awrite("/outputs/alias/escape.md", text)).error
    assert not (outside / "escape.md").exists()

    with capture_output_edit(path) as capture:
        assert capture is not None
        edited = await backend.aedit(path, "原文🦉\n", "编辑🪁\n")
    postimage = "编辑🪁\r\n".encode()
    assert (edited.error, edited.path, edited.occurrences) == (None, path, 1)
    assert capture.postimage is not None and capture.postimage.content == postimage
    assert file.read_bytes() == postimage
    file.write_bytes(b"later mutable bytes")
    assert capture.postimage.content == postimage

    tamper = True
    assert (await backend.awrite("/outputs/tampered.md", text)).error
    with capture_output_edit(path) as rejected:
        assert rejected is not None
        assert (await backend.aedit(path, "later mutable bytes", "second edit")).error
    assert rejected.postimage is None
    assert not sandbox.commands_run

    assert await backend.guard_workspace_output(path, text.encode())
    assert not await backend.guard_workspace_output(
        path, "private-原值-marker".encode()
    )
    assert environment.calls == 2
