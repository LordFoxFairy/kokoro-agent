"""e2b 云沙箱 backend（ADR-009 Phase 1b）：BaseSandbox 4 抽象映射 + run 级生命周期编排。

SDK 调用面按 e2b 2.30 真实签名；受保护文件沿公开 download_url 与 HTTPX async
绝对传输期限。生命周期语义（resume 重连而非新建）由注入 fake 验证；真实云箱仍待 key。
"""

from __future__ import annotations

import asyncio
import base64
from hashlib import sha256
import secrets
import re
import logging
import math
import threading
import time
from collections.abc import AsyncGenerator, Callable, Generator
from contextlib import aclosing, contextmanager
from contextvars import ContextVar
from typing import Annotated, Literal, Protocol, runtime_checkable

from deepagents.backends.protocol import (
    EditResult,
    ExecuteResponse,
    FileInfo,
    FileDownloadResponse,
    FileUploadResponse,
    ReadResult,
    GrepResult,
    LsResult,
    GlobResult,
    WriteResult,
)
from deepagents.backends.sandbox import BaseSandbox
from e2b import CommandExitException, Sandbox, SandboxException
from e2b.api.client_async import AsyncEnvdTransportWithLogger
from e2b.connection_config import (
    KEEPALIVE_PING_HEADER,
    KEEPALIVE_PING_INTERVAL_SEC,
    default_username,
)
from e2b.envd.process import process_connect, process_pb2
from pydantic import BaseModel, ConfigDict, Field, SecretStr
import httpx
from kokoro_agent.domain.run.workspace_output import (
    OUTPUT_ROOT,
    NativeWorkspaceWrite,
    output_path_digest,
)
from kokoro_agent.sandbox.execution_environment import (
    ExecutionEnvironmentProvider,
    ExecutionEnvironmentRunner,
    ExecutionBoundaryError,
)
from kokoro_agent.sandbox.file_egress import (
    FileEgress,
    FILE_UNAVAILABLE,
    MAX_FILE_BYTES,
    SnapshotFileTools,
    relative_path,
)
from kokoro_agent.sandbox.e2b_metadata import E2BMetadata, FilesystemSandbox
from kokoro_agent.sandbox.output_edit import E2B_OUTPUT_MUTATION, current_output_edit

LOGGER = logging.getLogger("kokoro_agent.sandbox.e2b")

# sandbox 即隔离边界（一 run 一箱）：箱内工作目录固定，无需再按 session 分桶。
_WORKDIR = "/home/user"


_WORKSPACE_WRITE_TIMEOUT = 30
_INPUT_CHUNK_BYTES = 64 * 1024


class _WriteProcessClient(Protocol):
    def astart(
        self,
        request: process_pb2.StartRequest,
        *,
        timeout: float,
        request_timeout: float,
        headers: dict[str, str],
    ) -> AsyncGenerator[process_pb2.StartResponse]: ...

    async def asend_input(
        self,
        request: process_pb2.SendInputRequest,
        *,
        request_timeout: float,
    ) -> process_pb2.SendInputResponse: ...

    async def asend_signal(
        self,
        request: process_pb2.SendSignalRequest,
        *,
        request_timeout: float,
    ) -> process_pb2.SendSignalResponse: ...


async def _mutate_workspace_file(
    sandbox: FilesystemSandbox,
    path: str,
    raw: bytes,
    stdout_limit: int,
    edit_args: tuple[str, ...] = (),
) -> bytes | None:
    origin = httpx.URL(sandbox.envd_api_url)
    if (
        origin.scheme not in {"http", "https"}
        or not origin.host
        or origin.userinfo
        or origin.query
        or origin.fragment
        or origin.path not in {"", "/"}
    ):
        return None
    user = base64.b64encode((default_username + ":").encode()).decode("ascii")
    digest = sha256(raw).hexdigest()
    pb = process_pb2
    tag = "kokoro-workspace-write-" + secrets.token_hex(16)
    request = pb.StartRequest(
        process=pb.ProcessConfig(
            cmd="/usr/bin/python3",
            args=[
                "-I",
                "-S",
                "-c",
                E2B_OUTPUT_MUTATION,
                path,
                str(len(raw)),
                digest,
                *edit_args,
            ],
            cwd=_WORKDIR,
        ),
        tag=tag,
        stdin=True,
    )
    with _file_io_logs():
        async with AsyncEnvdTransportWithLogger(
            trust_env=False,
            retries=0,
            limits=httpx.Limits(max_connections=2, max_keepalive_connections=0),
        ) as transport:
            # Generated Connect methods expose untyped **opts; declare only
            # the exact pinned SDK calls this one mutation uses.
            constructor: Callable[..., _WriteProcessClient] = getattr(
                process_connect, "ProcessClient"
            )
            client = constructor(
                str(origin).rstrip("/"),
                async_pool=transport.pool,
                json=True,
                headers=sandbox.connection_config.sandbox_headers,
            )
            async with aclosing(
                client.astart(
                    request,
                    timeout=_WORKSPACE_WRITE_TIMEOUT,
                    request_timeout=5,
                    headers={
                        "Authorization": "Basic " + user,
                        KEEPALIVE_PING_HEADER: str(KEEPALIVE_PING_INTERVAL_SEC),
                    },
                )
            ) as events:
                process = pb.ProcessSelector(tag=tag)
                ended = False
                try:
                    async with asyncio.timeout(_WORKSPACE_WRITE_TIMEOUT):
                        start = (await anext(events)).event
                        if not start.HasField("start") or start.start.pid <= 0:
                            return None
                        for offset in range(0, len(raw), _INPUT_CHUNK_BYTES):
                            await client.asend_input(
                                pb.SendInputRequest(
                                    process=process,
                                    input=pb.ProcessInput(
                                        stdin=raw[offset : offset + _INPUT_CHUNK_BYTES]
                                    ),
                                ),
                                request_timeout=5,
                            )
                        proof = bytearray()
                        async for response in events:
                            event = response.event
                            if ended:
                                return None
                            if event.HasField("keepalive"):
                                continue
                            if event.HasField("end"):
                                ended = True
                                if (
                                    not event.end.exited
                                    or event.end.exit_code != 0
                                    or event.end.error
                                ):
                                    return None
                                continue
                            if not event.HasField("data") or not event.data.HasField(
                                "stdout"
                            ):
                                return None
                            if len(proof) + len(event.data.stdout) > stdout_limit:
                                return None
                            proof.extend(event.data.stdout)
                        return bytes(proof) if ended else None
                finally:
                    if not ended:
                        try:
                            async with asyncio.timeout(2):
                                await client.asend_signal(
                                    pb.SendSignalRequest(
                                        process=process, signal=pb.SIGNAL_SIGKILL
                                    ),
                                    request_timeout=2,
                                )
                        except Exception:
                            # The fixed program has its own lifetime bound; no retry.
                            pass


# e2b SDK 的最小依赖面（依赖倒置）：backend 只认这些子代理，测试注入 fake 无需真箱。
class CommandOutcome(Protocol):
    @property
    def stdout(self) -> str: ...

    @property
    def stderr(self) -> str: ...

    @property
    def exit_code(self) -> int: ...


class SandboxCommands(Protocol):
    def run(
        self,
        cmd: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        envs: dict[str, str] | None = None,
    ) -> CommandOutcome: ...


class SandboxFiles(Protocol):
    def write(self, path: str, data: str | bytes) -> object: ...

    def read(self, path: str, format: Literal["bytes"]) -> object: ...


@runtime_checkable
class DownloadSandbox(Protocol):
    def download_url(
        self,
        path: str,
        user: str | None = None,
        use_signature_expiration: int | None = None,
    ) -> str: ...


def _download_sandbox(value: object) -> DownloadSandbox:
    if not isinstance(value, DownloadSandbox):
        raise ExecutionBoundaryError(FILE_UNAVAILABLE)
    return value


_FILE_IO: ContextVar[bool] = ContextVar("e2b_file_io", default=False)
_LOG_LOCK = threading.RLock()
_LOG_LEASES: dict[logging.Handler, int] = {}


class _SafeFileRecords(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if _FILE_IO.get() and (
            record.name == "httpx" or record.name.startswith("httpcore.")
        ):
            record.msg = "E2B_FILE transport"
            record.args = ()
            record.exc_info = None
            record.exc_text = None
            record.stack_info = None
            record.__dict__.pop("message", None)
        return True


_FILE_FILTER = _SafeFileRecords()


@contextmanager
def _file_io_logs() -> Generator[None, None, None]:
    # Sanitize producer handlers before enqueue; only this call's context changes.
    handlers = set(logging.getLogger().handlers)
    for item in list(logging.Logger.manager.loggerDict.values()):
        if isinstance(item, logging.Logger):
            handlers.update(item.handlers)
    if logging.lastResort is not None:
        handlers.add(logging.lastResort)
    with _LOG_LOCK:
        for handler in handlers:
            count = _LOG_LEASES.get(handler, 0)
            if count == 0:
                handler.addFilter(_FILE_FILTER)
            _LOG_LEASES[handler] = count + 1
    token = _FILE_IO.set(True)
    try:
        yield
    finally:
        _FILE_IO.reset(token)
        with _LOG_LOCK:
            for handler in handlers:
                count = _LOG_LEASES[handler] - 1
                if count:
                    _LOG_LEASES[handler] = count
                else:
                    del _LOG_LEASES[handler]
                    handler.removeFilter(_FILE_FILTER)


class SandboxClient(Protocol):
    @property
    def sandbox_id(self) -> str: ...

    @property
    def commands(self) -> SandboxCommands: ...

    @property
    def files(self) -> SandboxFiles: ...


class E2BSettings(BaseModel):
    """进程级 e2b 参数；api_key 缺失时选择 e2b backend 即 fail-loud（ADR-006 强制规则）。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    api_key: SecretStr | None
    template: str | None
    # sandbox 空闲存活期（秒）：HITL 等人窗口内保活；超期由 TTL 回收，文件靠归档兜底。
    timeout: Annotated[int, Field(gt=0)]


class E2BSandboxBackend(BaseSandbox):
    """execute 经 commands.run；普通文件走 SDK，受保护读取沿公开限时 URL。"""

    def __init__(
        self,
        sandbox: SandboxClient,
        *,
        environment_provider: ExecutionEnvironmentProvider | None = None,
        environment_loop: asyncio.AbstractEventLoop | None = None,
        max_output_bytes: int = 100_000,
    ) -> None:
        self._sandbox = sandbox
        self._environment = (
            ExecutionEnvironmentRunner(
                environment_provider,
                max_output_bytes=max_output_bytes,
                owner_loop=environment_loop,
            )
            if environment_provider is not None
            else None
        )
        self._file_egress = (
            FileEgress(self._environment, self._read_snapshot, 120)
            if self._environment is not None
            else None
        )
        self._file_tools = (
            SnapshotFileTools(self._file_egress, self._metadata, max_output_bytes)
            if self._file_egress is not None
            else None
        )

    def _metadata(
        self,
        path: str,
        recursive: bool,
        inspect: Callable[[bytes], None],
        deadline: float,
        cancelled: threading.Event,
    ) -> list[FileInfo]:
        with _file_io_logs():
            return E2BMetadata(self._sandbox)(
                path, recursive, inspect, deadline, cancelled
            )

    def _read_snapshot(self, path: str, maximum: int, deadline: float) -> bytes:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        # SDK 2.30 performs only local signing here, no connect/get_info/HTTP.
        # Keep the URL local, and join asyncio.run's executor on this owned thread.
        with _file_io_logs():
            url = _download_sandbox(self._sandbox).download_url(
                f"{_WORKDIR}/{relative_path(path)}",
                use_signature_expiration=max(1, math.ceil(remaining)),
            )
            return asyncio.run(self._download_snapshot(url, maximum, deadline))

    async def _download_snapshot(
        self, url: str, maximum: int, deadline: float
    ) -> bytes:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        request = httpx.Request(
            "GET",
            url,
            headers={"accept-encoding": "identity"},
            extensions={
                "timeout": {
                    "connect": remaining,
                    "read": remaining,
                    "write": remaining,
                    "pool": remaining,
                }
            },
        )
        if (
            request.url.scheme not in {"http", "https"}
            or not request.url.host
            or request.url.userinfo
            or request.url.fragment
        ):
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        async with asyncio.timeout(remaining):
            async with httpx.AsyncHTTPTransport(
                trust_env=False,
                retries=0,
                limits=httpx.Limits(max_connections=1, max_keepalive_connections=0),
            ) as transport:
                response = await transport.handle_async_request(request)
                try:
                    if (
                        response.status_code != 200
                        or response.headers.get("content-encoding", "identity")
                        != "identity"
                    ):
                        raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                    size = response.headers.get("content-length")
                    if size is not None and (
                        not size.isascii()
                        or not size.isdecimal()
                        or len(size) > 20
                        or int(size) > maximum
                    ):
                        raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                    content = bytearray()
                    async for chunk in response.aiter_raw():
                        if len(content) + len(chunk) > maximum:
                            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                        content.extend(chunk)
                    if size is not None and len(content) != int(size):
                        raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                    return bytes(content)
                finally:
                    await response.aclose()

    async def guard_workspace_output(self, path: str, content: bytes) -> bool:
        if self._file_egress is None:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        return await self._file_egress.aguard_snapshot(path, content)

    async def _write_product_output(self, file_path: str, content: str) -> WriteResult:
        try:
            if (
                not isinstance(self._sandbox, FilesystemSandbox)
                or output_path_digest(file_path) is None
                or len(content) > MAX_FILE_BYTES
            ):
                return WriteResult(error=FILE_UNAVAILABLE)
            path = relative_path(file_path)
            raw = content.encode("utf-8")
            if len(raw) <= MAX_FILE_BYTES:
                proof = f"{len(raw)}:{sha256(raw).hexdigest()}\n".encode("ascii")
                if (
                    await _mutate_workspace_file(self._sandbox, path, raw, len(proof))
                    == proof
                ):
                    return WriteResult(path=file_path)
        except Exception:
            # The provider exception or stderr can contain private material.
            pass
        return WriteResult(error=FILE_UNAVAILABLE)

    def write(self, file_path: str, content: str) -> WriteResult:
        if self._file_egress is not None and file_path.startswith(OUTPUT_ROOT):
            return asyncio.run(self._write_product_output(file_path, content))
        return super().write(file_path, content)

    async def awrite(self, file_path: str, content: str) -> WriteResult:
        if self._file_egress is not None and file_path.startswith(OUTPUT_ROOT):
            return await self._write_product_output(file_path, content)
        return await super().awrite(file_path, content)

    async def aedit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
    ) -> EditResult:
        capture = current_output_edit(file_path)
        if capture is None:
            return await super().aedit(file_path, old_string, new_string, replace_all)
        error = EditResult(error=FILE_UNAVAILABLE)
        try:
            if (
                self._file_egress is None
                or capture.postimage is not None
                or not isinstance(self._sandbox, FilesystemSandbox)
                or output_path_digest(file_path) is None
                or len(old_string) + len(new_string) > MAX_FILE_BYTES
            ):
                return error
            old, new = old_string.encode("utf-8"), new_string.encode("utf-8")
            if len(old) + len(new) > MAX_FILE_BYTES:
                return error
            result = await _mutate_workspace_file(
                self._sandbox,
                relative_path(file_path),
                old + new,
                MAX_FILE_BYTES + 96,
                (str(len(old)), str(int(replace_all))),
            )
            if result is None:
                return error
            header, separator, raw = result.partition(b"\n")
            proof = re.fullmatch(
                rb"([1-9][0-9]{0,7}):(0|[1-9][0-9]{0,7}):([0-9a-f]{64})", header
            )
            if (
                not separator
                or proof is None
                or not 1 <= int(proof[1]) <= MAX_FILE_BYTES + 1
                or not len(raw) == int(proof[2]) <= MAX_FILE_BYTES
                or sha256(raw).hexdigest().encode("ascii") != proof[3]
            ):
                return error
            occurrences = int(proof[1])
            capture.postimage = NativeWorkspaceWrite(file_path, raw, occurrences)
            return EditResult(path=file_path, occurrences=occurrences)
        except Exception:
            # Never attach provider diagnostics or the original postimage.
            return error

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> ReadResult:
        if self._file_egress is not None:
            return self._file_egress.read(file_path, offset, limit)
        return super().read(file_path, offset, limit)

    async def aread(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> ReadResult:
        if self._file_egress is not None:
            return await self._file_egress.aread(file_path, offset, limit)
        return await super().aread(file_path, offset, limit)

    def grep(
        self, pattern: str, path: str | None = None, glob: str | None = None
    ) -> GrepResult:
        if self._file_tools is not None:
            return self._file_tools.grep(pattern, path, glob)
        return super().grep(pattern, path, glob)

    def ls(self, path: str) -> LsResult:
        if self._file_tools is not None:
            return self._file_tools.ls(path)
        return super().ls(path)

    def glob(self, pattern: str, path: str = "/") -> GlobResult:
        if self._file_tools is not None:
            return self._file_tools.glob(pattern, path)
        return super().glob(pattern, path)

    async def agrep(
        self, pattern: str, path: str | None = None, glob: str | None = None
    ) -> GrepResult:
        if self._file_tools is not None:
            return await self._file_tools.agrep(pattern, path, glob)
        return await super().agrep(pattern, path, glob)

    async def als(self, path: str) -> LsResult:
        if self._file_tools is not None:
            return await self._file_tools.als(path)
        return await super().als(path)

    async def aglob(self, pattern: str, path: str = "/") -> GlobResult:
        if self._file_tools is not None:
            return await self._file_tools.aglob(pattern, path)
        return await super().aglob(pattern, path)

    async def adownload_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        if self._file_egress is not None:
            return await self._file_egress.adownload(paths)
        return await super().adownload_files(paths)

    @property
    def id(self) -> str:
        return self._sandbox.sandbox_id

    @property
    def sandbox_id(self) -> str:
        # 统一生命周期面（ADR-010）：编排层据此落 run_repository 记录，各档同一属性名。
        return self._sandbox.sandbox_id

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        if self._environment is not None:
            return self._environment.execute(
                timeout if timeout is not None else 120,
                lambda values, remaining: self._execute_environment(
                    command, values, remaining
                ),
            )
        return self._execute_plain(command, timeout)

    async def aexecute(
        self, command: str, *, timeout: int | None = None
    ) -> ExecuteResponse:
        if self._environment is None:
            return await super().aexecute(command, timeout=timeout)
        return await self._environment.aexecute(
            timeout if timeout is not None else 120,
            lambda values, remaining: self._execute_environment(
                command, values, remaining
            ),
        )

    def _execute_environment(
        self, command: str, values: dict[str, str], timeout: int
    ) -> ExecuteResponse:
        try:
            result = self._sandbox.commands.run(
                command, cwd=_WORKDIR, timeout=timeout, envs=values
            )
        except CommandExitException as failed:
            return ExecuteResponse(
                output=failed.stdout + failed.stderr,
                exit_code=failed.exit_code,
                truncated=False,
            )
        return ExecuteResponse(
            output=result.stdout + result.stderr,
            exit_code=result.exit_code,
            truncated=False,
        )

    def _execute_plain(self, command: str, timeout: int | None) -> ExecuteResponse:
        try:
            result = self._sandbox.commands.run(command, cwd=_WORKDIR, timeout=timeout)
        except CommandExitException as failed:
            # SDK 把非零退出码抛成异常；backend 契约里它是正常结果。
            return ExecuteResponse(
                output=f"{failed.stdout}{failed.stderr}",
                exit_code=failed.exit_code,
                truncated=False,
            )
        return ExecuteResponse(
            output=f"{result.stdout}{result.stderr}",
            exit_code=result.exit_code,
            truncated=False,
        )

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        responses: list[FileUploadResponse] = []
        for path, data in files:
            try:
                self._sandbox.files.write(f"{_WORKDIR}/{path.lstrip('/')}", data)
                responses.append(FileUploadResponse(path=path, error=None))
            except SandboxException:
                responses.append(FileUploadResponse(path=path, error="invalid_path"))
        return responses

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        if self._file_egress is not None:
            return self._file_egress.download(paths)
        responses: list[FileDownloadResponse] = []
        for path in paths:
            try:
                raw = self._sandbox.files.read(
                    f"{_WORKDIR}/{path.lstrip('/')}", format="bytes"
                )
                content = (
                    bytes(raw)
                    if isinstance(raw, (bytes, bytearray))
                    else str(raw).encode()
                )
                responses.append(
                    FileDownloadResponse(path=path, content=content, error=None)
                )
            except SandboxException:
                responses.append(
                    FileDownloadResponse(
                        path=path, content=None, error="file_not_found"
                    )
                )
        return responses


def kill_e2b_sandbox(settings: E2BSettings, sandbox_id: str) -> None:
    """终态主动回收：官方类形态 Sandbox.kill(sandbox_id)；无 key 直接返回（无箱可回收）。"""
    if settings.api_key is None:
        return
    Sandbox.kill(sandbox_id, api_key=settings.api_key.get_secret_value())


def connect_e2b_sandbox(
    settings: E2BSettings,
    *,
    sandbox_id: str | None,
    environment_provider: ExecutionEnvironmentProvider | None = None,
    environment_loop: asyncio.AbstractEventLoop | None = None,
    max_output_bytes: int = 100_000,
) -> E2BSandboxBackend:
    """run 级生命周期：有既往 sandbox_id 先重连（HITL 暂停期文件在箱内）；
    箱已被 TTL 回收则新建并告警（文件面由归档兜底）。SDK 网络调用是同步阻塞——
    调用方（async 装配路径）须经 to_thread。
    """
    if settings.api_key is None:
        raise ValueError("backend e2b requires KOKORO_E2B_API_KEY")
    api_key = settings.api_key.get_secret_value()
    if sandbox_id is not None:
        try:
            # 类形态 connect：按 id 重连（paused 箱自动 resume）——HITL 暂停期文件都在。
            sandbox = Sandbox.connect(
                sandbox_id, timeout=settings.timeout, api_key=api_key
            )
            return E2BSandboxBackend(
                sandbox,
                environment_provider=environment_provider,
                environment_loop=environment_loop,
                max_output_bytes=max_output_bytes,
            )
        except SandboxException:
            LOGGER.warning(
                "e2b sandbox %s gone (TTL?), creating a fresh one", sandbox_id
            )
    created = Sandbox.create(
        template=settings.template, timeout=settings.timeout, api_key=api_key
    )
    return E2BSandboxBackend(
        created,
        environment_provider=environment_provider,
        environment_loop=environment_loop,
        max_output_bytes=max_output_bytes,
    )
