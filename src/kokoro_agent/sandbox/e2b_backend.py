"""e2b 云沙箱 backend（ADR-009 Phase 1b）：BaseSandbox 4 抽象映射 + run 级生命周期编排。

SDK 调用面按 e2b 2.30 真实签名；受保护文件沿公开 download_url 与 HTTPX async
绝对传输期限。生命周期语义（resume 重连而非新建）由注入 fake 验证；真实云箱仍待 key。
"""

from __future__ import annotations

import asyncio
import logging
import math
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Annotated, Literal, Protocol, runtime_checkable

from deepagents.backends.protocol import (
    ExecuteResponse,
    FileDownloadResponse,
    FileUploadResponse,
    ReadResult,
    GrepResult,
    LsResult,
    GlobResult,
)
from deepagents.backends.sandbox import BaseSandbox
from e2b import CommandExitException, Sandbox, SandboxException
from pydantic import BaseModel, ConfigDict, Field, SecretStr
import httpx
from kokoro_agent.sandbox.execution_environment import (
    ExecutionEnvironmentProvider,
    ExecutionEnvironmentRunner,
    ExecutionBoundaryError,
)
from kokoro_agent.sandbox.file_egress import FileEgress, FILE_UNAVAILABLE, relative_path

LOGGER = logging.getLogger("kokoro_agent.sandbox.e2b")

# sandbox 即隔离边界（一 run 一箱）：箱内工作目录固定，无需再按 session 分桶。
_WORKDIR = "/home/user"


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
        if self._file_egress is not None:
            return GrepResult(error=FILE_UNAVAILABLE)
        return super().grep(pattern, path, glob)

    def ls(self, path: str) -> LsResult:
        if self._file_egress is not None:
            return LsResult(error=FILE_UNAVAILABLE)
        return super().ls(path)

    def glob(self, pattern: str, path: str = "/") -> GlobResult:
        if self._file_egress is not None:
            return GlobResult(error=FILE_UNAVAILABLE)
        return super().glob(pattern, path)

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
