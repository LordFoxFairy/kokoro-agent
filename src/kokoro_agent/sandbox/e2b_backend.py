"""e2b 云沙箱 backend（ADR-009 Phase 1b）：BaseSandbox 4 抽象映射 + run 级生命周期编排。

结构就位、真栈验证待 key：SDK 调用面按 e2b 2.30 真实签名；生命周期语义（resume 重连
而非新建）由注入 fake 验证；任何 SDK 行为偏差以 key 到位后的真栈为准修正。
"""

from __future__ import annotations

import logging
from typing import Annotated, Literal, Protocol

from deepagents.backends.protocol import (
    ExecuteResponse,
    FileDownloadResponse,
    FileUploadResponse,
)
from deepagents.backends.sandbox import BaseSandbox
from e2b import CommandExitException, Sandbox, SandboxException
from pydantic import BaseModel, ConfigDict, Field, SecretStr

LOGGER = logging.getLogger("kokoro_agent.sandbox.e2b")

# sandbox 即隔离边界（一 run 一箱）：箱内工作目录固定，无需再按 session 分桶。
_WORKDIR = "/home/user"


# e2b SDK 的最小依赖面（依赖倒置）：backend 只认这些成员，测试注入 fake 无需真箱。
class CommandOutcome(Protocol):
    @property
    def stdout(self) -> str: ...

    @property
    def stderr(self) -> str: ...

    @property
    def exit_code(self) -> int: ...


class SandboxCommands(Protocol):
    def run(
        self, cmd: str, *, cwd: str | None = None, timeout: int | None = None
    ) -> CommandOutcome: ...


class SandboxFiles(Protocol):
    def write(self, path: str, data: str | bytes) -> object: ...

    def read(self, path: str, format: Literal["bytes"]) -> object: ...


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
    """execute 经 commands.run（非零退出码是结果不是异常）；文件面走 files API。"""

    def __init__(self, sandbox: SandboxClient) -> None:
        self._sandbox = sandbox

    @property
    def id(self) -> str:
        return self._sandbox.sandbox_id

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
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
        responses: list[FileDownloadResponse] = []
        for path in paths:
            try:
                raw = self._sandbox.files.read(f"{_WORKDIR}/{path.lstrip('/')}", format="bytes")
                content = bytes(raw) if isinstance(raw, (bytes, bytearray)) else str(raw).encode()
                responses.append(FileDownloadResponse(path=path, content=content, error=None))
            except SandboxException:
                responses.append(FileDownloadResponse(path=path, content=None, error="file_not_found"))
        return responses


def connect_e2b_sandbox(settings: E2BSettings, *, sandbox_id: str | None) -> E2BSandboxBackend:
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
            sandbox = Sandbox.connect(sandbox_id, timeout=settings.timeout, api_key=api_key)
            return E2BSandboxBackend(sandbox)
        except SandboxException:
            LOGGER.warning("e2b sandbox %s gone (TTL?), creating a fresh one", sandbox_id)
    created = Sandbox.create(
        template=settings.template, timeout=settings.timeout, api_key=api_key
    )
    return E2BSandboxBackend(created)
