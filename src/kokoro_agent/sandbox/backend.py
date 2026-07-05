"""filesystem 权限 + 执行 backend 选择：filesystem/backend 每请求经 wire 决定，参数进程级注入。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

from typing import Annotated, Protocol

from deepagents.backends.local_shell import LocalShellBackend
from deepagents.backends.protocol import BackendProtocol
from deepagents.middleware.filesystem import FilesystemPermission
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from kokoro_agent.contract import Backend, FilesystemPerm
from kokoro_agent.sandbox.archive import (
    ArchivingLocalShellBackend,
    LocalWorkspace,
    S3Archiver,
    S3Workspace,
)
from kokoro_agent.sandbox.custom_backend import CustomBackendSettings, connect_custom_sandbox
from kokoro_agent.sandbox.docker_backend import (
    ArchivingDockerShellBackend,
    DockerSettings,
    connect_docker_sandbox,
)
from kokoro_agent.sandbox.e2b_backend import E2BSettings, connect_e2b_sandbox


class SandboxSettings(BaseModel):
    """local_shell backend 的进程级参数；backend 选择属每请求维度，不归入。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    local_shell_root: str | None
    local_shell_inherit_env: bool
    local_shell_timeout: Annotated[int, Field(gt=0)]
    local_shell_max_output_bytes: Annotated[int, Field(gt=0)]
    # 存储形态（ADR-009，与 session 读同一 yaml）：None=local 默认档，无归档动作。
    workspace: LocalWorkspace | S3Workspace | None
    workspace_s3_access_key: SecretStr | None
    workspace_s3_secret_key: SecretStr | None
    # e2b 云沙箱（ADR-009 1b）：api_key 缺失时选择 e2b backend 即 fail-loud。
    e2b: E2BSettings
    # docker 沙箱（ADR-009）：执行隔离进容器、文件面留宿主 workspace；image 缺失 fail-loud。
    docker: DockerSettings
    # custom 沙箱（ADR-010 BYO）：`pkg.module:factory` 自带实现；选 custom 未配引用即 fail-loud。
    custom: CustomBackendSettings

    @model_validator(mode="after")
    def _s3_requires_credentials(self) -> SandboxSettings:
        if isinstance(self.workspace, S3Workspace) and (
            self.workspace_s3_access_key is None or self.workspace_s3_secret_key is None
        ):
            raise ValueError(
                "workspace type s3 requires KOKORO_WORKSPACE_S3_ACCESS_KEY/SECRET_KEY"
            )
        return self


def build_filesystem_permissions(perm: FilesystemPerm) -> list[FilesystemPermission]:
    if perm == "workspace_write":
        return []
    return [FilesystemPermission(operations=["write"], paths=["/**"], mode="deny")]


def make_backend(
    kind: Backend, settings: SandboxSettings, *, workspace: str | None = None
) -> BackendProtocol | None:
    if kind == "state":
        return None
    if kind == "local_shell":
        # 工作区约定：{root}/{namespace:session_id}/ ——session files 端点按同约定直读。
        root = _workspace_root(settings, workspace)
        # s3 档（ADR-009）：写时归档装饰——session 读对象存储，写侧真源处增量推送。
        if (
            isinstance(settings.workspace, S3Workspace)
            and workspace is not None
            and root is not None
            and settings.workspace_s3_access_key is not None
            and settings.workspace_s3_secret_key is not None
        ):
            return ArchivingLocalShellBackend(
                root=Path(root),
                archiver=S3Archiver(
                    settings.workspace,
                    access_key=settings.workspace_s3_access_key,
                    secret_key=settings.workspace_s3_secret_key,
                ),
                prefix=workspace,
                timeout=settings.local_shell_timeout,
                max_output_bytes=settings.local_shell_max_output_bytes,
                inherit_env=settings.local_shell_inherit_env,
            )
        return LocalShellBackend(
            root_dir=root,
            # 虚拟根：模型的绝对路径（"/note.md"）映射进工作区，绝不触宿主真实根（越界即安全缺陷）。
            virtual_mode=True,
            timeout=settings.local_shell_timeout,
            max_output_bytes=settings.local_shell_max_output_bytes,
            inherit_env=settings.local_shell_inherit_env,
        )
    # docker/e2b/custom 有 run 级生命周期（ledger 绑定），走 make_backend_for_run 的 async 编排。
    raise ValueError(f"backend {kind} requires run-scoped assembly via make_backend_for_run")


def _workspace_root(settings: SandboxSettings, workspace: str | None) -> str | None:
    root = settings.local_shell_root
    if workspace is not None and root is not None:
        sub = Path(root) / workspace
        sub.mkdir(parents=True, exist_ok=True)
        return str(sub)
    return root


class SandboxBinding(Protocol):
    """run 级箱绑定存取（RunLedger 子集）：装配路径只依赖这两个方法。"""

    async def put_sandbox_id(self, run_id: str, sandbox_id: str) -> None: ...

    async def get_sandbox_id(self, run_id: str) -> str | None: ...


@dataclass(frozen=True)
class SandboxContext:
    """连接器唯一入参：本 run 的装配现场。"""

    settings: SandboxSettings
    workspace: str
    run_id: str
    # ledger 既往绑定（HITL resume 现场）：重连既往箱/容器而非新建。
    prior_sandbox_id: str | None


class SandboxConnector(Protocol):
    """每档一个连接器（Strategy）：sync 构造（编排层 to_thread），返回 None=无 backend。"""

    def __call__(self, context: SandboxContext) -> BackendProtocol | None: ...


def _connect_state(context: SandboxContext) -> BackendProtocol | None:
    return None


def _connect_local_shell(context: SandboxContext) -> BackendProtocol | None:
    return make_backend("local_shell", context.settings, workspace=context.workspace)


def _connect_docker(context: SandboxContext) -> BackendProtocol | None:
    root = _workspace_root(context.settings, context.workspace)
    if root is None:
        raise ValueError("backend docker requires KOKORO_AGENT_LOCAL_SHELL_ROOT")
    settings = context.settings
    backend = connect_docker_sandbox(
        settings.docker,
        root=Path(root),
        container_id=context.prior_sandbox_id,
        run_id=context.run_id,
        exec_timeout=settings.local_shell_timeout,
        max_output_bytes=settings.local_shell_max_output_bytes,
    )
    # docker + s3 组合档：与 local_shell 同享写时归档（缺此即静默丢文件面）。
    if (
        isinstance(settings.workspace, S3Workspace)
        and settings.workspace_s3_access_key is not None
        and settings.workspace_s3_secret_key is not None
    ):
        return ArchivingDockerShellBackend(
            root=Path(root),
            container_id=backend.container_id,
            archiver=S3Archiver(
                settings.workspace,
                access_key=settings.workspace_s3_access_key,
                secret_key=settings.workspace_s3_secret_key,
            ),
            prefix=context.workspace,
            timeout=settings.local_shell_timeout,
            max_output_bytes=settings.local_shell_max_output_bytes,
        )
    return backend


def _connect_e2b(context: SandboxContext) -> BackendProtocol | None:
    return connect_e2b_sandbox(context.settings.e2b, sandbox_id=context.prior_sandbox_id)


def _connect_custom(context: SandboxContext) -> BackendProtocol | None:
    return connect_custom_sandbox(
        context.settings.custom,
        run_id=context.run_id,
        workspace=context.workspace,
        workspace_root=_workspace_root(context.settings, context.workspace),
        prior_sandbox_id=context.prior_sandbox_id,
    )


# 注册表分派：加新档 = 写连接器 + 注册一行；覆盖度由 test_connectors_cover_backend_enum 守卫。
_CONNECTORS: dict[Backend, SandboxConnector] = {
    "state": _connect_state,
    "local_shell": _connect_local_shell,
    "docker": _connect_docker,
    "e2b": _connect_e2b,
    "custom": _connect_custom,
}


async def make_backend_for_run(
    kind: Backend,
    settings: SandboxSettings,
    *,
    workspace: str,
    run_id: str,
    binding: SandboxBinding,
) -> BackendProtocol | None:
    """统一装配入口：注册表选连接器 + 生命周期单点收口——
    产物带非空 `sandbox_id` 即落 ledger（keep-first），resume 经 prior 重连而非新建。
    """
    connector = _CONNECTORS.get(kind)
    if connector is None:
        raise NotImplementedError(f"backend {kind!r} has no registered connector")
    prior = await binding.get_sandbox_id(run_id)
    context = SandboxContext(
        settings=settings, workspace=workspace, run_id=run_id, prior_sandbox_id=prior
    )
    # 连接器一律 sync（docker CLI / SDK 网络调用秒级阻塞）：to_thread 让出事件循环。
    backend = await asyncio.to_thread(connector, context)
    bound = getattr(backend, "sandbox_id", None)
    if isinstance(bound, str) and bound and bound != prior:
        await binding.put_sandbox_id(run_id, bound)
    return backend


def registered_backends() -> frozenset[str]:
    """注册表覆盖面（守卫测试用）：与 Backend 枚举保持一致由测试强制。"""
    return frozenset(_CONNECTORS)
