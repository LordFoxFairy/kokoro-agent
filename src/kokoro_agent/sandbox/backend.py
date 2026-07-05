"""filesystem 权限 + 执行 backend 选择：filesystem/backend 每请求经 wire 决定，参数进程级注入。"""

from __future__ import annotations

from pathlib import Path

from typing import Annotated

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
        root = settings.local_shell_root
        if workspace is not None and root is not None:
            sub = Path(root) / workspace
            sub.mkdir(parents=True, exist_ok=True)
            root = str(sub)
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
    # e2b/custom backend V1 未落地：fail-loud，不静默降级为 state。
    raise NotImplementedError(f"backend {kind!r} is not supported in V1")
