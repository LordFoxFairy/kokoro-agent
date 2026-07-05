"""filesystem 权限 + 执行 backend 选择：filesystem/backend 每请求经 wire 决定，参数进程级注入。"""

from __future__ import annotations

from pathlib import Path

from typing import Annotated

from deepagents.backends.local_shell import LocalShellBackend
from deepagents.backends.protocol import BackendProtocol
from deepagents.middleware.filesystem import FilesystemPermission
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.contract import Backend, FilesystemPerm


class SandboxSettings(BaseModel):
    """local_shell backend 的进程级参数；backend 选择属每请求维度，不归入。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    local_shell_root: str | None
    local_shell_inherit_env: bool
    local_shell_timeout: Annotated[int, Field(gt=0)]
    local_shell_max_output_bytes: Annotated[int, Field(gt=0)]


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
        return LocalShellBackend(
            root_dir=root,
            virtual_mode=False,
            timeout=settings.local_shell_timeout,
            max_output_bytes=settings.local_shell_max_output_bytes,
            inherit_env=settings.local_shell_inherit_env,
        )
    # e2b/custom backend V1 未落地：fail-loud，不静默降级为 state。
    raise NotImplementedError(f"backend {kind!r} is not supported in V1")
