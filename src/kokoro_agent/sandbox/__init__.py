"""沙箱域：执行 backend 与 filesystem 权限策略。"""

from __future__ import annotations

from kokoro_agent.sandbox.archive import load_workspace_config
from kokoro_agent.sandbox.backend import (
    SandboxSettings,
    build_filesystem_permissions,
    make_backend,
    make_backend_for_run,
)
from kokoro_agent.sandbox.e2b_backend import E2BSettings

__all__ = [
    "E2BSettings",
    "SandboxSettings",
    "build_filesystem_permissions",
    "load_workspace_config",
    "make_backend",
    "make_backend_for_run",
]
