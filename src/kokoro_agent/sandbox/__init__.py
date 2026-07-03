"""沙箱域：执行 backend 与 filesystem 权限策略。"""

from __future__ import annotations

from kokoro_agent.sandbox.backend import (
    SandboxSettings,
    build_filesystem_permissions,
    make_backend,
)

__all__ = ["SandboxSettings", "build_filesystem_permissions", "make_backend"]
