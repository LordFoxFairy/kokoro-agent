"""沙箱域：执行 backend 与 filesystem 权限策略。"""

from __future__ import annotations

from kokoro_agent.sandbox.archive import load_workspace_config
from kokoro_agent.sandbox.backend import (
    SandboxSettings,
    build_filesystem_permissions,
    make_backend,
    make_backend_for_run, teardown_backend_for_run,
    registered_backends,
)
from kokoro_agent.sandbox.custom_backend import CustomBackendContext
from kokoro_agent.sandbox.e2b_backend import E2BSettings

__all__ = [
    # BYO 工厂作者的公共契约（ADR-010）：from kokoro_agent.sandbox import CustomBackendContext
    "CustomBackendContext",
    "E2BSettings",
    "SandboxSettings",
    "build_filesystem_permissions",
    "load_workspace_config",
    "make_backend",
    "make_backend_for_run",
    "teardown_backend_for_run",
    "registered_backends",
]
