"""Sandbox backend selection for agent execution."""

from __future__ import annotations

from deepagents.backends.local_shell import LocalShellBackend
from deepagents.backends.protocol import BackendProtocol

from kokoro_agent.config import RuntimeSettings


def backend_from_settings(settings: RuntimeSettings) -> BackendProtocol | None:
    if settings.backend == "state":
        return None
    return LocalShellBackend(
        root_dir=settings.local_shell_root,
        virtual_mode=False,
        timeout=settings.local_shell_timeout,
        max_output_bytes=settings.local_shell_max_output_bytes,
        inherit_env=settings.local_shell_inherit_env,
    )
