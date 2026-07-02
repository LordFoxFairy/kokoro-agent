"""DeepAgents filesystem permissions for Kokoro trust modes."""

from deepagents.middleware.filesystem import FilesystemPermission

from kokoro_agent.run.request import PermissionMode


def build_filesystem_permissions(mode: PermissionMode) -> list[FilesystemPermission]:
    if mode == "auto":
        return []
    return [FilesystemPermission(operations=["write"], paths=["/**"], mode="deny")]
