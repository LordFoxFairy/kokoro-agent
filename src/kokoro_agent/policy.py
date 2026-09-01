"""Worker-owned Agent policy values.

These values are Feature/Agent catalog data, not caller-controlled contract
fields. Keeping them outside ``contract`` makes the public launch frame unable
to smuggle a second runtime configuration into GA.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, StringConstraints
from typing import Annotated

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]
SubagentCreate = Literal["deny", "ask", "allow"]
FilesystemPerm = Literal["read_only", "workspace_write"]
Backend = Literal["state", "local_shell", "docker", "e2b", "custom"]


class PolicyModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class ModelConfig(PolicyModel):
    provider: NonEmptyStr
    name: NonEmptyStr
    effort: NonEmptyStr | None = None
    thinking: bool | None = None


class Permissions(PolicyModel):
    approval_tools: tuple[NonEmptyStr, ...] = ()
    review_tools: tuple[NonEmptyStr, ...] = ()
    subagent_create: SubagentCreate = "deny"
    filesystem: FilesystemPerm = "read_only"


__all__ = [
    "Backend",
    "FilesystemPerm",
    "ModelConfig",
    "Permissions",
    "SubagentCreate",
]
