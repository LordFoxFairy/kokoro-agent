"""Capability public contract 的 Skill 读取面。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Annotated, Protocol

from pydantic import BaseModel, ConfigDict, StringConstraints

from kokoro_agent.contract import ExecutionIdentity

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]


class ResolvedSkill(BaseModel):
    """一次 Skill 解析产生的读取凭据。

    这是 Capability client 的返回值，不是 Agent/Feature 配置。内容摘要只用于完整性和
    内容寻址读取；Agent 永远只声明 Skill 名称。
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    name: NonEmptyStr
    content_hash: NonEmptyStr
    description: str
    scope: NonEmptyStr


class SkillClientError(Exception):
    """Skill public contract 读取失败；调用方应按技能粒度 fail-closed。"""


class SkillClient(Protocol):
    """Capability 的 Skill 解析面。

    ``selectors`` 是 Agent/Feature 声明的稳定名称；Capability 根据可信
    ``ExecutionIdentity`` 与 GA 派生的 namespace 负责可见性、用户/项目/session logical path
    与授权，返回本次装配可读取的不可变 grant。
    grant 不进入 RunRequest，也不成为 Agent 的静态配置。
    """

    async def resolve(
        self, selectors: Sequence[str], identity: ExecutionIdentity, namespace: str
    ) -> tuple[ResolvedSkill, ...]: ...

class SkillReader(Protocol):
    """GA 在运行中需要的最小 Skill 读取面。

    CRUD、可见性和 logical path 由 Capability owner 负责；GA 只按已授权快照读取正文和包体。
    """

    async def load_package(
        self, scope: str, name: str, content_hash: str
    ) -> Mapping[str, str]: ...


class NoSkillsClient:
    """Deployment without Capability Skills; the base Agent remains runnable."""

    async def resolve(
        self,
        selectors: Sequence[str],
        identity: ExecutionIdentity,
        namespace: str,
    ) -> tuple[ResolvedSkill, ...]:
        del selectors, identity, namespace
        return ()

    async def load_package(
        self, scope: str, name: str, content_hash: str
    ) -> Mapping[str, str]:
        del scope, name, content_hash
        raise SkillClientError("Capability Skill client is not configured")


__all__ = [
    "NoSkillsClient",
    "ResolvedSkill",
    "SkillClient",
    "SkillClientError",
    "SkillReader",
]
