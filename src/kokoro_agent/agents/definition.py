"""可复用 Agent 的静态定义。

这里描述的是 DeepAgents 的构造输入，不是 DeepAgents 返回的 native runnable。定义不携带
Session、Run、namespace 或 worker 服务，因此可以被多个 Feature 复用。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from collections.abc import Iterable

from langchain_core.tools import StructuredTool

from kokoro_agent.policy import Backend, ModelConfig, Permissions

_KEY = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


@dataclass(frozen=True, slots=True)
class Agent:
    """一个完整、可独立运行的 DeepAgent 能力声明。"""

    key: str
    prompt: str
    tools: tuple[StructuredTool, ...] = ()
    skills: tuple[str, ...] = ()
    mcp: tuple[str, ...] = ()
    subagents: tuple[str, ...] = ()
    delivery: bool = False
    model: ModelConfig | None = None
    backend: Backend = "state"
    permissions: Permissions = Permissions()
    # 工具触发后需要等待用户回答的工具；其余审批规则由本次 Run 权限决定。
    pause_tools: frozenset[str] = frozenset()

    def configured(
        self,
        *,
        skills: Iterable[str] | None = None,
        mcp: Iterable[str] | None = None,
        prompt: str | None = None,
    ) -> Agent:
        """Return a feature-local copy without introducing a role/member type.

        Agent defaults stay reusable; a Feature may narrow the Skill/MCP surface or
        replace the prompt for one product entry while retaining the same Agent key.
        The copy is still a complete DeepAgents definition and is assembled by the
        same ``AgentFactory`` path.
        """
        return replace(
            self,
            skills=self.skills if skills is None else tuple(skills),
            mcp=self.mcp if mcp is None else tuple(mcp),
            prompt=self.prompt if prompt is None else prompt,
        )

    def __post_init__(self) -> None:
        if _KEY.fullmatch(self.key) is None:
            raise ValueError(f"invalid agent key: {self.key!r}")
        if not self.prompt.strip():
            raise ValueError(f"agent {self.key!r} needs a prompt")
        for label, values in (("skills", self.skills), ("mcp", self.mcp), ("subagents", self.subagents)):
            if len(set(values)) != len(values):
                raise ValueError(f"agent {self.key!r} has duplicate {label}")
        if not self.pause_tools.issubset({tool.name for tool in self.tools}):
            raise ValueError(f"agent {self.key!r} declares a pause tool that is not mounted")


__all__ = ["Agent"]
