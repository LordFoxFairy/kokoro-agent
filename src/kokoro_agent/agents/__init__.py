"""业务 agent 包层（plugin 式）：每类型一个自包含包，机制不随类型膨胀。

parts.py=与类型无关的共享装配件；agents/<type>/=业务包（配方 recipe.py + 人格
persona.md + 未来的类型专属 tools/skills）。web 传 entry → session 解析出
agent_type 上 wire → 此处注册表分派配方。新增 studio 类型 = 新增一个包 + 注册一行
（+契约枚举一值），机制零改动。

类型政策也在包声明里：pause_tools 是"chat 面暂停工具"（general 有 ask_user；
无对话面的 studio 类型为空集——它们根本不该挂人机问答工具）。
"""

from __future__ import annotations

from kokoro_agent.agents.general import GENERAL_PACKAGE
from kokoro_agent.agents.package import AgentTypePackage, Assembler
from kokoro_agent.agents.parts import (
    AssembleDeps,
    AssembledAgent,
    build_web_tools,
)
from kokoro_agent.contract import AgentType, RunRequest
from kokoro_agent.tools.registry import SUBAGENT_TOOL_NAME

AGENT_TYPES: dict[AgentType, AgentTypePackage] = {
    "general": GENERAL_PACKAGE,
}


def _package_for(request: RunRequest) -> AgentTypePackage:
    package = AGENT_TYPES.get(request.runtime.agent_type)
    if package is None:
        raise NotImplementedError(
            f"agent_type {request.runtime.agent_type!r} has no registered package"
        )
    return package


async def assemble(deps: AssembleDeps, request: RunRequest) -> AssembledAgent:
    """唯一装配入口：按 wire 的 agent_type 分派业务包配方；未知类型 fail-loud。"""
    return await _package_for(request).assemble(deps, request)


def approval_names(request: RunRequest) -> frozenset[str]:
    """pending 识别集 = wire 审批集 + 类型包 pause_tools（+ask 档委派工具）。"""
    names = frozenset(request.runtime.permissions.approval_tools) | _package_for(request).pause_tools
    if request.runtime.permissions.subagent_create == "ask":
        names |= {SUBAGENT_TOOL_NAME}
    return names


__all__ = [
    "AGENT_TYPES",
    "AgentTypePackage",
    "Assembler",
    "AssembleDeps",
    "AssembledAgent",
    "approval_names",
    "assemble",
    "build_web_tools",
]
