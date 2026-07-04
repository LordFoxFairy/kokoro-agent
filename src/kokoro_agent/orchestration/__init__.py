"""编排层公开面：共享装配件 + 各类型配方（general 现在；新增类型即新增配方文件）。"""

from kokoro_agent.orchestration.assemble import (
    AssembleDeps,
    AssembledAgent,
    approval_names,
    build_web_tools,
    catalog_subagents,
    general_purpose_subagent,
    wire_subagents,
)
from kokoro_agent.orchestration.context import compose_system_prompt
from kokoro_agent.orchestration.general import assemble_general

__all__ = [
    "AssembleDeps",
    "AssembledAgent",
    "approval_names",
    "assemble_general",
    "build_web_tools",
    "catalog_subagents",
    "compose_system_prompt",
    "general_purpose_subagent",
    "wire_subagents",
]
