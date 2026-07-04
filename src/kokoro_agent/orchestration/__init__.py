"""编排层公开面：RunRequest + RuntimeConfig → 可运行 InvokableAgent。

assemble = 每请求主配方（工具/守卫/子代理/后端/store 全拼装）；
context = 模型可见面唯一拼装点（人格 + 条件工具指引 + skills 全文）。
"""

from kokoro_agent.orchestration.assemble import (
    AssembleDeps,
    approval_names,
    assemble_agent,
    build_web_tools,
    catalog_subagents,
    general_purpose_subagent,
    wire_subagents,
)
from kokoro_agent.orchestration.context import (
    SteeringMiddleware,
    compose_system_prompt,
    render_tool_guidance,
)

__all__ = [
    "AssembleDeps",
    "SteeringMiddleware",
    "approval_names",
    "assemble_agent",
    "build_web_tools",
    "catalog_subagents",
    "compose_system_prompt",
    "general_purpose_subagent",
    "render_tool_guidance",
    "wire_subagents",
]
