"""工具面组装：一次 run 挂哪些工具、谁在授权白名单，一处看全。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from langchain_core.tools import BaseTool, StructuredTool

from kokoro_agent.agents.assembly.swarm import make_handoff_tool, swarm_candidates
from kokoro_agent.agents.deps import AssembleDeps
from kokoro_agent.contract import RunRequest
from kokoro_agent.contract.storage import workspace_key
from kokoro_agent.hub import ExecutionAssembly
from kokoro_agent.mcp.tools import make_mcp_tools
from kokoro_agent.tools.deliver import make_deliver_tool
from kokoro_agent.tools.registry import RESERVED_TOOL_NAMES, resolve_tools
from kokoro_agent.tools.skills import make_skill_tool


@dataclass(frozen=True, slots=True)
class Toolset:
    """本次 run 的工具全集与派生索引：一次构建，装配各步共用。"""

    tools: tuple[BaseTool, ...]
    # ToolPolicyMiddleware fail-closed 白名单：真挂载工具 + deepagents 保留工具（文件/执行/todo/task）。
    authorized: frozenset[str]
    # 子代理装配按名取工具：复用主链工具实例=复用其政策配置。
    by_name: Mapping[str, BaseTool]
    # 审批卡数据源：真挂载工具的自述（deepagents 保留工具不在册，wire 发空串由 web 兜底文案）。
    descriptions: Mapping[str, str]


async def build_toolset(
    request: RunRequest,
    deps: AssembleDeps,
    capabilities: ExecutionAssembly,
    *,
    core: tuple[StructuredTool, ...],
) -> Toolset:
    """四路工具来源合流（顺序即挂载序）：
    ① 注册表工具：wire 点名 + 类型核心工具（对话型=ask_user + propose_plan）
    ② 内置底座（恒挂，toolbox 一口出）：租户态 memory + 进程配置态 web_search/web_fetch
    ③ 技能工具（恒挂单工具，schema 不随池变）：清单在 prompt，正文经 skill(name) 按需读；
       附件物化由装配期 reconcile 中间件负责，工具只读账本判断就绪与否
    ④ MCP 稳定三工具（恒挂，schema 不随 server 集/远端漂移变）：list/describe/call
    ⑤ deliver 工具（恒挂，schema 不随配置变 D9）：无 workspace/无 deliveries 调用时降级
    ⑥ handoff 工具（仅本部署候选人格>1 时挂载）：会话内 swarm 移交主导人格
    """
    tools: list[BaseTool] = list(resolve_tools(request.runtime.tools, core=core))
    tools.extend(deps.toolbox.tools_for(request.context.namespace))
    tools.append(make_skill_tool(request.runtime.skills, capabilities.skills))
    mcp_grants = request.runtime.mcp_servers
    tools.extend(
        make_mcp_tools([grant.name for grant in mcp_grants], capabilities.mcp_servers)
    )
    tools.append(_deliver_tool(request, deps))
    candidates = swarm_candidates(deps.prompts)
    if len(candidates) > 1:
        tools.append(make_handoff_tool(candidates))
    return Toolset(
        tools=tuple(tools),
        authorized=frozenset(tool.name for tool in tools) | RESERVED_TOOL_NAMES,
        by_name={tool.name: tool for tool in tools},
        descriptions={tool.name: tool.description for tool in tools if tool.description},
    )


def _deliver_tool(request: RunRequest, deps: AssembleDeps) -> StructuredTool:
    """deliver 恒挂：工作区目录 = {local_shell_root}/{namespace:session_id}（同 backend 装配约定），
    root 缺省则无文件面 → workspace_dir=None（调用时降级）。deliveries 存储缺省=None 同理。"""
    root = deps.sandbox.local_shell_root
    namespace = request.context.namespace
    workspace_dir = (
        Path(root) / workspace_key(namespace, request.context.session_id) if root is not None else None
    )
    return make_deliver_tool(workspace_dir, deps.deliveries, namespace)
