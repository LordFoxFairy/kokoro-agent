"""工具面组装：一次 run 挂哪些工具、谁在授权白名单，一处看全。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from deepagents.backends.protocol import BackendProtocol
from langchain_core.tools import BaseTool, StructuredTool

from kokoro_agent.protocol import RunRequest
from kokoro_agent.agents.definition import Agent
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.mcp.config import McpServerEntry, McpServerUnavailable
from kokoro_agent.mcp.tools import make_mcp_tools
from kokoro_agent.clients.mcp import McpClientError
from kokoro_agent.clients.mcp import McpClient
from kokoro_agent.clients.storage import DeliveryClient
from kokoro_agent.domain.run.models import LeaseFence
from kokoro_agent.tools.deliver import make_deliver_tool
from kokoro_agent.tools.registry import RESERVED_TOOL_NAMES, resolve_tools
from kokoro_agent.tools.toolbox import ProcessToolbox
from kokoro_agent.mcp.config import McpServerConfig


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

    @classmethod
    def from_tools(
        cls,
        tools: Sequence[BaseTool],
        *,
        implicit_authorized: frozenset[str] = RESERVED_TOOL_NAMES,
    ) -> Toolset:
        """Build all derived indexes once and reject cross-source name collisions."""

        by_name: dict[str, BaseTool] = {}
        for tool in tools:
            if tool.name in by_name:
                raise ValueError(
                    f"duplicate tool name across GA sources: {tool.name!r}"
                )
            by_name[tool.name] = tool
        return cls(
            tools=tuple(tools),
            authorized=frozenset(by_name) | implicit_authorized,
            by_name=by_name,
            descriptions={
                name: tool.description
                for name, tool in by_name.items()
                if tool.description
            },
        )

    def with_tools(self, tools: Sequence[BaseTool]) -> Toolset:
        """Add Feature-local tools while preserving the existing authorization base."""

        mounted_names = frozenset(tool.name for tool in self.tools)
        implicit = self.authorized - mounted_names
        return self.from_tools((*self.tools, *tools), implicit_authorized=implicit)


async def build_toolset(
    request: RunRequest,
    *,
    agent: Agent,
    toolbox: ProcessToolbox,
    mcp_servers: Mapping[str, McpServerConfig],
    mcp_client: McpClient | None,
    backend: BackendProtocol,
    delivery: DeliveryClient | None,
    lease: LeaseFence | None = None,
    resolved_mcp: Mapping[str, McpServerEntry] | None = None,
) -> Toolset:
    """五路工具来源合流（顺序即挂载序）：
    ① 注册表工具：wire 点名 + 类型核心工具（对话型=ask_user）
    ② 内置底座（恒挂，toolbox 一口出）：租户态 memory + 进程配置态 web_search/web_fetch
    ③ MCP 稳定三工具（恒挂，schema 不随 server 集/远端漂移变）：list/describe/call
    ④ deliver 工具：只有 Agent 明确声明且 Storage public client 存在时装配
    ⑤ peer handoff 只在 Feature 选择 official Swarm 时由 swarm.py 装配，不混入单 Agent 工具面。
    """
    scope = RunScope.of(request)
    tools: list[BaseTool] = list(resolve_tools([], core=agent.tools))
    tools.extend(toolbox.tools_for(scope.namespace))
    mcp_names = list(agent.mcp)
    mcp_definitions = (
        resolved_mcp
        if resolved_mcp is not None
        else await resolve_declared_mcp(request, agent, mcp_client, mcp_servers)
    )
    tools.extend(make_mcp_tools(mcp_names, mcp_definitions))
    if agent.delivery and delivery is not None:
        tools.append(_deliver_tool(request, backend, delivery, lease))
    return Toolset.from_tools(tools)


async def resolve_declared_mcp(
    request: RunRequest,
    agent: Agent,
    client: McpClient | None,
    deployment: Mapping[str, McpServerConfig],
) -> Mapping[str, McpServerEntry]:
    """No declaration means no discovery; declarations never fall back to YAML."""
    if not agent.mcp:
        return {}
    if client is None:
        raise McpClientError("declared MCP client unavailable")
    resolved = await client.resolve(
        agent.mcp,
        request.execution_identity,
        RunScope.of(request).namespace,
        deployment,
    )
    if set(resolved) != set(agent.mcp) or any(
        isinstance(entry, McpServerUnavailable) for entry in resolved.values()
    ):
        raise McpClientError("declared MCP resolution incomplete")
    return resolved


def _deliver_tool(
    request: RunRequest,
    backend: BackendProtocol,
    delivery: DeliveryClient,
    lease: LeaseFence | None,
) -> StructuredTool:
    """Read through the same DeepAgents backend used by filesystem tools."""
    namespace = RunScope.of(request).namespace
    return make_deliver_tool(
        backend,
        delivery,
        namespace=namespace,
        run_id=request.run_id,
        identity=request.execution_identity,
        lease=lease,
    )
