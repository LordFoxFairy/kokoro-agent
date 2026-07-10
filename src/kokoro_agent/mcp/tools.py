"""MCP 工具加载：wire names → 部署配置解析 → 连接、白名单过滤、重命名 mcp__server__tool。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from langchain_core.tools import BaseTool
from langchain_mcp_adapters.client import MultiServerMCPClient

from kokoro_agent.mcp.config import McpConfigError, McpServerConfig, select_servers
from kokoro_agent.mcp.servers import McpConnectionError, build_connections


def mcp_tool_name(server: str, tool: str) -> str:
    return f"mcp__{server}__{tool}"


class _ToolClient(Protocol):
    async def get_tools(self, *, server_name: str | None = None) -> list[BaseTool]: ...


async def tools_from_client(
    client: _ToolClient, servers: Mapping[str, McpServerConfig]
) -> list[BaseTool]:
    loaded: list[BaseTool] = []
    for name, server in servers.items():
        allowed = frozenset(server.allowed_tools)
        for tool in await client.get_tools(server_name=name):
            if tool.name not in allowed:  # 白名单外一律丢弃。
                continue
            loaded.append(tool.model_copy(update={"name": mcp_tool_name(name, tool.name)}))
    return loaded


async def load_mcp_tools(
    names: Sequence[str], registry: Mapping[str, McpServerConfig]
) -> list[BaseTool]:
    if not names:
        return []
    servers = select_servers(registry, names)  # 未知名 fail-loud（配置即授权边界）。
    try:
        client = MultiServerMCPClient(build_connections(servers))
        return await tools_from_client(client, servers)
    except (McpConnectionError, McpConfigError):
        raise
    except Exception as exc:  # 连接/加载任何失败一律 fail-closed。
        raise McpConnectionError(f"failed to load mcp tools: {exc}") from exc
