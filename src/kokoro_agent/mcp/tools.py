"""MCP 工具加载：连接授权 server、白名单过滤、重命名为 mcp__server__tool。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from langchain_core.tools import BaseTool
from langchain_mcp_adapters.client import MultiServerMCPClient

from kokoro_agent.contract import McpServer
from kokoro_agent.mcp.servers import McpConnectionError, build_connections


def mcp_tool_name(server: str, tool: str) -> str:
    return f"mcp__{server}__{tool}"


class _ToolClient(Protocol):
    async def get_tools(self, *, server_name: str | None = None) -> list[BaseTool]: ...


async def tools_from_client(
    client: _ToolClient, servers: Sequence[McpServer]
) -> list[BaseTool]:
    loaded: list[BaseTool] = []
    for server in servers:
        allowed = frozenset(server.allowed_tools)
        for tool in await client.get_tools(server_name=server.name):
            if tool.name not in allowed:  # 白名单外一律丢弃。
                continue
            loaded.append(tool.model_copy(update={"name": mcp_tool_name(server.name, tool.name)}))
    return loaded


async def load_mcp_tools(servers: Sequence[McpServer]) -> list[BaseTool]:
    if not servers:
        return []
    try:
        client = MultiServerMCPClient(build_connections(servers))
        return await tools_from_client(client, servers)
    except McpConnectionError:
        raise
    except Exception as exc:  # 连接/加载任何失败一律 fail-closed。
        raise McpConnectionError(f"failed to load mcp tools: {exc}") from exc
