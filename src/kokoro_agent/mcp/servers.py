"""MCP 连接装配：McpServer 契约 → langchain StreamableHttpConnection。"""

from __future__ import annotations

from collections.abc import Sequence

from langchain_mcp_adapters.sessions import Connection, StreamableHttpConnection

from kokoro_agent.contract import McpServer


class McpConnectionError(Exception):
    pass


# 返回 Connection 联合别名以匹配 MultiServerMCPClient 的不变 dict 参数。
def build_connections(servers: Sequence[McpServer]) -> dict[str, Connection]:
    connections: dict[str, Connection] = {}
    for server in servers:
        if server.name in connections:
            raise McpConnectionError(f"duplicate mcp server name {server.name!r}")
        # transport 两值均映射到 streamable_http 连接类型。
        conn = StreamableHttpConnection(transport="streamable_http", url=server.url)
        if server.timeout_s is not None:
            conn["timeout"] = float(server.timeout_s)
        if server.headers is not None:
            # 个人/私有 MCP 凭据直传（V1 内网）；升级路径=secret 引用（设计稿 hub v2）。
            conn["headers"] = dict(server.headers)
        connections[server.name] = conn
    return connections
