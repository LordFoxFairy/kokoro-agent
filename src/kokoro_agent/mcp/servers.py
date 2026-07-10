"""MCP 连接装配：agent 侧 server 配置 → langchain StreamableHttpConnection。"""

from __future__ import annotations

from collections.abc import Mapping

from langchain_mcp_adapters.sessions import Connection, StreamableHttpConnection

from kokoro_agent.mcp.config import McpServerConfig


class McpConnectionError(Exception):
    pass


# 返回 Connection 联合别名以匹配 MultiServerMCPClient 的不变 dict 参数。
def build_connections(servers: Mapping[str, McpServerConfig]) -> dict[str, Connection]:
    connections: dict[str, Connection] = {}
    for name, server in servers.items():
        # transport 两值均映射到 streamable_http 连接类型。
        conn = StreamableHttpConnection(transport="streamable_http", url=server.url)
        if server.timeout_s is not None:
            conn["timeout"] = float(server.timeout_s)
        if server.headers is not None:
            # 凭据来自部署配置的 ${ENV} 展开（mcp/config.py）；wire/ledger 全程无凭据。
            conn["headers"] = dict(server.headers)
        connections[name] = conn
    return connections
