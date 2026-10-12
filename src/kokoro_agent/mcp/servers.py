"""MCP connection assembly for standard streamable HTTP and stdio transports.

Remote HTTP connections receive the existing DNS-pinning/redirect egress
factory. Local stdio servers are launched directly through
``langchain-mcp-adapters``; no Kokoro transport protocol or custom process
wrapper is introduced.
"""

from __future__ import annotations

from collections.abc import Mapping

from langchain_mcp_adapters.sessions import (
    Connection,
    McpHttpClientFactory,
    StdioConnection,
    StreamableHttpConnection,
)

from kokoro_agent import metrics
from kokoro_agent.mcp.config import (
    McpHttpServerConfig,
    McpServerConfig,
    McpStdioServerConfig,
)
from kokoro_agent.mcp.egress import (
    AddressResolver,
    build_mcp_client_factory,
    current_egress_mode,
)


class McpConnectionError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        metrics.record_mcp_unavailable()


def _stdio_connection(server: McpStdioServerConfig) -> StdioConnection:
    connection: StdioConnection = {
        "transport": "stdio",
        "command": server.command,
        "args": list(server.args),
    }
    if server.cwd is not None:
        connection["cwd"] = server.cwd
    if server.env is not None:
        connection["env"] = dict(server.env)
    return connection


def _http_connection(
    server: McpHttpServerConfig,
    *,
    client_factory: McpHttpClientFactory | None,
) -> StreamableHttpConnection:
    connection: StreamableHttpConnection = {
        "transport": "streamable_http",
        "url": server.url,
    }
    if server.timeout_s is not None:
        connection["timeout"] = float(server.timeout_s)
    if server.headers is not None:
        connection["headers"] = dict(server.headers)
    if client_factory is not None:
        connection["httpx_client_factory"] = client_factory
    return connection


def build_connections(
    servers: Mapping[str, McpServerConfig],
    *,
    egress_mode: str | None = None,
    resolver: AddressResolver | None = None,
) -> dict[str, Connection]:
    """Build standard adapter connection dictionaries without opening them.

    Construction is deliberately side-effect-free. The upstream adapter starts
    a stdio child only at the first tool discovery/call, and closes it when that
    session returns. HTTP-only egress policy never leaks into stdio entries.
    """

    mode = egress_mode if egress_mode is not None else current_egress_mode()
    client_factory = build_mcp_client_factory(mode, resolver=resolver)
    connections: dict[str, Connection] = {}
    for name, server in servers.items():
        if isinstance(server, McpStdioServerConfig):
            connections[name] = _stdio_connection(server)
            continue
        connections[name] = _http_connection(server, client_factory=client_factory)
    return connections
