"""MCP 连接装配：agent 侧 server 配置 → langchain StreamableHttpConnection。

连接构造处挂 egress 防线（mcp/egress.py）：strict 模式给每个连接注入 GuardedTransport
（连接前校验+锁定解析 IP，防 DNS rebinding，禁 redirect），off 模式放行。egress 模式是
进程级策略，由 worker/main.py 从已校验的 AppConfig 配置（KOKORO_MCP_EGRESS_MODE；本层不读
进程环境），本层经 current_egress_mode() 读取（gate/closure 脚本在 agent 注入 off 放行
127.0.0.1 fixture）。
"""

from __future__ import annotations

from collections.abc import Mapping

from langchain_mcp_adapters.sessions import Connection, StreamableHttpConnection

from kokoro_agent import metrics
from kokoro_agent.mcp.config import McpServerConfig
from kokoro_agent.mcp.egress import (
    AddressResolver,
    build_mcp_client_factory,
    current_egress_mode,
)


class McpConnectionError(Exception):
    def __init__(self, *args: object) -> None:
        # 每次构造即一次 MCP 连接失败（全部实例都是 raise 现场）：单点计数，fail-open。
        super().__init__(*args)
        metrics.record_mcp_unavailable()


# 返回 Connection 联合别名以匹配 MultiServerMCPClient 的不变 dict 参数。
def build_connections(
    servers: Mapping[str, McpServerConfig],
    *,
    egress_mode: str | None = None,
    resolver: AddressResolver | None = None,
) -> dict[str, Connection]:
    # egress_mode/resolver 缺省取进程级策略 / 默认解析器；测试可显式注入（fake resolver）。
    mode = egress_mode if egress_mode is not None else current_egress_mode()
    client_factory = build_mcp_client_factory(mode, resolver=resolver)
    connections: dict[str, Connection] = {}
    for name, server in servers.items():
        # transport 两值均映射到 streamable_http 连接类型。
        conn = StreamableHttpConnection(transport="streamable_http", url=server.url)
        if server.timeout_s is not None:
            conn["timeout"] = float(server.timeout_s)
        if server.headers is not None:
            # 凭据来自部署配置的 ${ENV} 展开 / Capability 句柄批解（mcp/config.py、mcp/local_registry.py）；
            # wire/run_repository 全程无凭据。
            conn["headers"] = dict(server.headers)
        if client_factory is not None:
            # strict：注入锁定解析 IP + 禁 redirect 的 httpx client，连接期动态防 SSRF/rebinding。
            conn["httpx_client_factory"] = client_factory
        connections[name] = conn
    return connections
