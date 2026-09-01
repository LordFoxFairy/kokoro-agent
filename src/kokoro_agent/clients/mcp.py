"""Capability public contract 的 MCP 读取面。

GA 只依赖这个窄协议。具体的 HTTP/gRPC 客户端由 worker 装配；本地内存 fixture 可以实现
同一协议，但不能让 Agent/Feature 直接依赖 Capability 的数据库结构。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from kokoro_agent.contract import ExecutionIdentity
from kokoro_agent.mcp.config import McpServerConfig, McpServerEntry


class McpClientError(Exception):
    """Capability MCP read failed before a server connection was attempted."""


class McpClient(Protocol):
    """GA 在一次运行中需要的 MCP 配置读取面。

    Callers provide only the names declared by an Agent. Snapshot/grant details stay inside the
    client implementation and never become Feature or Session fields.
    """

    async def resolve(
        self,
        selectors: Sequence[str],
        identity: ExecutionIdentity,
        namespace: str,
        deployment: Mapping[str, McpServerConfig],
    ) -> Mapping[str, McpServerEntry]: ...


__all__ = ["McpClient", "McpClientError"]
