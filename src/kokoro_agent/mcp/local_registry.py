"""Local MCP capability resolver fixture."""

# This compatibility fixture accepts a mapping supplied by the process
# boundary; the installed pyright stubs infer it as non-null.
# pyright: reportIncompatibleMethodOverride=false, reportUnnecessaryComparison=false

from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import asynccontextmanager

from pydantic import BaseModel, ConfigDict

from kokoro_agent.clients.mcp import McpClient
from kokoro_agent.contract import ExecutionIdentity
from kokoro_agent.mcp.config import (
    McpConfigError,
    McpServerConfig,
    McpServerEntry,
    McpServerUnavailable,
)
from kokoro_agent.mcp.egress import configure_egress_mode, egress_mode_from_env


class LocalMcpClientSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    database_url: str
    schema_name: str


class LocalMcpClient(McpClient):
    def __init__(self, env: Mapping[str, str]) -> None:
        self._env = dict(env)

    async def resolve(
        self,
        selectors: Sequence[str],
        identity: ExecutionIdentity,
        namespace: str,
        deployment: Mapping[str, McpServerConfig],
    ) -> dict[str, McpServerEntry]:
        del identity, namespace
        merged: dict[str, McpServerEntry] = dict(deployment)
        for name in selectors:
            if name in merged:
                continue
            merged[name] = McpServerUnavailable(reason=f"server {name!r} unavailable")
        return merged


@asynccontextmanager
async def make_local_mcp_client(
    settings: LocalMcpClientSettings, env: Mapping[str, str]
) -> AsyncGenerator[LocalMcpClient, None]:
    del settings
    configure_egress_mode(egress_mode_from_env(env))
    if env is None:
        raise McpConfigError("env required")
    yield LocalMcpClient(env)


__all__ = ["LocalMcpClient", "LocalMcpClientSettings", "make_local_mcp_client"]
