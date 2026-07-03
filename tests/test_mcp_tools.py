"""MCP 工具加载规格：白名单过滤 + 重命名 + fail-closed。"""

from __future__ import annotations

import pytest
from langchain_core.tools import BaseTool

import kokoro_agent.mcp.tools as tools_mod
from kokoro_agent.contract import McpServer
from kokoro_agent.mcp.servers import McpConnectionError
from kokoro_agent.mcp.tools import load_mcp_tools, tools_from_client


class _FakeTool(BaseTool):
    def _run(self, *args: object, **kwargs: object) -> str:
        return self.name


def _tool(name: str) -> BaseTool:
    return _FakeTool(name=name, description=name)


class _FakeClient:
    def __init__(self, tools: list[BaseTool]) -> None:
        self._tools = tools

    async def get_tools(self, *, server_name: str | None = None) -> list[BaseTool]:
        return list(self._tools)


class _BoomClient:
    def __init__(self, *args: object, **kwargs: object) -> None:
        pass

    async def get_tools(self, *, server_name: str | None = None) -> list[BaseTool]:
        raise RuntimeError("connection refused")


def _server() -> McpServer:
    return McpServer(
        name="srv",
        transport="streamable_http",
        url="http://localhost:9/mcp",
        allowed_tools=["ok", "also_ok"],
    )


async def test_whitelist_filter_and_rename() -> None:
    client = _FakeClient([_tool("ok"), _tool("blocked"), _tool("also_ok")])
    result = await tools_from_client(client, [_server()])
    assert [t.name for t in result] == ["mcp__srv__ok", "mcp__srv__also_ok"]


async def test_empty_servers_returns_empty() -> None:
    assert await load_mcp_tools([]) == []


async def test_client_error_wraps_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tools_mod, "MultiServerMCPClient", _BoomClient)
    with pytest.raises(McpConnectionError):
        await load_mcp_tools([_server()])
