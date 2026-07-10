"""MCP 工具加载规格：wire names → 部署注册表解析 + 白名单过滤 + 重命名 + fail-closed。"""

from __future__ import annotations

from pathlib import Path

import pytest
from langchain_core.tools import BaseTool

import kokoro_agent.mcp.tools as tools_mod
from kokoro_agent.mcp.config import (
    McpConfigError,
    McpServerConfig,
    load_mcp_servers,
    select_servers,
)
from kokoro_agent.mcp.servers import build_connections
from kokoro_agent.mcp.tools import McpConnectionError, load_mcp_tools, tools_from_client


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


def _config(**overrides: object) -> McpServerConfig:
    base: dict[str, object] = {
        "url": "http://localhost:9/mcp",
        "allowed_tools": ["ok", "also_ok"],
    }
    base.update(overrides)
    return McpServerConfig.model_validate(base)


REGISTRY = {"srv": _config()}


async def test_whitelist_filter_and_rename() -> None:
    client = _FakeClient([_tool("ok"), _tool("blocked"), _tool("also_ok")])
    result = await tools_from_client(client, {"srv": _config()})
    assert [t.name for t in result] == ["mcp__srv__ok", "mcp__srv__also_ok"]


async def test_empty_names_returns_empty() -> None:
    assert await load_mcp_tools([], REGISTRY) == []


async def test_unknown_server_name_fails_loud() -> None:
    # 配置即授权边界：wire 点名未配置的 server 必须炸，绝不静默跳过。
    with pytest.raises(McpConfigError, match="ghost"):
        await load_mcp_tools(["ghost"], REGISTRY)


async def test_client_error_wraps_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tools_mod, "MultiServerMCPClient", _BoomClient)
    with pytest.raises(McpConnectionError):
        await load_mcp_tools(["srv"], REGISTRY)


def test_select_servers_dedupes_and_keeps_order() -> None:
    registry = {"a": _config(), "b": _config()}
    assert list(select_servers(registry, ["b", "a", "b"])) == ["b", "a"]


def test_connection_carries_headers_and_timeout() -> None:
    servers = {
        "gh": _config(
            url="https://mcp.example/x", timeout_s=5, headers={"authorization": "Bearer tok"}
        ),
        "pub": _config(url="https://mcp.example/y"),
    }
    conns = build_connections(servers)
    assert conns["gh"].get("headers") == {"authorization": "Bearer tok"}
    assert conns["gh"].get("timeout") == 5.0
    assert "headers" not in conns["pub"]


# --- 部署注册表加载（KOKORO_MCP_CONFIG yaml） ---


def test_load_mcp_servers_resolves_env_placeholder(tmp_path: Path) -> None:
    # 凭据只走 env：yaml 里只有 ${NAME} 引用名，加载时展开（env 显式注入，架构规则）。
    config = tmp_path / "mcp.yaml"
    config.write_text(
        "servers:\n"
        "  gh:\n"
        "    url: https://mcp.example/x\n"
        "    allowed_tools: [t]\n"
        "    headers:\n"
        "      authorization: ${GH_MCP_TOKEN}\n",
        encoding="utf-8",
    )
    registry = load_mcp_servers(str(config), {"GH_MCP_TOKEN": "Bearer real-token"})
    assert registry["gh"].headers == {"authorization": "Bearer real-token"}


def test_load_mcp_servers_missing_env_ref_fails_loud(tmp_path: Path) -> None:
    config = tmp_path / "mcp.yaml"
    config.write_text(
        "servers:\n"
        "  gh:\n"
        "    url: https://mcp.example/x\n"
        "    allowed_tools: [t]\n"
        "    headers: {authorization: '${NO_SUCH_TOKEN}'}\n",
        encoding="utf-8",
    )
    with pytest.raises(McpConfigError, match="NO_SUCH_TOKEN"):
        load_mcp_servers(str(config), {})


def test_load_mcp_servers_unknown_key_fails_loud(tmp_path: Path) -> None:
    config = tmp_path / "mcp.yaml"
    config.write_text(
        "servers:\n  gh:\n    url: https://x\n    allowed_tools: [t]\n    tokens: {a: b}\n",
        encoding="utf-8",
    )
    with pytest.raises(Exception):
        load_mcp_servers(str(config), {})


def test_load_mcp_servers_absent_path_is_empty_registry() -> None:
    assert load_mcp_servers(None, {}) == {}
    assert load_mcp_servers("", {}) == {}
