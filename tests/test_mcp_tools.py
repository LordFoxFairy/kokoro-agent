"""MCP 稳定工具面规格：恒定三工具 / 惰性连接 / 白名单 / 不可达降级 / 前缀恒定。"""

# BaseTool.ainvoke 上游注解含未解泛型（langchain-core 边界，e2e/test_mcp_live 同款豁免）。
# pyright: reportUnknownMemberType=false

from __future__ import annotations

from typing import Any

import pytest
from langchain_core.tools import BaseTool, StructuredTool
from pydantic import BaseModel, ConfigDict

import kokoro_agent.mcp.tools as tools_mod
from kokoro_agent.mcp.config import (
    McpConfigError,
    McpServerConfig,
    select_servers,
)
from kokoro_agent.mcp.servers import build_connections
from kokoro_agent.mcp.tools import make_mcp_tools


class _EchoArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str


def _echo_tool(name: str, description: str = "echo tool") -> BaseTool:
    def run(text: str) -> str:
        return f"{name}:{text}"

    return StructuredTool(name=name, description=description, args_schema=_EchoArgs, func=run)


class _FakeClient:
    """替身 client：记录连接次数，返回预置工具。"""

    instances: int = 0

    def __init__(self, *args: object, **kwargs: object) -> None:
        type(self).instances += 1

    async def get_tools(self, *, server_name: str | None = None) -> list[BaseTool]:
        return [_echo_tool("ok", "第一行说明\n第二行细节"), _echo_tool("blocked")]


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


def _patched(monkeypatch: pytest.MonkeyPatch, client: type) -> None:
    monkeypatch.setattr(tools_mod, "MultiServerMCPClient", client)


async def test_list_filters_whitelist_and_shows_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    _patched(monkeypatch, _FakeClient)
    list_tool, _, _ = make_mcp_tools(["srv"], REGISTRY)
    out = await list_tool.ainvoke({})
    assert "srv/ok — 第一行说明" in out
    assert "blocked" not in out  # 白名单外不可见。


async def test_empty_names_says_so() -> None:
    list_tool, _, _ = make_mcp_tools([], REGISTRY)
    assert "没有可用的 MCP server" in await list_tool.ainvoke({})


def test_unknown_server_name_fails_loud_at_assembly() -> None:
    # 配置即授权边界：装配期即炸，绝不静默跳过。
    with pytest.raises(McpConfigError, match="ghost"):
        make_mcp_tools(["ghost"], REGISTRY)


async def test_lazy_connection_and_per_run_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeClient.instances = 0
    _patched(monkeypatch, _FakeClient)
    list_tool, describe_tool, _ = make_mcp_tools(["srv"], REGISTRY)
    assert _FakeClient.instances == 0  # 装配期零连接（run 启动不被远端拖死）。
    await list_tool.ainvoke({})
    await describe_tool.ainvoke({"server": "srv", "tool": "ok"})
    assert _FakeClient.instances == 1  # run 内缓存：list/describe 共享一次连接。


async def test_describe_returns_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    _patched(monkeypatch, _FakeClient)
    _, describe_tool, _ = make_mcp_tools(["srv"], REGISTRY)
    out = await describe_tool.ainvoke({"server": "srv", "tool": "ok"})
    assert "text" in out and "schema" in out


async def test_call_roundtrip_and_unknown_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    _patched(monkeypatch, _FakeClient)
    _, _, call_tool = make_mcp_tools(["srv"], REGISTRY)
    result = await call_tool.ainvoke({"server": "srv", "tool": "ok", "arguments": {"text": "你好"}})
    assert result == "ok:你好"
    assert "error" in await call_tool.ainvoke({"server": "srv", "tool": "blocked", "arguments": {}})
    assert "error" in await call_tool.ainvoke({"server": "other", "tool": "ok", "arguments": {}})


async def test_unreachable_server_degrades_not_crashes(monkeypatch: pytest.MonkeyPatch) -> None:
    _patched(monkeypatch, _BoomClient)
    list_tool, _, call_tool = make_mcp_tools(["srv"], REGISTRY)
    listed = await list_tool.ainvoke({})
    assert "不可达" in listed  # 运行时不可达=外部常态：降级告知，不炸 run。
    assert "error" in await call_tool.ainvoke({"server": "srv", "tool": "ok", "arguments": {}})


def test_server_set_change_keeps_tool_surface_identical() -> None:
    # D9 前缀不变量：server 集 A/B/空 切换，三工具面（name/description/schema）逐字节相同。
    registry = {"a": _config(), "b": _config()}

    def surface(names: list[str]) -> list[tuple[str, str, str]]:
        out: list[tuple[str, str, str]] = []
        for tool in make_mcp_tools(names, registry):
            schema: Any = tool.args_schema
            assert isinstance(schema, type) and issubclass(schema, BaseModel)
            out.append((tool.name, tool.description, str(schema.model_json_schema())))
        return out

    assert surface(["a"]) == surface(["b", "a"]) == surface([])


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
