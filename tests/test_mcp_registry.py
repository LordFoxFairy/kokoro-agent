"""MCP Mongo 注册表读路规格（真 Mongo）：双源合并 / namespace 覆盖 / 禁用遮蔽不回退 /
软删回退 / secret_ref env 展开与 fail-loud / secret:path 占名不可用 / 未知名 fail-loud 不变。"""

# BaseTool.ainvoke 上游注解含未解泛型（langchain-core 边界，test_mcp_tools 同款豁免）。
# pyright: reportUnknownMemberType=false

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncGenerator

import pytest
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from kokoro_agent.mcp.config import (
    McpConfigError,
    McpServerConfig,
    McpServerUnavailable,
    select_servers,
)
from kokoro_agent.mcp.registry import McpRegistry
from kokoro_agent.mcp.tools import make_mcp_tools

_MONGO_URL = os.environ.get("KOKORO_MONGO_URL", "mongodb://127.0.0.1:27017")
_NS = "ns1"


@pytest.fixture
async def collection() -> AsyncGenerator[AsyncCollection[dict[str, object]], None]:
    """真 mongo，唯一 collection 自隔离（test_skill_materialize 同款形态）。"""
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(_MONGO_URL)
    coll = client["kokoro_test"][f"mcp_servers_{uuid.uuid4().hex[:8]}"]
    try:
        yield coll
    finally:
        await coll.drop()
        await client.close()


def _doc(
    scope: str,
    name: str,
    *,
    url: str,
    enabled: bool = True,
    deleted_at: int | None = None,
    secret_ref: str | None = None,
    allowed_tools: list[str] | None = None,
) -> dict[str, object]:
    """按 contract/storage.McpServerDoc 形状种文档（写面权威在 kokoro-hub，此处直插种子）。"""
    return {
        "scope": scope,
        "name": name,
        "transport": "streamable_http",
        "url": url,
        "allowed_tools": allowed_tools if allowed_tools is not None else ["t"],
        "secret_ref": secret_ref,
        "enabled": enabled,
        "updated_at": 1,
        "deleted_at": deleted_at,
    }


def _yaml(url: str) -> McpServerConfig:
    return McpServerConfig.model_validate({"url": url, "allowed_tools": ["t"]})


async def test_merge_overlays_yaml_then_official_then_namespace(
    collection: AsyncCollection[dict[str, object]],
) -> None:
    # 合并序（低→高）：部署 yaml < Mongo official < Mongo namespace。
    await collection.insert_many(
        [
            _doc("official", "a", url="https://official/a"),
            _doc(_NS, "a", url="https://ns/a"),
            _doc("official", "b", url="https://official/b"),
        ]
    )
    registry = McpRegistry(collection, {})
    deploy = {"a": _yaml("https://yaml/a"), "c": _yaml("https://yaml/c")}
    merged = await registry.resolve(["a", "b", "c"], _NS, deploy)
    assert isinstance(merged["a"], McpServerConfig) and merged["a"].url == "https://ns/a"
    assert isinstance(merged["b"], McpServerConfig) and merged["b"].url == "https://official/b"
    assert merged["c"] == deploy["c"]  # 注册表无同名：yaml 基线原样保留。


async def test_disabled_namespace_doc_shadows_without_fallback(
    collection: AsyncCollection[dict[str, object]],
) -> None:
    # 活跃禁用文档占名遮蔽同名 official 与 yaml、不回退（对齐 hub 侧注释：绝不回退官方凭据）。
    await collection.insert_many(
        [
            _doc("official", "a", url="https://official/a"),
            _doc(_NS, "a", url="https://ns/a", enabled=False),
        ]
    )
    merged = await McpRegistry(collection, {}).resolve(["a"], _NS, {"a": _yaml("https://yaml/a")})
    assert isinstance(merged["a"], McpServerUnavailable)

    # 穿过恒定工具面：装配不炸（名字已知），list 标注不可用，call 是 error 文本。
    list_tool, _, call_tool = make_mcp_tools(["a"], merged)
    listed = await list_tool.ainvoke({})
    assert "a: [不可用]" in listed
    result = await call_tool.ainvoke({"server": "a", "tool": "t", "arguments": {}})
    assert result.startswith("error:") and "不可用" in result


async def test_disabled_official_doc_shadows_yaml(
    collection: AsyncCollection[dict[str, object]],
) -> None:
    # official 层禁用同样占名：管理面下架 yaml 种子 server，不回退部署定义。
    await collection.insert_one(_doc("official", "a", url="https://official/a", enabled=False))
    merged = await McpRegistry(collection, {}).resolve(["a"], _NS, {"a": _yaml("https://yaml/a")})
    assert isinstance(merged["a"], McpServerUnavailable)


async def test_soft_deleted_doc_falls_back_to_lower_layer(
    collection: AsyncCollection[dict[str, object]],
) -> None:
    # 软删=不存在（与禁用遮蔽相反）：namespace 软删回退 official；official 软删回退 yaml。
    await collection.insert_many(
        [
            _doc(_NS, "a", url="https://ns/a", deleted_at=123),
            _doc("official", "a", url="https://official/a"),
            _doc("official", "b", url="https://official/b", deleted_at=456),
        ]
    )
    merged = await McpRegistry(collection, {}).resolve(
        ["a", "b"], _NS, {"b": _yaml("https://yaml/b")}
    )
    assert isinstance(merged["a"], McpServerConfig) and merged["a"].url == "https://official/a"
    assert isinstance(merged["b"], McpServerConfig) and merged["b"].url == "https://yaml/b"


async def test_secret_ref_env_expands_into_authorization_header(
    collection: AsyncCollection[dict[str, object]],
) -> None:
    # env:VAR 整值注入 authorization（对齐 yaml ${ENV} 展开语义）；env 显式注入不碰进程环境。
    await collection.insert_one(_doc(_NS, "a", url="https://ns/a", secret_ref="env:MCP_TOK"))
    registry = McpRegistry(collection, {"MCP_TOK": "Bearer real-token"})
    merged = await registry.resolve(["a"], _NS, {})
    entry = merged["a"]
    assert isinstance(entry, McpServerConfig)
    assert entry.headers == {"authorization": "Bearer real-token"}


async def test_secret_ref_env_missing_fails_loud(
    collection: AsyncCollection[dict[str, object]],
) -> None:
    await collection.insert_one(_doc(_NS, "a", url="https://ns/a", secret_ref="env:NO_SUCH_TOK"))
    with pytest.raises(McpConfigError, match="NO_SUCH_TOK"):
        await McpRegistry(collection, {}).resolve(["a"], _NS, {})


async def test_secret_ref_vault_marks_server_unavailable_not_crash(
    collection: AsyncCollection[dict[str, object]],
) -> None:
    # secret:path V1 不支持（P2 网关侧解析）：装配不炸，调用时 error 文本。
    await collection.insert_one(_doc(_NS, "a", url="https://ns/a", secret_ref="secret:team/gh"))
    merged = await McpRegistry(collection, {}).resolve(["a"], _NS, {})
    assert isinstance(merged["a"], McpServerUnavailable)
    _, describe_tool, call_tool = make_mcp_tools(["a"], merged)
    assert "error" in await describe_tool.ainvoke({"server": "a", "tool": "t"})
    assert "error" in await call_tool.ainvoke({"server": "a", "tool": "t", "arguments": {}})


async def test_unknown_name_still_fails_loud_after_merge(
    collection: AsyncCollection[dict[str, object]],
) -> None:
    # names 仍是授权边界：两源都无此名 → 装配期 fail-loud 不变。
    await collection.insert_one(_doc(_NS, "a", url="https://ns/a"))
    merged = await McpRegistry(collection, {}).resolve(["a", "ghost"], _NS, {})
    with pytest.raises(McpConfigError, match="ghost"):
        make_mcp_tools(["ghost"], merged)


async def test_empty_names_passes_deploy_through(
    collection: AsyncCollection[dict[str, object]],
) -> None:
    # 快照未点名任何 server：不查注册表，yaml 基线原样透传。
    deploy = {"a": _yaml("https://yaml/a")}
    merged = await McpRegistry(collection, {}).resolve([], _NS, deploy)
    assert merged == dict(deploy)


async def test_unavailable_entry_selectable_by_name(
    collection: AsyncCollection[dict[str, object]],
) -> None:
    # 占名不可用位可被 select（已知名不炸），与未知名 fail-loud 形成对照。
    await collection.insert_one(_doc(_NS, "a", url="https://ns/a", enabled=False))
    merged = await McpRegistry(collection, {}).resolve(["a"], _NS, {})
    assert isinstance(select_servers(merged, ["a"])["a"], McpServerUnavailable)
