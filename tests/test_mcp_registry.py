"""MCP 装配定义解析规格（真 Mongo，MCP-REVISION 消费端）：会话快照 McpGrant[] → 不可变
版本快照。覆盖 config_hash 校验 fail-closed / 未知 revision 拒装 / 活文档 disable·软删·缺失
fail-closed（紧急撤销）/ 版本锁定 / secret_ref env·handle·secret: 三档 / 明文不落日志 /
yaml 兜底仅无 grant / 未知名 fail-loud。

合并/覆盖/池查询语义已上移 hub+session（agent 只按 grant 精确取快照，不再做 official/namespace 合并）。
"""

# BaseTool.ainvoke 上游注解含未解泛型（langchain-core 边界，test_mcp_tools 同款豁免）。
# pyright: reportUnknownMemberType=false

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import AsyncGenerator, Mapping, Sequence

import pytest
from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase

from kokoro_agent.contract.control import McpGrant
from kokoro_agent.contract.storage import (
    MCP_SERVER_REVISIONS_COLLECTION,
    MCP_SERVERS_COLLECTION,
)
from kokoro_agent.mcp.config import (
    McpConfigError,
    McpServerConfig,
    McpServerUnavailable,
    select_servers,
)
from kokoro_agent.mcp.registry import McpRegistry
from kokoro_agent.mcp.secret_client import SecretResolveError
from kokoro_agent.mcp.tools import make_mcp_tools

_MONGO_URL = os.environ.get("KOKORO_MONGO_URL", "mongodb://127.0.0.1:27017")
_NS = "ns1"
_HASH_A = "a" * 64
_HASH_B = "b" * 64
# 合法句柄形状（srt_ + 32 hex，与 hub SELF_SECRET_REF_RE 同构）。
_HANDLE_A = "srt_" + "a1b2c3d4" * 4
_HANDLE_B = "srt_" + "0f1e2d3c" * 4


class _FakeResolver:
    """替身 SecretResolver：记录批解调用，返回预置明文或抛 SecretResolveError（模拟 hub 全有或全无）。"""

    def __init__(self, secrets: Mapping[str, str] | None = None, *, fail: bool = False) -> None:
        self._secrets = dict(secrets or {})
        self._fail = fail
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    async def resolve(self, namespace: str, handles: Sequence[str]) -> Mapping[str, str]:
        self.calls.append((namespace, tuple(handles)))
        if self._fail:
            raise SecretResolveError("simulated hub failure")
        if any(h not in self._secrets for h in handles):
            raise SecretResolveError("simulated 404")
        return {h: self._secrets[h] for h in handles}


@pytest.fixture
async def db() -> AsyncGenerator[AsyncDatabase[dict[str, object]], None]:
    """真 mongo，唯一 db 自隔离：revisions + servers 两集合同库（读写分离热路径）。"""
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(_MONGO_URL)
    database = client[f"kokoro_test_mcp_{uuid.uuid4().hex[:8]}"]
    try:
        yield database
    finally:
        await client.drop_database(database.name)
        await client.close()


def _registry(
    database: AsyncDatabase[dict[str, object]],
    env: Mapping[str, str] | None = None,
    resolver: _FakeResolver | None = None,
) -> McpRegistry:
    return McpRegistry(
        database[MCP_SERVER_REVISIONS_COLLECTION],
        database[MCP_SERVERS_COLLECTION],
        env or {},
        resolver,
    )


async def _seed_rev(
    database: AsyncDatabase[dict[str, object]],
    scope: str,
    name: str,
    revision: int,
    config_hash: str,
    *,
    url: str,
    secret_ref: str | None = None,
    allowed_tools: list[str] | None = None,
    transport: str = "streamable_http",
) -> None:
    await database[MCP_SERVER_REVISIONS_COLLECTION].insert_one(
        {
            "scope": scope,
            "name": name,
            "revision": revision,
            "config_hash": config_hash,
            "transport": transport,
            "url": url,
            "allowed_tools": allowed_tools if allowed_tools is not None else ["t"],
            "secret_ref": secret_ref,
            "created_at": 1,
        }
    )


async def _seed_live(
    database: AsyncDatabase[dict[str, object]],
    scope: str,
    name: str,
    revision: int,
    *,
    url: str,
    enabled: bool = True,
    deleted_at: int | None = None,
    secret_ref: str | None = None,
    allowed_tools: list[str] | None = None,
) -> None:
    await database[MCP_SERVERS_COLLECTION].insert_one(
        {
            "scope": scope,
            "name": name,
            "revision": revision,
            "transport": "streamable_http",
            "url": url,
            "allowed_tools": allowed_tools if allowed_tools is not None else ["t"],
            "secret_ref": secret_ref,
            "enabled": enabled,
            "updated_at": 1,
            "deleted_at": deleted_at,
        }
    )


async def _seed(
    database: AsyncDatabase[dict[str, object]],
    scope: str,
    name: str,
    revision: int,
    config_hash: str,
    *,
    url: str,
    enabled: bool = True,
    deleted_at: int | None = None,
    secret_ref: str | None = None,
    allowed_tools: list[str] | None = None,
) -> None:
    """常态：活文档与其当前 revision 快照一致落地（hub upsert 的形态）。"""
    await _seed_rev(
        database, scope, name, revision, config_hash,
        url=url, secret_ref=secret_ref, allowed_tools=allowed_tools,
    )
    await _seed_live(
        database, scope, name, revision,
        url=url, enabled=enabled, deleted_at=deleted_at,
        secret_ref=secret_ref, allowed_tools=allowed_tools,
    )


def _grant(scope: str, name: str, revision: int, config_hash: str) -> McpGrant:
    return McpGrant(scope=scope, name=name, revision=revision, config_hash=config_hash)


def _yaml(url: str) -> McpServerConfig:
    return McpServerConfig.model_validate({"url": url, "allowed_tools": ["t"]})


async def test_grant_resolves_to_snapshot_config(db: AsyncDatabase[dict[str, object]]) -> None:
    # config_hash 匹配 + 活文档 enabled → 用快照 url/tools 装配。
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a/v1", allowed_tools=["search", "read"])
    merged = await _registry(db).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})
    entry = merged["a"]
    assert isinstance(entry, McpServerConfig)
    assert entry.url == "https://ns/a/v1"
    assert entry.allowed_tools == ["search", "read"]


async def test_config_hash_mismatch_fails_closed(db: AsyncDatabase[dict[str, object]]) -> None:
    # grant.config_hash 与快照行不一致（授权内容漂移/篡改）→ fail-closed 拒装。
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a")
    merged = await _registry(db).resolve([_grant(_NS, "a", 1, _HASH_B)], _NS, {})
    assert isinstance(merged["a"], McpServerUnavailable)
    assert "config_hash" in merged["a"].reason


async def test_unknown_revision_fails_closed(db: AsyncDatabase[dict[str, object]]) -> None:
    # 快照行不存在（未知 revision / 越界）→ fail-closed 拒装。
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a")
    merged = await _registry(db).resolve([_grant(_NS, "a", 9, _HASH_A)], _NS, {})
    assert isinstance(merged["a"], McpServerUnavailable)
    assert "revision=9" in merged["a"].reason


async def test_disabled_live_doc_fails_closed(db: AsyncDatabase[dict[str, object]]) -> None:
    # 紧急撤销：活文档 disable → 旧会话 grant 立即拒装（快照仍在，活文档说停就停）。
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a", enabled=False)
    merged = await _registry(db).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})
    assert isinstance(merged["a"], McpServerUnavailable)
    assert "禁用或撤销" in merged["a"].reason
    # 穿过恒定工具面：装配不炸（名字已知），list 标注不可用，call 是 error 文本。
    list_tool, _, call_tool = make_mcp_tools(["a"], merged)
    assert "a: [不可用]" in await list_tool.ainvoke({})
    result = await call_tool.ainvoke({"server": "a", "tool": "t", "arguments": {}})
    assert result.startswith("error:") and "不可用" in result


async def test_soft_deleted_live_doc_fails_closed(db: AsyncDatabase[dict[str, object]]) -> None:
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a", deleted_at=123)
    merged = await _registry(db).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})
    assert isinstance(merged["a"], McpServerUnavailable)


async def test_missing_live_doc_fails_closed(db: AsyncDatabase[dict[str, object]]) -> None:
    # 快照在但活文档缺失（异常状态）→ fail-closed，绝不装。
    await _seed_rev(db, _NS, "a", 1, _HASH_A, url="https://ns/a")
    merged = await _registry(db).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})
    assert isinstance(merged["a"], McpServerUnavailable)


async def test_version_lock_old_grant_keeps_original_config(
    db: AsyncDatabase[dict[str, object]],
) -> None:
    # 版本锁定：改版后活文档在 rev2，但旧会话 grant=rev1 仍取 rev1 快照的原 url。
    await _seed_rev(db, _NS, "a", 1, _HASH_A, url="https://ns/a/v1")
    await _seed_rev(db, _NS, "a", 2, _HASH_B, url="https://ns/a/v2")
    await _seed_live(db, _NS, "a", 2, url="https://ns/a/v2")  # 活文档已 bump 到 v2
    # 旧会话锁 rev1：
    old = await _registry(db).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})
    assert isinstance(old["a"], McpServerConfig) and old["a"].url == "https://ns/a/v1"
    # 新会话取 rev2：
    new = await _registry(db).resolve([_grant(_NS, "a", 2, _HASH_B)], _NS, {})
    assert isinstance(new["a"], McpServerConfig) and new["a"].url == "https://ns/a/v2"


async def test_secret_ref_env_expands_into_header(db: AsyncDatabase[dict[str, object]]) -> None:
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a", secret_ref="env:MCP_TOK")
    merged = await _registry(db, {"MCP_TOK": "Bearer real-token"}).resolve(
        [_grant(_NS, "a", 1, _HASH_A)], _NS, {}
    )
    entry = merged["a"]
    assert isinstance(entry, McpServerConfig) and entry.headers == {"authorization": "Bearer real-token"}


async def test_secret_ref_env_missing_fails_loud(db: AsyncDatabase[dict[str, object]]) -> None:
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a", secret_ref="env:NO_SUCH_TOK")
    with pytest.raises(McpConfigError, match="NO_SUCH_TOK"):
        await _registry(db).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})


async def test_secret_ref_vault_ref_fails_loud(db: AsyncDatabase[dict[str, object]]) -> None:
    # D1：secret:path 留位废除——装配期即 fail-loud，不再占名降级。
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a", secret_ref="secret:team/gh")
    with pytest.raises(McpConfigError, match="已废除"):
        await _registry(db).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})


async def test_secret_ref_handle_resolves_into_header(db: AsyncDatabase[dict[str, object]]) -> None:
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a", secret_ref=f"handle:{_HANDLE_A}")
    resolver = _FakeResolver({_HANDLE_A: "Bearer resolved-token"})
    merged = await _registry(db, {}, resolver).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})
    entry = merged["a"]
    assert isinstance(entry, McpServerConfig) and entry.headers == {"authorization": "Bearer resolved-token"}
    # 批解 caller 传入已验 namespace，句柄为 bare srt_（无 handle: 前缀）。
    assert resolver.calls == [(_NS, (_HANDLE_A,))]


async def test_multiple_handles_batched_single_call(db: AsyncDatabase[dict[str, object]]) -> None:
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a", secret_ref=f"handle:{_HANDLE_A}")
    await _seed(db, "official", "b", 1, _HASH_B, url="https://official/b", secret_ref=f"handle:{_HANDLE_B}")
    resolver = _FakeResolver({_HANDLE_A: "Bearer aa", _HANDLE_B: "Bearer bb"})
    merged = await _registry(db, {}, resolver).resolve(
        [_grant(_NS, "a", 1, _HASH_A), _grant("official", "b", 1, _HASH_B)], _NS, {}
    )
    assert isinstance(merged["a"], McpServerConfig) and merged["a"].headers == {"authorization": "Bearer aa"}
    assert isinstance(merged["b"], McpServerConfig) and merged["b"].headers == {"authorization": "Bearer bb"}
    assert len(resolver.calls) == 1  # 单次批解
    assert resolver.calls[0] == (_NS, tuple(sorted((_HANDLE_A, _HANDLE_B))))


async def test_handle_resolve_failure_marks_unavailable(db: AsyncDatabase[dict[str, object]]) -> None:
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a", secret_ref=f"handle:{_HANDLE_A}")
    merged = await _registry(db, {}, _FakeResolver(fail=True)).resolve(
        [_grant(_NS, "a", 1, _HASH_A)], _NS, {}
    )
    assert isinstance(merged["a"], McpServerUnavailable)


async def test_handle_without_resolver_marks_unavailable(db: AsyncDatabase[dict[str, object]]) -> None:
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a", secret_ref=f"handle:{_HANDLE_A}")
    merged = await _registry(db).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})
    assert isinstance(merged["a"], McpServerUnavailable)


async def test_disabled_server_secret_not_resolved(db: AsyncDatabase[dict[str, object]]) -> None:
    # 拒装的 server 不解密其凭据：disable 的 handle server 不进批解调用。
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a", secret_ref=f"handle:{_HANDLE_A}", enabled=False)
    resolver = _FakeResolver({_HANDLE_A: "Bearer x"})
    merged = await _registry(db, {}, resolver).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})
    assert isinstance(merged["a"], McpServerUnavailable)
    assert resolver.calls == []  # 无可装配 handle → 不发批解


async def test_resolved_plaintext_never_logged(
    db: AsyncDatabase[dict[str, object]], caplog: pytest.LogCaptureFixture
) -> None:
    secret = "Bearer top-secret-plaintext-should-never-log"
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a", secret_ref=f"handle:{_HANDLE_A}")
    resolver = _FakeResolver({_HANDLE_A: secret})
    with caplog.at_level(logging.DEBUG):
        merged = await _registry(db, {}, resolver).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})
    entry = merged["a"]
    assert isinstance(entry, McpServerConfig) and entry.headers == {"authorization": secret}
    assert "top-secret-plaintext-should-never-log" not in caplog.text


async def test_empty_grants_passes_deploy_through(db: AsyncDatabase[dict[str, object]]) -> None:
    # 会话未授权任何 mcp server：不查快照，yaml 部署基线原样透传。
    deploy = {"a": _yaml("https://yaml/a")}
    merged = await _registry(db).resolve([], _NS, deploy)
    assert merged == dict(deploy)


async def test_grant_overrides_yaml_same_name(db: AsyncDatabase[dict[str, object]]) -> None:
    # E2E-33 覆盖语义重述：yaml 死端口，grant 快照真地址覆盖同名（真相在 grant）。
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://real/a")
    merged = await _registry(db).resolve(
        [_grant(_NS, "a", 1, _HASH_A)], _NS, {"a": _yaml("https://dead-port/a")}
    )
    assert isinstance(merged["a"], McpServerConfig) and merged["a"].url == "https://real/a"


async def test_unknown_name_still_fails_loud(db: AsyncDatabase[dict[str, object]]) -> None:
    # names 仍是授权边界：make_mcp_tools 选一个 grant 未覆盖的名 → fail-loud。
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a")
    merged = await _registry(db).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})
    with pytest.raises(McpConfigError, match="ghost"):
        make_mcp_tools(["ghost"], merged)


async def test_unavailable_entry_selectable_by_name(db: AsyncDatabase[dict[str, object]]) -> None:
    # 占名不可用位可被 select（已知名不炸），与未知名 fail-loud 形成对照。
    await _seed(db, _NS, "a", 1, _HASH_A, url="https://ns/a", enabled=False)
    merged = await _registry(db).resolve([_grant(_NS, "a", 1, _HASH_A)], _NS, {})
    assert isinstance(select_servers(merged, ["a"])["a"], McpServerUnavailable)
