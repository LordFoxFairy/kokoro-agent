"""MCP server Mongo 注册表读路（kokoro-hub HUB-3 写面的消费端）：装配定义双源合并。

- 合并序（低 → 高）：部署 yaml（official 基线）< Mongo official < Mongo namespace。
- 活跃文档（未软删）即占名：禁用文档遮蔽同名低层定义、不回退——绝不静默回退到
  官方/部署凭据（语义对齐 kokoro-hub MongoMcpServerRepository 注释）。软删=不存在，
  回退低层定义。
- 每 run 按能力快照的 names 精确查一次（快照定死 names，非全表）；连接惰性化仍在
  mcp/tools.py（本模块只产定义，不碰连接）。
- secret_ref 三档：
  - `env:VAR` → env 取值整值进 authorization header（对齐 yaml ${ENV} 展开语义：整值注入、
    缺失 fail-loud）；
  - `handle:srt_...` → 装配期收集本 run 全部句柄，一次批调 hub runtime resolve 换明文
    （caller=agent 凭据），明文整值进 authorization header 且只驻内存；批解失败 = 相关 server
    占名不可用，不炸 run；
  - `secret:path` → 留位废除（MCP-SECRET spec D1：不留兼容轴），遇到即 McpConfigError fail-loud。
env 由调用方显式注入（进程环境只在 worker/main 读取，架构规则）。
"""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import asynccontextmanager

from pydantic import BaseModel, ConfigDict
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from kokoro_agent.contract.storage import (
    MCP_SERVERS_COLLECTION,
    McpServerDoc,
    mcp_servers_doc_adapter,
)
from kokoro_agent.mcp.config import (
    McpConfigError,
    McpServerConfig,
    McpServerEntry,
    McpServerUnavailable,
)
from kokoro_agent.mcp.egress import configure_egress_mode, egress_mode_from_env
from kokoro_agent.mcp.secret_client import (
    SecretResolveError,
    SecretResolver,
    hub_secret_resolver_from_env,
)

# 官方位保留 scope 名（与 skills/hub.py OFFICIAL_SCOPE、kokoro-hub 侧常量同值）。
OFFICIAL_SCOPE = "official"

# env 引用形状与 hub 写面校验（MCP_SECRET_REF_RE 的 env 分支）同构；写面已拒明文凭据。
_ENV_SECRET_REF = re.compile(r"^env:([A-Z][A-Z0-9_]{0,127})$")
# handle 引用形状与 hub self 面 SELF_SECRET_REF_RE 同构（handle: + srt_ + 32 hex）。
_HANDLE_SECRET_REF = re.compile(r"^handle:(srt_[0-9a-f]{32})$")
_VAULT_SECRET_PREFIX = "secret:"


def _handle_of(secret_ref: str) -> str | None:
    """`handle:srt_...` → bare srt_ 句柄（供 hub 批解）；非 handle 形状 → None。"""
    matched = _HANDLE_SECRET_REF.match(secret_ref)
    return matched.group(1) if matched else None


def entry_from_doc(
    doc: McpServerDoc,
    env: Mapping[str, str],
    resolved_handles: Mapping[str, str],
    handle_resolve_failed: bool,
) -> McpServerEntry:
    """注册表文档 → 装配定义位。禁用/句柄解析失败 → 占名不可用；secret:/malformed/env 缺失 fail-loud。

    resolved_handles/handle_resolve_failed 由 resolve() 的一次批解产出（本函数不发网络请求）。
    """
    if not doc.enabled:
        # 活跃禁用文档占名遮蔽、不回退（对齐 hub 侧注释：绝不静默回退到官方凭据）。
        return McpServerUnavailable(reason=f"server 已禁用（scope={doc.scope}）")
    headers: dict[str, str] | None = None
    ref = doc.secret_ref
    if ref is not None:
        handle = _handle_of(ref)
        if handle is not None:
            # 批解失败或该句柄缺席（hub 全有或全无，200 必含全量）：占名不可用，不炸 run。
            if handle_resolve_failed or handle not in resolved_handles:
                return McpServerUnavailable(
                    reason=f"凭据句柄 {handle} 解析失败（server 暂不可用）"
                )
            # 明文即完整 header 值（如 "Bearer xxx"），整值注入且只驻内存，绝不落库/落日志。
            headers = {"authorization": resolved_handles[handle]}
        elif ref.startswith(_VAULT_SECRET_PREFIX):
            # MCP-SECRET spec D1：secret:path 留位废除，不再占名降级——遇到即 fail-loud。
            raise McpConfigError(
                f"mcp server {doc.name!r} secret_ref {ref!r} 已废除（D1：仅支持 handle:/env:）"
            )
        else:
            matched = _ENV_SECRET_REF.match(ref)
            if matched is None:
                raise McpConfigError(f"mcp server {doc.name!r} secret_ref {ref!r} malformed")
            value = env.get(matched.group(1))
            if value is None or value == "":
                # 对齐 yaml ${ENV} 展开语义：凭据引用缺失 fail-loud，绝不带残缺凭据连接。
                raise McpConfigError(f"mcp server {doc.name!r} secret_ref {ref!r} is not set")
            # env 值即完整 header 值，与 yaml headers ${ENV} 整值注入同轴。
            headers = {"authorization": value}
    return McpServerConfig(
        transport=doc.transport,
        url=doc.url,
        allowed_tools=list(doc.allowed_tools),
        headers=headers,
    )


class McpRegistry:
    """mcp_servers 集合读面：per-run 定义解析（写面/池查询权威在 kokoro-hub）。"""

    def __init__(
        self,
        collection: AsyncCollection[dict[str, object]],
        env: Mapping[str, str],
        secret_resolver: SecretResolver | None = None,
    ) -> None:
        self._collection = collection
        # 启动期快照（worker/main 显式注入进程 env）：凭据解析不再触碰进程环境。
        self._env = dict(env)
        # hub 凭据解析出口（`handle:` 换明文）；None = 未配置 → handle 引用占名不可用不炸 run。
        self._secret_resolver = secret_resolver

    async def resolve(
        self,
        names: Sequence[str],
        namespace: str,
        deploy: Mapping[str, McpServerConfig],
    ) -> dict[str, McpServerEntry]:
        """双源合并定义表；names 仍是授权边界——未知名由 select_servers fail-loud 不变。"""
        if not names:
            return dict(deploy)
        cursor = self._collection.find(
            {
                "name": {"$in": sorted(set(names))},
                "scope": {"$in": [OFFICIAL_SCOPE, namespace]},
                "deleted_at": None,  # 软删=不存在：回退低层定义。
            }
        )
        official: dict[str, McpServerDoc] = {}
        owned: dict[str, McpServerDoc] = {}
        async for raw in cursor:
            doc = mcp_servers_doc_adapter.validate_python(
                {key: value for key, value in raw.items() if key != "_id"}
            )
            (owned if doc.scope == namespace else official)[doc.name] = doc
        resolved_handles, handle_resolve_failed = await self._resolve_handles(
            official, owned, namespace
        )
        merged: dict[str, McpServerEntry] = dict(deploy)
        for layer in (official, owned):  # namespace 层后写入 = 覆盖同名 official 与 yaml。
            for name, doc in layer.items():
                merged[name] = entry_from_doc(
                    doc, self._env, resolved_handles, handle_resolve_failed
                )
        return merged

    async def _resolve_handles(
        self,
        official: Mapping[str, McpServerDoc],
        owned: Mapping[str, McpServerDoc],
        namespace: str,
    ) -> tuple[Mapping[str, str], bool]:
        """收集本 run 启用文档的全部 `handle:` 句柄，一次批解（每 run 至多一批）。

        返回 (handle→明文, 是否失败)。无句柄 → ({}, False)；无解析出口或批解异常 → ({}, True)
        （相关 server 由 entry_from_doc 占名不可用，不炸 run）。
        """
        handles: set[str] = set()
        for layer in (official, owned):
            for doc in layer.values():
                if doc.enabled and doc.secret_ref is not None:
                    handle = _handle_of(doc.secret_ref)
                    if handle is not None:
                        handles.add(handle)
        if not handles:
            return {}, False
        if self._secret_resolver is None:
            return {}, True
        try:
            resolved = await self._secret_resolver.resolve(namespace, sorted(handles))
        except SecretResolveError:
            return {}, True
        return resolved, False


class McpRegistrySettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    mongo_url: str
    mongo_db: str


@asynccontextmanager
async def make_mcp_registry(
    settings: McpRegistrySettings, env: Mapping[str, str]
) -> AsyncGenerator[McpRegistry, None]:
    """与 skills/hub.py 同库同客户端形态：进程级一个连接，run 内只做精确读。

    hub 凭据解析出口从同一注入 env 装配（KOKORO_HUB_BASE_URL + KOKORO_INTERNAL_SECRET_AGENT）；
    缺任一 → 无 `handle:` 解析能力（占名不可用不炸 run）。同时把连接期 egress 模式
    （KOKORO_MCP_EGRESS_MODE）从注入 env 配置为进程级策略——mcp 层唯一收到注入 env 的启动钩子，
    连接层 build_connections 据此设防（避免在连接层读进程环境，守 env 单点纪律）。"""
    configure_egress_mode(egress_mode_from_env(env))
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(settings.mongo_url)
    try:
        yield McpRegistry(
            client[settings.mongo_db][MCP_SERVERS_COLLECTION],
            env,
            hub_secret_resolver_from_env(env),
        )
    finally:
        await client.close()
