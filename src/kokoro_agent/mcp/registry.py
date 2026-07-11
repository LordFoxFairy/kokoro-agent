"""MCP server Mongo 注册表读路（kokoro-hub HUB-3 写面的消费端）：装配定义双源合并。

- 合并序（低 → 高）：部署 yaml（official 基线）< Mongo official < Mongo namespace。
- 活跃文档（未软删）即占名：禁用文档遮蔽同名低层定义、不回退——绝不静默回退到
  官方/部署凭据（语义对齐 kokoro-hub MongoMcpServerRepository 注释）。软删=不存在，
  回退低层定义。
- 每 run 按能力快照的 names 精确查一次（快照定死 names，非全表）；连接惰性化仍在
  mcp/tools.py（本模块只产定义，不碰连接）。
- secret_ref：env:VAR → env 取值整值进 authorization header（对齐 yaml ${ENV} 展开
  语义：整值注入、缺失 fail-loud）；secret:path V1 不支持 → 该 server 占名不可用
  （装配不炸，调用时 error 文本；P2：secret 管理器在网关侧解析）。
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

# 官方位保留 scope 名（与 skills/hub.py OFFICIAL_SCOPE、kokoro-hub 侧常量同值）。
OFFICIAL_SCOPE = "official"

# env 引用形状与 hub 写面校验（MCP_SECRET_REF_RE 的 env 分支）同构；写面已拒明文凭据。
_ENV_SECRET_REF = re.compile(r"^env:([A-Z][A-Z0-9_]{0,127})$")
_VAULT_SECRET_PREFIX = "secret:"


def entry_from_doc(doc: McpServerDoc, env: Mapping[str, str]) -> McpServerEntry:
    """注册表文档 → 装配定义位。禁用/secret:path → 占名不可用；env 引用缺失 fail-loud。"""
    if not doc.enabled:
        # 活跃禁用文档占名遮蔽、不回退（对齐 hub 侧注释：绝不静默回退到官方凭据）。
        return McpServerUnavailable(reason=f"server 已禁用（scope={doc.scope}）")
    headers: dict[str, str] | None = None
    if doc.secret_ref is not None:
        if doc.secret_ref.startswith(_VAULT_SECRET_PREFIX):
            # P2：secret 管理器引用待网关侧解析；V1 占名不可用（装配不炸，调用时 error 文本）。
            return McpServerUnavailable(
                reason=f"secret_ref {doc.secret_ref!r} V1 不支持（仅 env:VAR）"
            )
        matched = _ENV_SECRET_REF.match(doc.secret_ref)
        if matched is None:
            raise McpConfigError(f"mcp server {doc.name!r} secret_ref {doc.secret_ref!r} malformed")
        value = env.get(matched.group(1))
        if value is None or value == "":
            # 对齐 yaml ${ENV} 展开语义：凭据引用缺失 fail-loud，绝不带残缺凭据连接。
            raise McpConfigError(f"mcp server {doc.name!r} secret_ref {doc.secret_ref!r} is not set")
        # env 值即完整 header 值（如 "Bearer xxx"），与 yaml headers ${ENV} 整值注入同轴。
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
        self, collection: AsyncCollection[dict[str, object]], env: Mapping[str, str]
    ) -> None:
        self._collection = collection
        # 启动期快照（worker/main 显式注入进程 env）：凭据解析不再触碰进程环境。
        self._env = dict(env)

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
        merged: dict[str, McpServerEntry] = dict(deploy)
        for layer in (official, owned):  # namespace 层后写入 = 覆盖同名 official 与 yaml。
            for name, doc in layer.items():
                merged[name] = entry_from_doc(doc, self._env)
        return merged


class McpRegistrySettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    mongo_url: str
    mongo_db: str


@asynccontextmanager
async def make_mcp_registry(
    settings: McpRegistrySettings, env: Mapping[str, str]
) -> AsyncGenerator[McpRegistry, None]:
    """与 skills/hub.py 同库同客户端形态：进程级一个连接，run 内只做精确读。"""
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(settings.mongo_url)
    try:
        yield McpRegistry(client[settings.mongo_db][MCP_SERVERS_COLLECTION], env)
    finally:
        await client.close()
