"""MCP server 装配定义解析（MCP-REVISION 消费端）：按会话快照 McpGrant[] 取不可变版本快照。

HUB-CONSIST 语义（wire 从 names 切为 McpGrant{scope,name,revision,config_hash}）：
- 每个 grant 按 (scope,name,revision) 读 mcp_server_revisions 不可变快照行——会话锁定的就是这份
  配置，绝不靠 wire 猜"现在的配置"，也不再在 agent 侧做 official/namespace 合并（合并已由 hub/
  session 权威解析成 grant）。
- config_hash 校验：grant.config_hash 与快照行 config_hash 不一致 = 授权内容已被篡改/漂移 →
  fail-closed 拒装（McpServerUnavailable 占名不可用，不炸 run）。
- 活文档 fail-closed：读 mcp_servers 活文档现况，disable/软删/缺失 → 拒装（紧急撤销对旧会话立即生效）。
- 版本锁定：快照行不可变，旧会话拿 revision=N 永远是那份 config；改版 bump 不影响旧会话。
- secret_ref 三档（沿用 MCP-SECRET 半场；轮换不 bump revision）：
  - `env:VAR` → env 取整值进 authorization header（缺失 fail-loud）；
  - `handle:srt_...` → 装配期收集本 run 全部句柄，一次批调 hub runtime resolve 换明文
    （caller=agent 凭据）；批解失败 = 相关 server 占名不可用，不炸 run；
  - `secret:path` → 留位废除（D1 不留兼容轴），遇到即 McpConfigError fail-loud。
- yaml 兜底仅限无 grant 的部署级 server（E2E-33 死端口覆盖语义重述：真相在 grant/快照，
  yaml 不再参与 namespace 池——granted 名恒由 grant 覆盖 yaml 同名）。
env 由调用方显式注入（进程环境只在 worker/main 读取，架构规则）。
"""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import asynccontextmanager

from pydantic import BaseModel, ConfigDict
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from kokoro_agent.contract.control import McpGrant
from kokoro_agent.contract.storage import (
    MCP_SERVER_REVISIONS_COLLECTION,
    MCP_SERVERS_COLLECTION,
    McpServerDoc,
    McpServerRevisionDoc,
    mcp_server_revisions_doc_adapter,
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

# env 引用形状与 hub 写面校验（MCP_SECRET_REF_RE 的 env 分支）同构；写面已拒明文凭据。
_ENV_SECRET_REF = re.compile(r"^env:([A-Z][A-Z0-9_]{0,127})$")
# handle 引用形状与 hub self 面 SELF_SECRET_REF_RE 同构（handle: + srt_ + 32 hex）。
_HANDLE_SECRET_REF = re.compile(r"^handle:(srt_[0-9a-f]{32})$")
_VAULT_SECRET_PREFIX = "secret:"


def _handle_of(secret_ref: str) -> str | None:
    """`handle:srt_...` → bare srt_ 句柄（供 hub 批解）；非 handle 形状 → None。"""
    matched = _HANDLE_SECRET_REF.match(secret_ref)
    return matched.group(1) if matched else None


def _headers_from_ref(
    name: str,
    secret_ref: str | None,
    env: Mapping[str, str],
    resolved_handles: Mapping[str, str],
    handle_resolve_failed: bool,
) -> dict[str, str] | None | McpServerUnavailable:
    """secret_ref → authorization header 值。

    - None → 无凭据（headers=None）。
    - `handle:` 批解失败/缺席 → McpServerUnavailable（占名不可用，不炸 run）。
    - `env:` 缺失 / `secret:` 废除 / malformed → McpConfigError fail-loud。
    """
    if secret_ref is None:
        return None
    handle = _handle_of(secret_ref)
    if handle is not None:
        # 批解失败或该句柄缺席（hub 全有或全无，200 必含全量）：占名不可用，不炸 run。
        if handle_resolve_failed or handle not in resolved_handles:
            return McpServerUnavailable(reason=f"凭据句柄 {handle} 解析失败（server 暂不可用）")
        # 明文即完整 header 值（如 "Bearer xxx"），整值注入且只驻内存，绝不落库/落日志。
        return {"authorization": resolved_handles[handle]}
    if secret_ref.startswith(_VAULT_SECRET_PREFIX):
        # MCP-SECRET spec D1：secret:path 留位废除，不再占名降级——遇到即 fail-loud。
        raise McpConfigError(
            f"mcp server {name!r} secret_ref {secret_ref!r} 已废除（D1：仅支持 handle:/env:）"
        )
    matched = _ENV_SECRET_REF.match(secret_ref)
    if matched is None:
        raise McpConfigError(f"mcp server {name!r} secret_ref {secret_ref!r} malformed")
    value = env.get(matched.group(1))
    if value is None or value == "":
        # 对齐 yaml ${ENV} 展开语义：凭据引用缺失 fail-loud，绝不带残缺凭据连接。
        raise McpConfigError(f"mcp server {name!r} secret_ref {secret_ref!r} is not set")
    # env 值即完整 header 值，与 yaml headers ${ENV} 整值注入同轴。
    return {"authorization": value}


class McpRegistry:
    """会话快照 McpGrant[] → per-run 装配定义表（写面/池查询/合并权威在 kokoro-hub）。

    读 hub 同库两集合（读写分离，每 run 不跨服务 RPC）：mcp_server_revisions（不可变快照 =
    授权内容）+ mcp_servers（活文档现况 = fail-closed 闸）。凭据明文经 hub runtime resolve 换。
    """

    def __init__(
        self,
        revisions: AsyncCollection[dict[str, object]],
        servers: AsyncCollection[dict[str, object]],
        env: Mapping[str, str],
        secret_resolver: SecretResolver | None = None,
    ) -> None:
        self._revisions = revisions
        self._servers = servers
        # 启动期快照（worker/main 显式注入进程 env）：凭据解析不再触碰进程环境。
        self._env = dict(env)
        # hub 凭据解析出口（`handle:` 换明文）；None = 未配置 → handle 引用占名不可用不炸 run。
        self._secret_resolver = secret_resolver

    async def resolve(
        self,
        grants: Sequence[McpGrant],
        namespace: str,
        deploy: Mapping[str, McpServerConfig],
    ) -> dict[str, McpServerEntry]:
        """grants → 装配定义表。grant 覆盖 yaml 同名（yaml 仅无 grant 的部署级 server 兜底）。"""
        merged: dict[str, McpServerEntry] = dict(deploy)
        if not grants:
            return merged
        snapshots = await self._load_snapshots(grants)
        live = await self._load_live(grants)
        resolved_handles, handle_resolve_failed = await self._resolve_handles(
            grants, snapshots, live, namespace
        )
        for grant in grants:
            merged[grant.name] = self._entry_for_grant(
                grant, snapshots, live, resolved_handles, handle_resolve_failed
            )
        return merged

    def _entry_for_grant(
        self,
        grant: McpGrant,
        snapshots: Mapping[tuple[str, str, int], McpServerRevisionDoc],
        live: Mapping[tuple[str, str], McpServerDoc],
        resolved_handles: Mapping[str, str],
        handle_resolve_failed: bool,
    ) -> McpServerEntry:
        snap = snapshots.get((grant.scope, grant.name, grant.revision))
        if snap is None:
            # 未知 revision（从未落格 / 越界）：fail-closed 拒装。
            return McpServerUnavailable(
                reason=f"版本快照 revision={grant.revision} 不存在（scope={grant.scope}）"
            )
        if snap.config_hash != grant.config_hash:
            # 授权内容锁不匹配（篡改/漂移）：fail-closed 拒装，绝不装一份未授权的配置。
            return McpServerUnavailable(
                reason=f"config_hash 不一致（授权内容已变，scope={grant.scope} revision={grant.revision}）"
            )
        status = live.get((grant.scope, grant.name))
        if status is None or status.deleted_at is not None or not status.enabled:
            # 活文档 disable/软删/缺失：紧急撤销对旧会话立即生效 → fail-closed 拒装。
            return McpServerUnavailable(
                reason=f"server 已禁用或撤销（scope={grant.scope}，紧急撤销即刻生效）"
            )
        headers = _headers_from_ref(
            grant.name, snap.secret_ref, self._env, resolved_handles, handle_resolve_failed
        )
        if isinstance(headers, McpServerUnavailable):
            return headers
        return McpServerConfig(
            transport=snap.transport,
            url=snap.url,
            allowed_tools=list(snap.allowed_tools),
            headers=headers,
        )

    async def _load_snapshots(
        self, grants: Sequence[McpGrant]
    ) -> dict[tuple[str, str, int], McpServerRevisionDoc]:
        """按 (scope,name,revision) 批读不可变快照行。"""
        keys = [
            {"scope": g.scope, "name": g.name, "revision": g.revision}
            for g in {(g.scope, g.name, g.revision): g for g in grants}.values()
        ]
        out: dict[tuple[str, str, int], McpServerRevisionDoc] = {}
        cursor = self._revisions.find({"$or": keys})
        async for raw in cursor:
            doc = mcp_server_revisions_doc_adapter.validate_python(
                {key: value for key, value in raw.items() if key != "_id"}
            )
            out[(doc.scope, doc.name, doc.revision)] = doc
        return out

    async def _load_live(
        self, grants: Sequence[McpGrant]
    ) -> dict[tuple[str, str], McpServerDoc]:
        """按 (scope,name) 批读活文档现况（enabled/deleted 支撑 fail-closed）。"""
        keys = [
            {"scope": scope, "name": name}
            for scope, name in {(g.scope, g.name) for g in grants}
        ]
        out: dict[tuple[str, str], McpServerDoc] = {}
        cursor = self._servers.find({"$or": keys})
        async for raw in cursor:
            doc = mcp_servers_doc_adapter.validate_python(
                {key: value for key, value in raw.items() if key != "_id"}
            )
            out[(doc.scope, doc.name)] = doc
        return out

    async def _resolve_handles(
        self,
        grants: Sequence[McpGrant],
        snapshots: Mapping[tuple[str, str, int], McpServerRevisionDoc],
        live: Mapping[tuple[str, str], McpServerDoc],
        namespace: str,
    ) -> tuple[Mapping[str, str], bool]:
        """收集本 run 可装配 grant 的全部 `handle:` 句柄，一次批解（每 run 至多一批）。

        只对通过 config_hash + 活文档现况闸的 grant 收句柄（拒装的 server 不解密其凭据）。
        返回 (handle→明文, 是否失败)。无句柄 → ({}, False)；无出口或批解异常 → ({}, True)。
        """
        handles: set[str] = set()
        for grant in grants:
            snap = snapshots.get((grant.scope, grant.name, grant.revision))
            if snap is None or snap.config_hash != grant.config_hash:
                continue
            status = live.get((grant.scope, grant.name))
            if status is None or status.deleted_at is not None or not status.enabled:
                continue
            if snap.secret_ref is not None:
                handle = _handle_of(snap.secret_ref)
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
        db = client[settings.mongo_db]
        yield McpRegistry(
            db[MCP_SERVER_REVISIONS_COLLECTION],
            db[MCP_SERVERS_COLLECTION],
            env,
            hub_secret_resolver_from_env(env),
        )
    finally:
        await client.close()
