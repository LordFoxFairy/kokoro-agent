"""技能资产物化 reconcile 与装配期中间件（graph state 账本驱动）。

一次 run 装配（before_agent）对本会话技能集做一次对账：
- 账本 {name: content_hash} 从 graph state 读入（checkpoint 态 → resume/跨 worker 认账）。
- 有附件的包才物化；hash 与账本相符且目录在 → 跳过，否则按 hash 取包整包写入 /.skills/<name>/**。
- /.skills 目录缺失（沙箱重建）→ 列举为空 → 账本失效 → 全量重写自愈。
- 会话不含的旧目录 → GC 删除（沙箱档经 aexecute 相对 rm；无 exec 能力则跳过删除）。
- 单包失败（取包抛错/上传抛错）不阻断整体：该技能不落账本（工具侧据此标记不可用），其余照常。

对账产出的新账本整体落回 state（LastValue 覆盖），供同 run 的 skill 工具读取与下 run 认账。
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from pathlib import PurePosixPath
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langgraph.runtime import Runtime

from kokoro_agent.contract import SkillGrant
from kokoro_agent.skills.hub import SkillHub, SkillHubError
from kokoro_agent.skills.supply import SKILLS_ROOT, ExecCapableBackend, MaterializeBackend
from kokoro_agent.state import SKILLS_MATERIALIZED_STATE_KEY, KokoroAgentState

LOGGER = logging.getLogger("kokoro_agent.skills.materialize")

# 与 hub 入库校验同形（GC 删除前二次确认目标名安全，杜绝相对 rm 越界/注入）。
_SAFE_SKILL_NAME = re.compile(r"^[a-z][a-z0-9-]{1,63}$")


async def _list_skill_dirs(backend: MaterializeBackend, skills_root: str) -> set[str]:
    """列举 /.skills 下的技能目录名；目录缺失/异常一律视为空集（触发全量自愈）。"""
    root = skills_root.rstrip("/") or "/"
    try:
        result = await backend.als(root)
    except Exception:  # noqa: BLE001 — backend 探目录异常＝目录不可信＝当空集自愈
        LOGGER.warning("skills dir listing failed at %s; treating as empty", root, exc_info=True)
        return set()
    if result.error is not None or result.entries is None:
        return set()
    return {
        PurePosixPath(entry["path"]).name
        for entry in result.entries
        if entry.get("is_dir")
    }


async def _delete_skill_dir(backend: MaterializeBackend, skills_root: str, name: str) -> None:
    """GC 删除单个残留目录：仅沙箱档（有 aexecute）执行相对 rm，落点=工作区根/.skills/<name>。"""
    if not _SAFE_SKILL_NAME.fullmatch(name):
        return  # 非法名不碰（防越界；正常物化名都通过 hub 校验）
    if not isinstance(backend, ExecCapableBackend):
        return  # 无 shell 能力档（如 BYO 纯文件 backend）：GC 尽力而为，跳过删除
    rel = f".{skills_root.rstrip('/')}/{name}"  # "/.skills" → "./.skills/<name>"
    try:
        await backend.aexecute(f"rm -rf -- {rel}")
    except Exception:  # noqa: BLE001 — GC 是清洁动作,失败不阻断对账
        LOGGER.warning("skills GC failed for %r", name, exc_info=True)


async def _upload_package(
    backend: MaterializeBackend, skills_root: str, name: str, files: Mapping[str, str]
) -> None:
    """整包写入沙箱 /.skills/<name>/**（含 SKILL.md，保内相对引用完整）。"""
    payload = [
        (f"{skills_root}{name}/{rel}", files[rel].encode("utf-8")) for rel in sorted(files)
    ]
    await backend.aupload_files(payload)


async def reconcile_skill_assets(
    *,
    ledger: Mapping[str, str],
    grants: Sequence[SkillGrant],
    hub: SkillHub,
    backend: MaterializeBackend,
    skills_root: str = SKILLS_ROOT,
) -> dict[str, str]:
    """对账并返回新账本 {name: content_hash}（仅含成功物化的附件包）。"""
    granted_names = {grant.name for grant in grants}
    present = await _list_skill_dirs(backend, skills_root)

    # GC：沙箱残留但本会话已不含的目录（沙箱重建后 present 为空则无残留可删）。
    for stale in present - granted_names:
        await _delete_skill_dir(backend, skills_root, stale)

    new_ledger: dict[str, str] = {}
    for grant in grants:
        try:
            # 取包按快照卡的 scope 定死归属（同名跨 scope 不错位）。
            files = await hub.load_package_if_assets(grant.scope, grant.name, grant.content_hash)
        except SkillHubError:
            LOGGER.warning("skill %r package unavailable; skipped", grant.name, exc_info=True)
            continue  # 取包失败：不落账本 → 工具标记不可用；不阻断其余
        if files is None:
            continue  # 纯知识包（无附件），无需物化
        if ledger.get(grant.name) == grant.content_hash and grant.name in present:
            new_ledger[grant.name] = grant.content_hash  # hash 未变且目录在：跳过写入
            continue
        try:
            await _upload_package(backend, skills_root, grant.name, files)
        except Exception:  # noqa: BLE001 — 单包上传失败不阻断其余技能
            LOGGER.warning("skill %r upload failed; marked unavailable", grant.name, exc_info=True)
            continue
        new_ledger[grant.name] = grant.content_hash
    return new_ledger


class SkillMaterializerMiddleware(AgentMiddleware[KokoroAgentState, Any]):
    """装配期物化对账：before_agent 一次，读 state 账本 → reconcile → 新账本落回 state。"""

    state_schema = KokoroAgentState

    def __init__(
        self,
        *,
        grants: Sequence[SkillGrant],
        hub: SkillHub,
        backend: MaterializeBackend,
        skills_root: str = SKILLS_ROOT,
    ) -> None:
        super().__init__()
        self._grants = tuple(grants)
        self._hub = hub
        self._backend = backend
        self._skills_root = skills_root

    async def abefore_agent(
        self, state: KokoroAgentState, runtime: Runtime[Any]
    ) -> dict[str, Any] | None:
        new_ledger = await reconcile_skill_assets(
            ledger=state.get("skills_materialized") or {},
            grants=self._grants,
            hub=self._hub,
            backend=self._backend,
            skills_root=self._skills_root,
        )
        return {SKILLS_MATERIALIZED_STATE_KEY: new_ledger}
