"""技能资产物化 reconcile 与账本规格：真 Mongo hub + 真 LocalShellBackend（磁盘落盘）。

账本 = graph state（checkpoint 态），非闭包局部变量：resume/新实例/跨 worker 认账。
覆盖：纯包不物化、增量物化、hash 未变跳过、hash 变更重写、缺目录自愈、GC、单包失败不阻断、
以及跨 checkpoint 的 resume 认账（新起 graph + 新中间件实例，同 thread 从 checkpoint 恢复）。
"""

from __future__ import annotations

import shutil
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from typing import Any

from deepagents.backends.local_shell import LocalShellBackend
from deepagents.backends.protocol import FileUploadResponse
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.base import BaseCheckpointSaver
from pymongo import AsyncMongoClient

from fakes import usage_recorder
from kokoro_agent.contract import SkillGrant
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.skills import SkillMaterializerMiddleware, reconcile_skill_assets
from kokoro_agent.skills.hub import LocalPackageStore, SkillHub, seed_official
from kokoro_agent.state import RunScope
from kokoro_agent.streams.redis import RedisStream
from test_skill_hub import PDF_MD, STYLE_MD, scan, write_skill_dir

_MONGO_URL = "mongodb://127.0.0.1:27017"


class _SpyBackend(LocalShellBackend):
    """真 LocalShellBackend（磁盘落盘）+ 上传间谍：断言真实上传次数与载荷路径，非假存储。"""

    def __init__(self, root: Path) -> None:
        super().__init__(root_dir=str(root), virtual_mode=True, timeout=30, max_output_bytes=100_000)
        self.uploads: list[list[str]] = []

    async def aupload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        self.uploads.append([path for path, _ in files])
        return await super().aupload_files(files)


@pytest.fixture
async def hub(tmp_path: Path) -> AsyncGenerator[SkillHub, None]:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(_MONGO_URL)
    suffix = uuid.uuid4().hex[:8]
    database = client["kokoro_test"]
    skills = database[f"skills_{suffix}"]
    state = database[f"skill_state_{suffix}"]
    src = tmp_path / "seed"
    write_skill_dir(src, "style", STYLE_MD)  # 纯知识包（无附件）
    write_skill_dir(src, "pdf", PDF_MD, extra={"make_report.py": "print('report')"})  # 附件包
    instance = SkillHub(skills, state, LocalPackageStore(str(tmp_path / "packages")))
    await seed_official(instance, scan(src))
    try:
        yield instance
    finally:
        await skills.drop()
        await state.drop()
        await client.close()


async def grant_for(hub: SkillHub, name: str, scope: str = "official") -> SkillGrant:
    card = (await hub.resolve_cards([scope], [name]))[0]
    return SkillGrant(name=name, content_hash=card.content_hash, description=card.description, scope=scope)


def _skill_file(root: Path, name: str, rel: str) -> Path:
    return root / ".skills" / name / rel


# --- reconcile 语义 ---


async def test_plain_package_not_materialized(hub: SkillHub, tmp_path: Path) -> None:
    backend = _SpyBackend(tmp_path / "ws")
    ledger = await reconcile_skill_assets(
        ledger={}, grants=[await grant_for(hub, "style")], hub=hub, backend=backend
    )
    assert ledger == {}  # 纯包不进账本
    assert backend.uploads == []  # 零物化


async def test_asset_package_materialized_and_recorded(hub: SkillHub, tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    backend = _SpyBackend(ws)
    grant = await grant_for(hub, "pdf")
    ledger = await reconcile_skill_assets(
        ledger={}, grants=[grant], hub=hub, backend=backend
    )
    assert ledger == {"pdf": grant.content_hash}  # 账本记 name→hash
    assert len(backend.uploads) == 1
    assert _skill_file(ws, "pdf", "SKILL.md") in [Path(p) for p in _resolved(ws, backend.uploads[0])]
    assert _skill_file(ws, "pdf", "make_report.py").read_text() == "print('report')"  # 真落盘


def _resolved(ws: Path, virtual_paths: list[str]) -> list[str]:
    # 虚拟根映射：/.skills/... → {ws}/.skills/...
    return [str(ws / path.lstrip("/")) for path in virtual_paths]


async def test_unchanged_hash_skips_second_reconcile(hub: SkillHub, tmp_path: Path) -> None:
    backend = _SpyBackend(tmp_path / "ws")
    grant = await grant_for(hub, "pdf")
    first = await reconcile_skill_assets(
        ledger={}, grants=[grant], hub=hub, backend=backend
    )
    # 第二次对账：账本相符 + 目录在 → 跳过上传（间谍计数不增）。
    second = await reconcile_skill_assets(
        ledger=first, grants=[grant], hub=hub, backend=backend
    )
    assert second == first
    assert len(backend.uploads) == 1  # 仅首次上传


async def test_changed_hash_rewrites(hub: SkillHub, tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    backend = _SpyBackend(ws)
    v1 = await grant_for(hub, "pdf")
    ledger = await reconcile_skill_assets(
        ledger={}, grants=[v1], hub=hub, backend=backend
    )
    v2_src = tmp_path / "v2"
    write_skill_dir(v2_src, "pdf", PDF_MD, extra={"make_report.py": "print('v2')"})
    await seed_official(hub, scan(v2_src))  # 官方升级
    v2 = await grant_for(hub, "pdf")
    assert v2.content_hash != v1.content_hash
    ledger = await reconcile_skill_assets(
        ledger=ledger, grants=[v2], hub=hub, backend=backend
    )
    assert ledger == {"pdf": v2.content_hash}  # 账本换 hash
    assert len(backend.uploads) == 2  # hash 变 → 重写
    assert _skill_file(ws, "pdf", "make_report.py").read_text() == "print('v2')"


async def test_missing_dir_forces_full_rewrite(hub: SkillHub, tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    backend = _SpyBackend(ws)
    grant = await grant_for(hub, "pdf")
    ledger = await reconcile_skill_assets(
        ledger={}, grants=[grant], hub=hub, backend=backend
    )
    shutil.rmtree(ws / ".skills")  # 沙箱重建：目录被销毁
    # 账本仍认 pdf，但目录已缺 → 不信账本 → 强制重写自愈。
    ledger = await reconcile_skill_assets(
        ledger=ledger, grants=[grant], hub=hub, backend=backend
    )
    assert ledger == {"pdf": grant.content_hash}
    assert len(backend.uploads) == 2  # 自愈再次上传
    assert _skill_file(ws, "pdf", "make_report.py").exists()  # 目录回来了


async def test_gc_removes_stale_dir(hub: SkillHub, tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    backend = _SpyBackend(ws)
    ledger = await reconcile_skill_assets(
        ledger={}, grants=[await grant_for(hub, "pdf")], hub=hub, backend=backend
    )
    assert _skill_file(ws, "pdf", "SKILL.md").exists()
    # 会话不再含 pdf（改为纯包 style）→ 旧 pdf 目录 GC 删除。
    ledger = await reconcile_skill_assets(
        ledger=ledger, grants=[await grant_for(hub, "style")], hub=hub, backend=backend
    )
    assert ledger == {}
    assert not (ws / ".skills" / "pdf").exists()  # 残留目录被清


async def test_single_package_failure_does_not_block_others(hub: SkillHub, tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    backend = _SpyBackend(ws)
    good = await grant_for(hub, "pdf")
    ghost = SkillGrant(name="ghost", content_hash="deadbeef", description="不存在", scope="official")  # 取包必抛错
    ledger = await reconcile_skill_assets(
        ledger={}, grants=[ghost, good], hub=hub, backend=backend
    )
    assert ledger == {"pdf": good.content_hash}  # 坏包跳过，好包照常
    assert "ghost" not in ledger
    assert _skill_file(ws, "pdf", "make_report.py").exists()


# --- 账本跨 checkpoint resume 认账（全图真跑）---


def _scope(namespace: str = "ns1") -> RunScope:
    return RunScope(namespace=namespace, session_id="s1", run_id="rn", thread_id="s1")


async def _invoke(
    agent: InvokableAgent, payload: object, run_id: str, bus: RedisStream, thread_id: str
) -> None:
    async def claim() -> bool:
        return True

    await invoke_once(
        RunEmitter(bus, run_id),
        agent,
        thread_id,
        payload,
        approval_tool_names=frozenset(),
        source_for=lambda _name: "built-in",
        claim_terminal=claim,
        record_usage=usage_recorder()[0],
    )


async def test_ledger_survives_resume_zero_reupload(
    hub: SkillHub, tmp_path: Path, stream: RedisStream, checkpointer: BaseCheckpointSaver[str]
) -> None:
    # 同一真 backend 目录 + 同一 checkpointer/thread；两次独立装配（新 graph + 新中间件实例）。
    backend = _SpyBackend(tmp_path / "ws")
    grant = await grant_for(hub, "pdf")
    scope = _scope()
    script = [AIMessage(content="done"), AIMessage(content="done2")]

    def assemble() -> InvokableAgent:
        materializer: list[AgentMiddleware[Any, Any, Any]] = [
            SkillMaterializerMiddleware(grants=[grant], hub=hub, backend=backend)
        ]
        return build_agent(
            model=LocalFakeChatModel.with_script(script),
            tools=[],
            system_prompt="x",
            subagents=[],
            checkpointer=checkpointer,
            permissions=[],
            interrupt_on={},
            middleware=materializer,
            backend=backend,
        )

    await _invoke(
        assemble(),
        {"messages": [HumanMessage(content="hi")], "scope": scope.as_state()},
        "rn",
        stream,
        scope.scoped_thread_id,
    )
    assert len(backend.uploads) == 1  # 首装配物化

    # resume：新实例从 checkpoint 恢复账本 {pdf:hash} → hash 未变 + 目录在 → 零重传。
    await _invoke(
        assemble(),
        {"messages": [HumanMessage(content="again")]},
        "rn2",
        stream,
        scope.scoped_thread_id,
    )
    assert len(backend.uploads) == 1  # 账本经 checkpoint 认账，无二次上传
