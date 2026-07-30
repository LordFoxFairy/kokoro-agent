"""技能资产物化 reconcile 与账本规格：immutable assembly + 真 LocalShellBackend。

账本是 checkpoint graph state；覆盖增量、重写、自愈、GC 与 resume 认账。
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from typing import Any

from deepagents.backends.local_shell import LocalShellBackend
from deepagents.backends.protocol import FileUploadResponse
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.base import BaseCheckpointSaver

from fakes import FakeLedger, completed_execution_context, usage_recorder
from kokoro_agent.contract import SkillGrant
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.skills import SkillMaterializerMiddleware, reconcile_skill_assets
from kokoro_agent.skills.hub import SkillHub
from kokoro_agent.state import RunScope
from kokoro_agent.streams.redis import RedisStream
from skill_fixtures import PDF_FILES, STYLE_FILES, make_skill_hub, snapshot_grant


class _SpyBackend(LocalShellBackend):
    """真 LocalShellBackend（磁盘落盘）+ 上传间谍：断言真实上传次数与载荷路径，非假存储。"""

    def __init__(self, root: Path) -> None:
        super().__init__(root_dir=str(root), virtual_mode=True, timeout=30, max_output_bytes=100_000)
        self.uploads: list[list[str]] = []

    async def aupload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        self.uploads.append([path for path, _ in files])
        return await super().aupload_files(files)


@pytest.fixture
def hub() -> SkillHub:
    return make_skill_hub(
        ("official", "style", STYLE_FILES),
        ("official", "pdf", PDF_FILES),
    )


# 与 hub fixture seed 的包内容一致（池查询权威在 kokoro-hub，测试按已知内容构快照卡）。
SEED = {"style": STYLE_FILES, "pdf": PDF_FILES}


def grant_for(name: str, scope: str = "official") -> SkillGrant:
    return snapshot_grant(SEED[name], name, scope)


def _skill_file(root: Path, name: str, rel: str) -> Path:
    return root / ".skills" / name / rel


# --- reconcile 语义 ---


async def test_plain_package_not_materialized(hub: SkillHub, tmp_path: Path) -> None:
    backend = _SpyBackend(tmp_path / "ws")
    ledger = await reconcile_skill_assets(
        ledger={}, grants=[grant_for("style")], hub=hub, backend=backend
    )
    assert ledger == {}  # 纯包不进账本
    assert backend.uploads == []  # 零物化


async def test_asset_package_materialized_and_recorded(hub: SkillHub, tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    backend = _SpyBackend(ws)
    grant = grant_for("pdf")
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
    grant = grant_for("pdf")
    first = await reconcile_skill_assets(
        ledger={}, grants=[grant], hub=hub, backend=backend
    )
    # 第二次对账：账本相符 + 目录在 → 跳过上传（间谍计数不增）。
    second = await reconcile_skill_assets(
        ledger=first, grants=[grant], hub=hub, backend=backend
    )
    assert second == first
    assert len(backend.uploads) == 1  # 仅首次上传


async def test_changed_hash_rewrites(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    backend = _SpyBackend(ws)
    v1 = grant_for("pdf")
    hub = make_skill_hub(("official", "pdf", PDF_FILES))
    ledger = await reconcile_skill_assets(
        ledger={}, grants=[v1], hub=hub, backend=backend
    )
    v2_files = {**PDF_FILES, "make_report.py": "print('v2')"}
    v2 = snapshot_grant(v2_files, "pdf")
    assert v2.content_hash != v1.content_hash
    hub = make_skill_hub(
        ("official", "pdf", PDF_FILES),
        ("official", "pdf", v2_files),
    )
    ledger = await reconcile_skill_assets(
        ledger=ledger, grants=[v2], hub=hub, backend=backend
    )
    assert ledger == {"pdf": v2.content_hash}  # 账本换 hash
    assert len(backend.uploads) == 2  # hash 变 → 重写
    assert _skill_file(ws, "pdf", "make_report.py").read_text() == "print('v2')"


async def test_missing_dir_forces_full_rewrite(hub: SkillHub, tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    backend = _SpyBackend(ws)
    grant = grant_for("pdf")
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
        ledger={}, grants=[grant_for("pdf")], hub=hub, backend=backend
    )
    assert _skill_file(ws, "pdf", "SKILL.md").exists()
    # 会话不再含 pdf（改为纯包 style）→ 旧 pdf 目录 GC 删除。
    ledger = await reconcile_skill_assets(
        ledger=ledger, grants=[grant_for("style")], hub=hub, backend=backend
    )
    assert ledger == {}
    assert not (ws / ".skills" / "pdf").exists()  # 残留目录被清


async def test_single_package_failure_does_not_block_others(hub: SkillHub, tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    backend = _SpyBackend(ws)
    good = grant_for("pdf")
    ghost = SkillGrant(option_ref="skill:ghost", name="ghost", content_hash="deadbeef", description="不存在", scope="official")  # 取包必抛错
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

    ledger = FakeLedger()
    await invoke_once(
        RunEmitter(bus, run_id, outbox=ledger),
        agent,
        {
            "configurable": {"thread_id": thread_id},
            "metadata": {"kokoro_run_id": run_id},
        },
        payload,
        approval_tool_names=frozenset(),
        source_for=lambda _name: "built-in",
        claim_terminal=claim,
        prepare_completed=lambda: completed_execution_context(run_id),
        record_usage=usage_recorder()[0],
    )


async def test_ledger_survives_resume_zero_reupload(
    hub: SkillHub, tmp_path: Path, stream: RedisStream, checkpointer: BaseCheckpointSaver[str]
) -> None:
    # 同一真 backend 目录 + 同一 checkpointer/thread；两次独立装配（新 graph + 新中间件实例）。
    backend = _SpyBackend(tmp_path / "ws")
    grant = grant_for("pdf")
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
