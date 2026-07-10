"""skill 工具与清单规格（真件：真文件 → seed → 真 Mongo + 包体存储驱动）。

工具不再上传附件（物化归装配期 reconcile 中间件，见 test_skill_materialize）；本文件覆盖
清单渲染、授权 fail-closed、正文双路（含内容锁）、以及工具按 graph state 账本引导附件就绪与否。
"""

# BaseTool.ainvoke 上游注解含未解泛型（langchain-core 边界，e2e/test_mcp_live 同款豁免）。
# pyright: reportUnknownMemberType=false

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from langchain.tools import ToolRuntime
from langchain_core.tools import StructuredTool
from pydantic import BaseModel
from pymongo import AsyncMongoClient

from kokoro_agent.agents.assembly.prompt import render_skill_manifest
from kokoro_agent.contract import SkillGrant
from kokoro_agent.skills import SKILLS_ROOT
from kokoro_agent.skills.hub import LocalPackageStore, SkillHub, seed_official
from kokoro_agent.state import KokoroAgentState
from kokoro_agent.tools.skills import make_skill_tool
from test_skill_hub import PDF_MD, STYLE_MD, scan, write_skill_dir

_MONGO_URL = "mongodb://127.0.0.1:27017"
SCOPES = ("ns1", "official")


async def grant_for(hub: SkillHub, name: str) -> SkillGrant:
    """会话快照那一刻的授权卡（session 侧消费 list_pool 产出的同形）。"""
    card = (await hub.resolve_cards(SCOPES, [name]))[0]
    return SkillGrant(name=name, content_hash=card.content_hash, description=card.description)


async def _read(tool: StructuredTool, name: str, ledger: dict[str, str]) -> str:
    """直呼工具协程并注入带账本的 runtime（standalone ainvoke 不注入 graph state）。"""
    coroutine = tool.coroutine
    assert coroutine is not None
    return await coroutine(name, _runtime(ledger))


def _runtime(ledger: dict[str, str]) -> ToolRuntime[None, KokoroAgentState]:
    """构造带账本的 ToolRuntime（工具经 runtime.state 读物化账本）。"""
    state: KokoroAgentState = {"messages": [], "scope": {}, "skills_materialized": ledger}
    return ToolRuntime(
        state=state,
        context=None,
        config={},
        stream_writer=lambda _chunk: None,
        tool_call_id="call-1",
        store=None,
    )


@pytest.fixture
async def hub(tmp_path: Path) -> AsyncGenerator[SkillHub, None]:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(_MONGO_URL)
    suffix = uuid.uuid4().hex[:8]
    database = client["kokoro_test"]
    skills = database[f"skills_{suffix}"]
    state = database[f"skill_state_{suffix}"]
    src = tmp_path / "seed"
    write_skill_dir(src, "style", STYLE_MD)
    write_skill_dir(src, "pdf", PDF_MD, extra={"make_report.py": "print('report')"})
    instance = SkillHub(skills, state, LocalPackageStore(str(tmp_path / "packages")))
    await seed_official(instance, scan(src))
    try:
        yield instance
    finally:
        await skills.drop()
        await state.drop()
        await client.close()


# --- 清单（发现=阅读，无检索）---


def test_manifest_renders_granted_in_order() -> None:
    # 零查询：清单直接渲染会话快照 grants（wire 序=清单序）。
    grants = [
        SkillGrant(name="pdf", content_hash="h-pdf", description="PDF 报告生成流程"),
        SkillGrant(name="style", content_hash="h-style", description="写作风格指南"),
    ]
    rendered = render_skill_manifest("base", grants)
    assert rendered.index("pdf") < rendered.index("style")  # 授权序=清单序（prompt 字节稳定）。
    assert "写作风格指南" in rendered


def test_manifest_empty_pool_keeps_base_untouched() -> None:
    assert render_skill_manifest("base prompt", []) == "base prompt"


def test_manifest_same_grants_identical_bytes() -> None:
    grants = [
        SkillGrant(name="style", content_hash="h-style", description="写作风格指南"),
        SkillGrant(name="pdf", content_hash="h-pdf", description="PDF 报告生成流程"),
    ]
    assert render_skill_manifest("base", grants) == render_skill_manifest("base", list(grants))


# --- skill(name) 单工具 ---


async def test_skill_fails_closed_outside_run_pool(hub: SkillHub) -> None:
    tool = make_skill_tool([await grant_for(hub, "style")], SCOPES, hub)
    assert "error" in await _read(tool, "pdf", {})  # 库里有但本 run 未授权。
    assert "error" in await _read(tool, "ghost", {})  # 不存在。


async def test_skill_plain_package_returns_body(hub: SkillHub) -> None:
    tool = make_skill_tool([await grant_for(hub, "style")], SCOPES, hub)
    body = await _read(tool, "style", {})
    assert "先结论后论据" in body  # 纯知识包：正文即全部，不涉物化账本。


async def test_skill_asset_ready_when_ledger_has_hash(hub: SkillHub) -> None:
    grant = await grant_for(hub, "pdf")
    tool = make_skill_tool([grant], SCOPES, hub)
    # 账本记 pdf→快照 hash（reconcile 已物化）→ 工具告知附件就绪与路径。
    result = await _read(tool, "pdf", {"pdf": grant.content_hash})
    assert f"{SKILLS_ROOT}pdf/make_report.py" in result
    assert "已就绪" in result


async def test_skill_asset_unavailable_when_not_in_ledger(hub: SkillHub) -> None:
    grant = await grant_for(hub, "pdf")
    tool = make_skill_tool([grant], SCOPES, hub)
    # 账本无 pdf（未物化/物化失败）→ 正文可用但标记附件不可用（不谎报就绪）。
    result = await _read(tool, "pdf", {})
    assert "不可用" in result
    assert "已就绪" not in result  # 不出现就绪清单块（路径本身在正文里天然出现，故只查就绪标记）


async def test_skill_tool_reads_snapshot_hash_after_official_upgrade(
    hub: SkillHub, tmp_path: Path
) -> None:
    # 会话快照旧 hash 生效：官方升级后，工具按 v1 grant 读回 v1 正文（内容锁走工具层，正文双路）。
    v1_grant = await grant_for(hub, "pdf")
    v2 = tmp_path / "v2"
    write_skill_dir(
        v2, "pdf", PDF_MD.replace("处理数据", "处理数据（v2 流程）"),
        extra={"make_report.py": "print('v2')"},
    )
    await seed_official(hub, scan(v2))  # 官方升级。
    new_hash = (await hub.resolve_cards(SCOPES, ["pdf"]))[0].content_hash
    assert new_hash != v1_grant.content_hash

    tool = make_skill_tool([v1_grant], SCOPES, hub)
    body = await _read(tool, "pdf", {"pdf": v1_grant.content_hash})
    assert "（v2 流程）" not in body  # 旧正文（快照锁定），不随官方升级漂移。


async def test_skill_pool_change_keeps_tool_schema_identical(hub: SkillHub) -> None:
    # D9：池 A/B/空 切换，工具 schema 逐字节相同（清单在 prompt 侧且随会话快照恒定）。
    def surface(grants: list[SkillGrant]) -> tuple[str, str, str]:
        tool = make_skill_tool(grants, SCOPES, hub)
        schema = tool.args_schema
        assert isinstance(schema, type) and issubclass(schema, BaseModel)
        return (tool.name, tool.description, str(schema.model_json_schema()))

    def g(name: str) -> SkillGrant:
        return SkillGrant(name=name, content_hash="h", description="d")

    assert surface([g("style")]) == surface([g("pdf"), g("style")]) == surface([])
