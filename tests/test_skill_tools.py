"""Skill 工具与清单规格：run-scoped immutable Hub assembly 驱动。

工具不上传附件；物化归装配期 reconcile 中间件。
"""

# BaseTool.ainvoke 上游注解含未解泛型（langchain-core 边界，e2e/test_mcp_live 同款豁免）。
# pyright: reportUnknownMemberType=false

from __future__ import annotations

import pytest
from langchain.tools import ToolRuntime
from langchain_core.tools import StructuredTool
from pydantic import BaseModel

from kokoro_agent.agents.assembly.prompt import render_skill_manifest
from kokoro_agent.contract import SkillGrant
from kokoro_agent.skills import SKILLS_ROOT
from kokoro_agent.skills.hub import SkillHub
from kokoro_agent.state import KokoroAgentState
from kokoro_agent.tools.skills import make_skill_tool
from skill_fixtures import PDF_FILES, STYLE_FILES, make_skill_hub, snapshot_grant

# 与 hub fixture seed 的包内容一致（池查询权威在 kokoro-hub，测试按已知内容构快照卡）。
SEED = {"style": STYLE_FILES, "pdf": PDF_FILES}


def grant_for(name: str, scope: str = "official") -> SkillGrant:
    """会话快照那一刻的授权卡（session 侧从 kokoro-hub 池产出的同形，scope 定死归属）。"""
    return snapshot_grant(SEED[name], name, scope)


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
def hub() -> SkillHub:
    return make_skill_hub(
        ("official", "style", STYLE_FILES),
        ("official", "pdf", PDF_FILES),
    )


# --- 清单（发现=阅读，无检索）---


def test_manifest_renders_granted_in_order() -> None:
    # 零查询：清单直接渲染会话快照 grants（wire 序=清单序）。
    grants = [
        SkillGrant(option_ref="skill:pdf", name="pdf", content_hash="h-pdf", description="PDF 报告生成流程", scope="official"),
        SkillGrant(option_ref="skill:style", name="style", content_hash="h-style", description="写作风格指南", scope="official"),
    ]
    rendered = render_skill_manifest("base", grants)
    assert rendered.index("pdf") < rendered.index("style")  # 授权序=清单序（prompt 字节稳定）。
    assert "写作风格指南" in rendered


def test_manifest_empty_pool_keeps_base_untouched() -> None:
    assert render_skill_manifest("base prompt", []) == "base prompt"


def test_manifest_same_grants_identical_bytes() -> None:
    grants = [
        SkillGrant(option_ref="skill:style", name="style", content_hash="h-style", description="写作风格指南", scope="official"),
        SkillGrant(option_ref="skill:pdf", name="pdf", content_hash="h-pdf", description="PDF 报告生成流程", scope="official"),
    ]
    assert render_skill_manifest("base", grants) == render_skill_manifest("base", list(grants))


# --- skill(name) 单工具 ---


async def test_skill_fails_closed_outside_run_pool(hub: SkillHub) -> None:
    tool = make_skill_tool([grant_for("style")], hub)
    assert "error" in await _read(tool, "pdf", {})  # 库里有但本 run 未授权。
    assert "error" in await _read(tool, "ghost", {})  # 不存在。


async def test_skill_plain_package_returns_body(hub: SkillHub) -> None:
    tool = make_skill_tool([grant_for("style")], hub)
    body = await _read(tool, "style", {})
    assert "先结论后论据" in body  # 纯知识包：正文即全部，不涉物化账本。


async def test_skill_asset_ready_when_ledger_has_hash(hub: SkillHub) -> None:
    grant = grant_for("pdf")
    tool = make_skill_tool([grant], hub)
    # 账本记 pdf→快照 hash（reconcile 已物化）→ 工具告知附件就绪与路径。
    result = await _read(tool, "pdf", {"pdf": grant.content_hash})
    assert f"{SKILLS_ROOT}pdf/make_report.py" in result
    assert "已就绪" in result


async def test_skill_asset_unavailable_when_not_in_ledger(hub: SkillHub) -> None:
    grant = grant_for("pdf")
    tool = make_skill_tool([grant], hub)
    # 账本无 pdf（未物化/物化失败）→ 正文可用但标记附件不可用（不谎报就绪）。
    result = await _read(tool, "pdf", {})
    assert "不可用" in result
    assert "已就绪" not in result  # 不出现就绪清单块（路径本身在正文里天然出现，故只查就绪标记）


async def test_skill_tool_reads_exact_snapshot_when_multiple_hashes_are_cached() -> None:
    v1_grant = grant_for("pdf")
    v2_files = {
        "SKILL.md": PDF_FILES["SKILL.md"].replace("处理数据", "处理数据（v2 流程）"),
        "make_report.py": "print('v2')",
    }
    new_hash = snapshot_grant(v2_files, "pdf").content_hash
    assert new_hash != v1_grant.content_hash
    hub = make_skill_hub(
        ("official", "pdf", PDF_FILES),
        ("official", "pdf", v2_files),
    )
    tool = make_skill_tool([v1_grant], hub)
    body = await _read(tool, "pdf", {"pdf": v1_grant.content_hash})
    assert "（v2 流程）" not in body  # 旧正文（快照锁定），不随官方升级漂移。


async def test_skill_pool_change_keeps_tool_schema_identical(hub: SkillHub) -> None:
    # D9：池 A/B/空 切换，工具 schema 逐字节相同（清单在 prompt 侧且随会话快照恒定）。
    def surface(grants: list[SkillGrant]) -> tuple[str, str, str]:
        tool = make_skill_tool(grants, hub)
        schema = tool.args_schema
        assert isinstance(schema, type) and issubclass(schema, BaseModel)
        return (tool.name, tool.description, str(schema.model_json_schema()))

    def g(name: str) -> SkillGrant:
        return SkillGrant(option_ref=f"skill:{name}", name=name, content_hash="h", description="d", scope="official")

    assert surface([g("style")]) == surface([g("pdf"), g("style")]) == surface([])
