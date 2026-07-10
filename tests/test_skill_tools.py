"""skill 工具与清单规格（真件：真文件 → seed → 真 Mongo + 包体存储驱动）。"""

# BaseTool.ainvoke 上游注解含未解泛型（langchain-core 边界，e2e/test_mcp_live 同款豁免）。
# pyright: reportUnknownMemberType=false

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from deepagents.backends.protocol import FileUploadResponse
from pydantic import BaseModel
from pymongo import AsyncMongoClient

from kokoro_agent.agents.assembly.prompt import render_skill_manifest
from kokoro_agent.contract import SkillGrant
from kokoro_agent.skills import SKILLS_ROOT
from kokoro_agent.skills.hub import LocalPackageStore, SkillHub, seed_official
from kokoro_agent.tools.skills import make_skill_tool
from test_skill_hub import PDF_MD, STYLE_MD, scan, write_skill_dir

_MONGO_URL = "mongodb://127.0.0.1:27017"
SCOPES = ("ns1", "official")


async def grant_for(hub: SkillHub, name: str) -> SkillGrant:
    """会话快照那一刻的授权卡（session 侧消费 list_pool 产出的同形）。"""
    card = (await hub.resolve_cards(SCOPES, [name]))[0]
    return SkillGrant(name=name, content_hash=card.content_hash, description=card.description)


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


class _RecordingBackend:
    def __init__(self) -> None:
        self.uploads: list[list[tuple[str, bytes]]] = []

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        self.uploads.append(files)
        return []


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
    tool = make_skill_tool([await grant_for(hub, "style")], SCOPES, hub, None)
    assert "error" in await tool.ainvoke({"name": "pdf"})  # 库里有但本 run 未授权。
    assert "error" in await tool.ainvoke({"name": "ghost"})  # 不存在。


async def test_skill_plain_package_returns_body_without_upload(hub: SkillHub) -> None:
    backend = _RecordingBackend()
    tool = make_skill_tool([await grant_for(hub, "style")], SCOPES, hub, backend)
    body = await tool.ainvoke({"name": "style"})
    assert "先结论后论据" in body
    assert backend.uploads == []  # 纯知识包零物化。


async def test_skill_asset_package_supplies_whole_package_idempotently(hub: SkillHub) -> None:
    backend = _RecordingBackend()
    tool = make_skill_tool([await grant_for(hub, "pdf")], SCOPES, hub, backend)
    first = await tool.ainvoke({"name": "pdf"})
    assert f"{SKILLS_ROOT}pdf/make_report.py" in first  # 附件路径告知模型。
    assert [p for p, _ in backend.uploads[0]] == [
        f"{SKILLS_ROOT}pdf/SKILL.md",
        f"{SKILLS_ROOT}pdf/make_report.py",
    ]  # 整包供给，保持包内相对引用。
    await tool.ainvoke({"name": "pdf"})
    assert len(backend.uploads) == 1  # run 内幂等。


async def test_skill_asset_without_backend_degrades_explicitly(hub: SkillHub) -> None:
    tool = make_skill_tool([await grant_for(hub, "pdf")], SCOPES, hub, None)
    assert "无法执行技能资产" in await tool.ainvoke({"name": "pdf"})


async def test_skill_tool_reads_snapshot_hash_after_official_upgrade(
    hub: SkillHub, tmp_path: Path
) -> None:
    # 会话快照旧 hash 生效：官方升级后，工具按 v1 grant 读回 v1 正文与 v1 附件（内容锁走工具层）。
    v1_grant = await grant_for(hub, "pdf")  # fixture 已 seed v1（make_report.py=print('report')）。
    v2 = tmp_path / "v2"
    write_skill_dir(
        v2, "pdf", PDF_MD.replace("处理数据", "处理数据（v2 流程）"),
        extra={"make_report.py": "print('v2')"},
    )
    await seed_official(hub, scan(v2))  # 官方升级。
    new_hash = (await hub.resolve_cards(SCOPES, ["pdf"]))[0].content_hash
    assert new_hash != v1_grant.content_hash

    backend = _RecordingBackend()
    tool = make_skill_tool([v1_grant], SCOPES, hub, backend)
    body = await tool.ainvoke({"name": "pdf"})
    assert "（v2 流程）" not in body  # 旧正文（快照锁定）。
    uploaded = {path: data for path, data in backend.uploads[0]}
    assert uploaded[f"{SKILLS_ROOT}pdf/make_report.py"] == b"print('report')"  # 旧附件（v1）。


async def test_skill_pool_change_keeps_tool_schema_identical(hub: SkillHub) -> None:
    # D9：池 A/B/空 切换，工具 schema 逐字节相同（清单在 prompt 侧且随会话快照恒定）。
    def surface(grants: list[SkillGrant]) -> tuple[str, str, str]:
        tool = make_skill_tool(grants, SCOPES, hub, None)
        schema = tool.args_schema
        assert isinstance(schema, type) and issubclass(schema, BaseModel)
        return (tool.name, tool.description, str(schema.model_json_schema()))

    def g(name: str) -> SkillGrant:
        return SkillGrant(name=name, content_hash="h", description="d")

    assert surface([g("style")]) == surface([g("pdf"), g("style")]) == surface([])
