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
from kokoro_agent.skills import SKILLS_ROOT
from kokoro_agent.skills.hub import LocalPackageStore, SkillHub, seed_official
from kokoro_agent.tools.skills import make_skill_tool
from test_skill_hub import PDF_MD, STYLE_MD, scan, write_skill_dir

_MONGO_URL = "mongodb://127.0.0.1:27017"
SCOPES = ("ns1", "official")


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


async def test_manifest_renders_granted_in_order(hub: SkillHub) -> None:
    cards = await hub.resolve_cards(SCOPES, ["pdf", "style"])
    rendered = render_skill_manifest("base", cards)
    assert rendered.index("pdf") < rendered.index("style")  # 授权序=清单序（prompt 字节稳定）。
    assert "写作风格指南" in rendered


def test_manifest_empty_pool_keeps_base_untouched() -> None:
    assert render_skill_manifest("base prompt", []) == "base prompt"


async def test_manifest_same_input_identical_bytes(hub: SkillHub) -> None:
    cards = await hub.resolve_cards(SCOPES, ["style", "pdf"])
    assert render_skill_manifest("base", cards) == render_skill_manifest("base", list(cards))


# --- skill(name) 单工具 ---


async def test_skill_fails_closed_outside_run_pool(hub: SkillHub) -> None:
    tool = make_skill_tool(["style"], SCOPES, hub, None)
    assert "error" in await tool.ainvoke({"name": "pdf"})  # 库里有但本 run 未授权。
    assert "error" in await tool.ainvoke({"name": "ghost"})  # 不存在。


async def test_skill_plain_package_returns_body_without_upload(hub: SkillHub) -> None:
    backend = _RecordingBackend()
    tool = make_skill_tool(["style"], SCOPES, hub, backend)
    body = await tool.ainvoke({"name": "style"})
    assert "先结论后论据" in body
    assert backend.uploads == []  # 纯知识包零物化。


async def test_skill_asset_package_supplies_whole_package_idempotently(hub: SkillHub) -> None:
    backend = _RecordingBackend()
    tool = make_skill_tool(["pdf"], SCOPES, hub, backend)
    first = await tool.ainvoke({"name": "pdf"})
    assert f"{SKILLS_ROOT}pdf/make_report.py" in first  # 附件路径告知模型。
    assert [p for p, _ in backend.uploads[0]] == [
        f"{SKILLS_ROOT}pdf/SKILL.md",
        f"{SKILLS_ROOT}pdf/make_report.py",
    ]  # 整包供给，保持包内相对引用。
    await tool.ainvoke({"name": "pdf"})
    assert len(backend.uploads) == 1  # run 内幂等。


async def test_skill_asset_without_backend_degrades_explicitly(hub: SkillHub) -> None:
    tool = make_skill_tool(["pdf"], SCOPES, hub, None)
    assert "无法执行技能资产" in await tool.ainvoke({"name": "pdf"})


async def test_skill_pool_change_keeps_tool_schema_identical(hub: SkillHub) -> None:
    # D9：池 A/B/空 切换，工具 schema 逐字节相同（清单在 prompt 侧且随会话快照恒定）。
    def surface(names: list[str]) -> tuple[str, str, str]:
        tool = make_skill_tool(names, SCOPES, hub, None)
        schema = tool.args_schema
        assert isinstance(schema, type) and issubclass(schema, BaseModel)
        return (tool.name, tool.description, str(schema.model_json_schema()))

    assert surface(["style"]) == surface(["pdf", "style"]) == surface([])
