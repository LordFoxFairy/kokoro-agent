"""技能库工具规格（渐进披露）：find 过滤 / read 直返 / 资产按需幂等供给 / 前缀恒定。"""

# BaseTool.ainvoke 上游注解含未解泛型（langchain-core 边界，e2e/test_mcp_live 同款豁免）。
# pyright: reportUnknownMemberType=false

from __future__ import annotations

from deepagents.backends.protocol import FileUploadResponse
from pydantic import BaseModel

from kokoro_agent.skills import SKILLS_ROOT, SkillLibrary, SkillPackage
from kokoro_agent.tools.skills import make_skill_tools


def _library() -> SkillLibrary:
    return SkillLibrary({
        "style": SkillPackage(
            name="style", description="写作风格指南",
            files={"SKILL.md": "---\nname: style\ndescription: 写作风格指南\n---\n正文A"},
        ),
        "pdf": SkillPackage(
            name="pdf", description="PDF 处理流程",
            files={
                "SKILL.md": "---\nname: pdf\ndescription: PDF 处理流程\n---\n用 tool.py 处理",
                "tool.py": "print('pdf')",
            },
        ),
        "tone": SkillPackage(
            name="tone", description="语气调整",
            files={"SKILL.md": "---\nname: tone\ndescription: 语气调整\n---\n正文B"},
        ),
    })


class _RecordingBackend:
    def __init__(self) -> None:
        self.uploads: list[list[tuple[str, bytes]]] = []

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        self.uploads.append(files)
        return []


async def test_find_lists_only_granted_and_filters_by_query() -> None:
    find, _ = make_skill_tools(["style", "pdf"], _library(), None)
    all_cards = await find.ainvoke({"query": ""})
    assert "style" in all_cards and "pdf" in all_cards
    assert "tone" not in all_cards  # 库里有但本 run 未授权：find 面不可见。
    filtered = await find.ainvoke({"query": "PDF"})
    assert "pdf" in filtered and "style" not in filtered
    assert "没有匹配" in await find.ainvoke({"query": "ghost"})


async def test_find_empty_pool_says_so() -> None:
    find, _ = make_skill_tools([], _library(), None)
    assert "没有可用技能" in await find.ainvoke({"query": ""})


async def test_read_fails_closed_outside_run_pool() -> None:
    _, read = make_skill_tools(["style"], _library(), None)
    assert "error" in await read.ainvoke({"name": "tone"})  # 未授权
    assert "error" in await read.ainvoke({"name": "ghost"})  # 不存在


async def test_read_plain_skill_returns_body_without_upload() -> None:
    backend = _RecordingBackend()
    _, read = make_skill_tools(["style"], _library(), backend)
    body = await read.ainvoke({"name": "style"})
    assert "正文A" in body
    assert backend.uploads == []  # 纯文档包：零物化。


async def test_read_asset_skill_supplies_whole_package_idempotently() -> None:
    backend = _RecordingBackend()
    _, read = make_skill_tools(["pdf"], _library(), backend)
    first = await read.ainvoke({"name": "pdf"})
    assert f"{SKILLS_ROOT}pdf/tool.py" in first  # 资产路径告知模型。
    assert len(backend.uploads) == 1
    assert [p for p, _ in backend.uploads[0]] == [
        f"{SKILLS_ROOT}pdf/SKILL.md",
        f"{SKILLS_ROOT}pdf/tool.py",
    ]  # 整包供给，保持包内相对引用。
    await read.ainvoke({"name": "pdf"})
    assert len(backend.uploads) == 1  # run 内幂等：不重传。


async def test_read_asset_skill_without_backend_degrades_explicitly() -> None:
    _, read = make_skill_tools(["pdf"], _library(), None)
    body = await read.ainvoke({"name": "pdf"})
    assert "无法执行技能资产" in body  # state 档显式降级，不静默。


def test_skill_pool_change_keeps_tool_surface_identical() -> None:
    # 块3 核心断言：skill 池 A/B 切换，工具面（name/description/schema）逐字节相同——前缀恒定。
    def surface(names: list[str]) -> list[tuple[str, str, str]]:
        out: list[tuple[str, str, str]] = []
        for tool in make_skill_tools(names, _library(), None):
            schema = tool.args_schema
            assert isinstance(schema, type) and issubclass(schema, BaseModel)
            out.append((tool.name, tool.description, str(schema.model_json_schema())))
        return out

    assert surface(["style"]) == surface(["pdf", "tone"]) == surface([])
