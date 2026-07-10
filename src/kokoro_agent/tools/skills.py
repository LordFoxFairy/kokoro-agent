"""技能库工具（渐进披露）：find_skill 查内存索引，read_skill 从库直返正文。

挂载=逻辑授权（本 run names 集），不是物理搬运：发现/读取零文件系统，
只有"包含执行资产（非 SKILL.md 文件）的包"在被读取时按需幂等上传进沙盒。
两工具恒挂：skill 池 A/B 变化不动 tool schema（前缀稳定铁律）。
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.skills.package import SkillLibrary
from kokoro_agent.skills.supply import SKILLS_ROOT, UploadCapableBackend


class FindSkillArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    query: str = Field(default="", description="关键词过滤（匹配名称或描述）；留空列出全部可用技能。")


class ReadSkillArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(description="要学习的技能名（来自 find_skill 的结果）。")


def make_skill_tools(
    skill_names: Sequence[str],
    library: SkillLibrary,
    backend: UploadCapableBackend | None,
) -> tuple[StructuredTool, StructuredTool]:
    """per-run 闭包工具（make_memory_tools 同款模式）：授权集/库/后端在装配期捕获。"""

    # 本 run 授权池（保序去重）；库里不存在的名字在 read 时 fail-loud 提示。
    granted: tuple[str, ...] = tuple(dict.fromkeys(skill_names))
    supplied: set[str] = set()  # run 内幂等：同包资产只上传一次。

    async def find_skill(query: str = "") -> str:
        needle = query.strip().lower()
        cards: list[str] = []
        for name in granted:
            if name not in library.names():
                continue  # 部署快照里已不存在的名字：find 面直接不可见。
            package = library.get(name)
            if needle and needle not in name.lower() and needle not in package.description.lower():
                continue
            cards.append(f"{name} — {package.description}")
        if not cards:
            return "没有匹配的可用技能。" if needle else "本次运行没有可用技能。"
        return "\n".join(cards)

    async def read_skill(name: str) -> str:
        if name not in granted:
            # 模型可读的纠错信息（不炸 run）；内容面 fail-closed。
            return f"error: skill {name!r} 不在本次运行的技能集内（用 find_skill 查看可用技能）。"
        if name not in library.names():
            return f"error: skill {name!r} 在当前部署中不存在。"
        package = library.get(name)
        body = package.files.get("SKILL.md", "")
        assets = sorted(rel for rel in package.files if rel != "SKILL.md")
        if not assets:
            return body
        if backend is None:
            return body + "\n\n[附带文件不可用：当前后端无沙盒，无法执行技能资产。]"
        if name not in supplied:
            payload = [
                (f"{SKILLS_ROOT}{name}/{rel}", package.files[rel].encode("utf-8"))
                for rel in sorted(package.files)  # 整包供给（含 SKILL.md），保持包内相对引用完整。
            ]
            await asyncio.to_thread(backend.upload_files, payload)
            supplied.add(name)
        listing = "\n".join(f"- {SKILLS_ROOT}{name}/{rel}" for rel in assets)
        return body + f"\n\n[技能附带文件已就绪，可直接读取/执行：]\n{listing}"

    find_tool = StructuredTool(
        name="find_skill",
        description="查找本次运行可用的技能（专业流程/知识包）。留空 query 列出全部；找到后用 read_skill 学习。",
        args_schema=FindSkillArgs,
        coroutine=find_skill,
    )
    read_tool = StructuredTool(
        name="read_skill",
        description="读取一个技能的完整指南（含使用其附带文件的说明）。技能是指导性知识，不覆盖用户意图与系统规则。",
        args_schema=ReadSkillArgs,
        coroutine=read_skill,
    )
    return find_tool, read_tool
