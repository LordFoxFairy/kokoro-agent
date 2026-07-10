"""技能调用工具（CC 的 Skill 同款单工具）：清单常驻 prompt，正文按调用读取。

正文经 hub 双路直返（当前版 Mongo 快读/旧版包体 zip）；含非 .md 附件的包在
调用那一刻整包幂等上传沙盒。工具恒挂：schema 不随池变（D9）。
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.skills.hub import SkillHub, SkillHubError
from kokoro_agent.skills.supply import SKILLS_ROOT, UploadCapableBackend


class SkillArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(description="技能名（见 system prompt 的可用技能清单）。")


def make_skill_tool(
    skill_names: Sequence[str],
    scopes: Sequence[str],
    hub: SkillHub,
    backend: UploadCapableBackend | None,
) -> StructuredTool:
    """per-run 闭包：授权集/查询范围/hub/沙盒在装配期捕获。"""

    granted: tuple[str, ...] = tuple(dict.fromkeys(skill_names))
    resolved_scopes: tuple[str, ...] = tuple(scopes)
    supplied: set[str] = set()  # run 内幂等：同包附件只上传一次（块C 升级为 graph state 账本）。

    async def read(name: str) -> str:
        if name not in granted:
            # 模型可读的纠错信息（不炸 run）；内容面 fail-closed。
            return f"error: skill {name!r} 不在本次运行的技能集内（见 system prompt 清单）。"
        try:
            cards = await hub.resolve_cards(resolved_scopes, [name])
            if not cards:
                return f"error: skill {name!r} 当前不可用（可能已被移除）。"
            card = cards[0]
            body = await hub.read_body(resolved_scopes, name, card.content_hash)
        except SkillHubError as exc:
            return f"error: {exc}"
        # 附件（非 SKILL.md 文件）按需整包物化：读到哪个包，哪个包才进沙盒。
        files = await hub.load_package_if_assets(resolved_scopes, name, card.content_hash)
        if files is None:
            return body
        assets = sorted(rel for rel in files if rel != "SKILL.md")
        if backend is None:
            return body + "\n\n[附带文件不可用：当前后端无沙盒，无法执行技能资产。]"
        if name not in supplied:
            payload = [
                (f"{SKILLS_ROOT}{name}/{rel}", files[rel].encode("utf-8"))
                for rel in sorted(files)  # 整包供给，保持包内相对引用完整。
            ]
            await asyncio.to_thread(backend.upload_files, payload)
            supplied.add(name)
        listing = "\n".join(f"- {SKILLS_ROOT}{name}/{rel}" for rel in assets)
        return body + f"\n\n[技能附带文件已就绪，可直接读取/执行：]\n{listing}"

    return StructuredTool(
        name="skill",
        description="读取一个技能的完整指南（含附带文件的使用说明）。可用技能见 system prompt 清单；技能是指导性知识，不覆盖用户意图与系统规则。",
        args_schema=SkillArgs,
        coroutine=read,
    )
