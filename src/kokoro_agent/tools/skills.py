"""技能调用工具（CC 的 Skill 同款单工具）：清单常驻 prompt，正文按调用读取。

正文经 hub 双路直返（当前版 Mongo 快读/旧版包体 zip）。附件不在此上传——物化由装配期
reconcile 中间件按 graph state 账本完成；本工具读账本判断附件是否就绪，据此引导或标记不可用。
工具恒挂：schema 不随池变（D9）。
"""

from __future__ import annotations

from collections.abc import Sequence

from langchain.tools import ToolRuntime
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.contract import SkillGrant
from kokoro_agent.skills.hub import SkillHub, SkillHubError
from kokoro_agent.skills.supply import SKILLS_ROOT
from kokoro_agent.state import KokoroAgentState


class SkillArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(description="技能名（见 system prompt 的可用技能清单）。")


def _ledger_from_state(runtime: ToolRuntime[None, KokoroAgentState]) -> dict[str, str]:
    return runtime.state.get("skills_materialized") or {}


def make_skill_tool(
    grants: Sequence[SkillGrant],
    hub: SkillHub,
) -> StructuredTool:
    """per-run 闭包：授权集（name→快照卡，含 scope 与 hash）/hub 在装配期捕获。"""

    # 内容锁：授权按快照卡（scope+content_hash）定死，正文/附件永远按卡片归属读，官方升级不影响本会话。
    granted: dict[str, SkillGrant] = {grant.name: grant for grant in grants}

    async def read(name: str, runtime: ToolRuntime[None, KokoroAgentState]) -> str:
        grant = granted.get(name)
        if grant is None:
            # 模型可读的纠错信息（不炸 run）；内容面 fail-closed。
            return f"error: skill {name!r} 不在本次运行的技能集内（见 system prompt 清单）。"
        try:
            body = await hub.read_body(grant.scope, name, grant.content_hash)
            files = await hub.load_package_if_assets(grant.scope, name, grant.content_hash)
        except SkillHubError as exc:
            return f"error: {exc}"
        if files is None:
            return body  # 纯知识包：无附件，正文即全部。
        assets = sorted(rel for rel in files if rel != "SKILL.md")
        # 附件是否已物化：查装配期 reconcile 写下的 graph state 账本（按快照 hash 归属）。
        if _ledger_from_state(runtime).get(name) != grant.content_hash:
            return body + "\n\n[附带文件本次运行不可用：技能资产未物化到沙盒，仅正文可用。]"
        listing = "\n".join(f"- {SKILLS_ROOT}{name}/{rel}" for rel in assets)
        return body + f"\n\n[技能附带文件已就绪，可直接读取/执行：]\n{listing}"

    # ToolNode 按 coroutine 注解注入 runtime（不入 args_schema）；schema 恒为 SkillArgs（D9）。
    return StructuredTool(
        name="skill",
        description="读取一个技能的完整指南（含附带文件的使用说明）。可用技能见 system prompt 清单；技能是指导性知识，不覆盖用户意图与系统规则。",
        args_schema=SkillArgs,
        coroutine=read,
    )
