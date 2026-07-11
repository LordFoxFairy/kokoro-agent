"""system prompt 解析：agent（preset）名两级取值 + 技能清单段（CC 式清单常驻）。

wire 不携带内联 prompt；清单顺序=授权 names 序（稳定即 prompt 字节稳定）。
同 agent 且同技能池期间前缀恒定；池/内容变更=下一 run 一次性换轨（D9 合法触发）。
"""

from __future__ import annotations

from collections.abc import Sequence

from kokoro_agent.agents.deps import AssembleDeps
from kokoro_agent.contract import RunRequest, RuntimeConfig, SkillGrant


def resolve_system_prompt(runtime: RuntimeConfig, deps: AssembleDeps, *, default: str) -> str:
    """prompt 两级取值：
    ① agent（preset）名资产（prompts/<agent>.md）——具名 preset 的出厂 prompt
    ② default——类型工厂声明的通用缺省
    """
    if runtime.agent is not None:
        prompt = deps.prompts.get(runtime.agent)
        if prompt is not None:
            return prompt
    return default


def render_skill_manifest(base: str, grants: Sequence[SkillGrant]) -> str:
    """清单段拼装（纯函数）：卡片序=授权序（=wire 序），同输入字节恒等（前缀稳定的可测面）。"""
    if not grants:
        return base
    lines = "\n".join(f"- {grant.name}: {grant.description}" for grant in grants)
    return (
        f"{base}\n\n## 可用技能\n{lines}\n"
        "（动手前浏览清单；对得上任务的，用 skill 工具读取完整指南后照做。）"
    )


def build_system_prompt(request: RunRequest, deps: AssembleDeps, *, default: str) -> str:
    """基础 prompt + 可用技能清单段：清单零查询，直接渲染会话快照 grants（name+description
    常驻，正文经 skill 工具按快照 hash 按需读取）。"""
    base = resolve_system_prompt(request.runtime, deps, default=default)
    return render_skill_manifest(base, request.runtime.skills)
