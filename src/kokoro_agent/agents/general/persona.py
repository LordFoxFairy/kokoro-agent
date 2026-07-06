"""system prompt 解析：人格三级取值（Skills V2 后不再拼接任何 skills 文本——
渐进披露由 deepagents SkillsMiddleware 在图内承担）。"""

from __future__ import annotations

from kokoro_agent.agents.base import AssembleDeps
from kokoro_agent.contract import RuntimeConfig
from kokoro_agent.prompts import GENERAL_PERSONA


def resolve_system_prompt(runtime: RuntimeConfig, deps: AssembleDeps) -> str:
    """人格取第一个命中：
    ① wire 内联 system_prompt——本次显式覆盖
    ② 入口名人格资产（personas 库按 entry 名解析）——具名入口的出厂人格
    ③ GENERAL_PERSONA——通用缺省
    """
    persona = runtime.system_prompt
    if persona is None and runtime.entry is not None:
        persona = deps.personas.get(runtime.entry)
    return persona if persona is not None else GENERAL_PERSONA
