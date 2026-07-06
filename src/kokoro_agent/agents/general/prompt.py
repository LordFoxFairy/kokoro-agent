"""system prompt 解析：agent prompt 三级取值（Skills V2 后不再拼接任何 skills 文本——
渐进披露由 deepagents SkillsMiddleware 在图内承担）。"""

from __future__ import annotations

from kokoro_agent.agents.base import AssembleDeps
from kokoro_agent.contract import RuntimeConfig
from kokoro_agent.prompts import GENERAL_PROMPT


def resolve_system_prompt(runtime: RuntimeConfig, deps: AssembleDeps) -> str:
    """prompt 取第一个命中：
    ① wire 内联 system_prompt——本次显式覆盖
    ② 入口名 prompt 资产（prompts 库按 entry 名解析）——具名入口的出厂 prompt
    ③ GENERAL_PROMPT——通用缺省
    """
    prompt = runtime.system_prompt
    if prompt is None and runtime.entry is not None:
        prompt = deps.prompts.get(runtime.entry)
    return prompt if prompt is not None else GENERAL_PROMPT
