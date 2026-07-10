"""system prompt 解析：wire 不携带内联 prompt（客户端供给系统提示词=安全洞+破坏前缀稳定），
prompt 只按 agent（preset）名解析目录资产。Skills 渐进披露由图内机制承担，不拼接文本。"""

from __future__ import annotations

from kokoro_agent.agents.deps import AssembleDeps
from kokoro_agent.contract import RuntimeConfig


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
