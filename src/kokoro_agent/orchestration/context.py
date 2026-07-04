"""上下文构造器：模型可见面的组合（人格 + skills 全文）。"""

from __future__ import annotations

def compose_system_prompt(persona: str, skills_prompt: str | None) -> str:
    """两段组合：人格（入口预设或成品缺省）+ skills 全文。
    工具用法不进 system prompt——它活在各工具的 description，由 LangChain 经工具 schema 交给模型。"""
    return "\n\n".join(part for part in (persona, skills_prompt) if part)

