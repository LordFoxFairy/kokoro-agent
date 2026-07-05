"""内置 prompt 资产（随包出厂的 <name>.md）与 system prompt 组合口。

部署扩展人格归 assets 域（PersonaLibrary 快照，同名覆盖内置）；此处只管出厂件。
"""

from __future__ import annotations

from importlib.resources import files


def load_prompt(name: str) -> str:
    return files("kokoro_agent.prompts").joinpath(f"{name}.md").read_text(encoding="utf-8").strip()


GENERAL_PERSONA: str = load_prompt("general")


def compose_system_prompt(persona: str, skills_prompt: str | None) -> str:
    """模型可见面的两段组合：人格 + skills 全文。工具用法不进 system prompt——
    它活在各工具的 description，由 LangChain 经工具 schema 交给模型。"""
    return "\n\n".join(part for part in (persona, skills_prompt) if part)
