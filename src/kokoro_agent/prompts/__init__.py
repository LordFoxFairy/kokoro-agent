"""内置 prompt 资产（随包出厂的 <name>.md）与 system prompt 组合口。

部署扩展人格归 assets 域（PersonaLibrary 快照，同名覆盖内置）；此处只管出厂件。
"""

from __future__ import annotations

from importlib.resources import files


def load_prompt(name: str) -> str:
    return files("kokoro_agent.prompts").joinpath(f"{name}.md").read_text(encoding="utf-8").strip()


GENERAL_PERSONA: str = load_prompt("general")

