"""内置 agent prompt 资产（随包出厂的 <name>.md）与 system prompt 组合口。

部署扩展 prompt 归 content_source（PromptLibrary 快照，同名覆盖内置）；此处只管出厂件。
"""

from __future__ import annotations

from importlib.resources import files

from kokoro_agent.prompts.library import PromptLibrary


def load_prompt(name: str) -> str:
    return files("kokoro_agent.prompts").joinpath(f"{name}.md").read_text(encoding="utf-8").strip()


GENERAL_PROMPT: str = load_prompt("general")

__all__ = ["GENERAL_PROMPT", "PromptLibrary", "load_prompt"]
