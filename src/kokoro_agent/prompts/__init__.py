"""prompt 资产域：各 agent 类型的人格文本随包分发；对应配方在 orchestration/<type>.py。"""

from __future__ import annotations

from importlib.resources import files


def load_prompt(name: str) -> str:
    return files("kokoro_agent.prompts").joinpath(f"{name}.md").read_text(encoding="utf-8").strip()


GENERAL_PERSONA: str = load_prompt("general")
