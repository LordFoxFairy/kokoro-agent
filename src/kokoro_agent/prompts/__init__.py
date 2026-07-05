"""prompt 资产域：人格文本随包分发（用户裁定 prompt 不进 .py）；工厂在 agents/<type>.py。"""

from __future__ import annotations

from importlib.resources import files


def load_prompt(name: str) -> str:
    return files("kokoro_agent.prompts").joinpath(f"{name}.md").read_text(encoding="utf-8").strip()


GENERAL_PERSONA: str = load_prompt("general")
