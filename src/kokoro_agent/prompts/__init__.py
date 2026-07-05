"""prompt 资产域：跨包共享/子代理人格文本；类型人格随各自业务包（agents/<type>/persona.md）。"""

from __future__ import annotations

from importlib.resources import files


def load_prompt(name: str) -> str:
    return files("kokoro_agent.prompts").joinpath(f"{name}.md").read_text(encoding="utf-8").strip()

