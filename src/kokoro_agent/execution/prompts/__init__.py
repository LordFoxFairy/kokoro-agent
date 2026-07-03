"""系统提示词资源：正文外置 system.md，包内资源读取随 wheel 分发。"""

from __future__ import annotations

from importlib.resources import files

SYSTEM_PROMPT = (
    files("kokoro_agent.execution.prompts").joinpath("system.md").read_text(encoding="utf-8").strip()
)
