"""Agent 装配所用的系统提示常量；正文外置于 prompts/system.md 便于维护。"""

from __future__ import annotations

from importlib.resources import files

# 包内资源读取（非裸路径），打包后随 wheel 一并分发。
SYSTEM_PROMPT = (
    files("kokoro_agent.domain").joinpath("prompts/system.md").read_text(encoding="utf-8").strip()
)
