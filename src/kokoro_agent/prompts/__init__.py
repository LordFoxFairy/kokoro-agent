"""内置 Agent prompt 资产（随包出厂的 ``<name>.md``）。

Prompt 是 Agent/Feature 的静态装配输入；用户或项目内容不通过本包覆盖 prompt。Skill 由
DeepAgents 原生 SkillsMiddleware 注入元数据，并通过原生 ``read_file`` 渐进读取。
"""

from __future__ import annotations

from importlib.resources import files


def load_prompt(name: str) -> str:
    return files("kokoro_agent.prompts").joinpath(f"{name}.md").read_text(encoding="utf-8").strip()


GENERAL_PROMPT: str = load_prompt("general")

__all__ = ["GENERAL_PROMPT", "load_prompt"]
