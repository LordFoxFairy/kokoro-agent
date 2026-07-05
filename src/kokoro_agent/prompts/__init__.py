"""人格资产域：prompts/<name>.md（内置随包）+ KOKORO_PERSONAS_DIR 部署扩展（同名覆盖）。

配置里 system_prompt 是可选内联覆盖；缺省时 agent 按入口/子代理名在此按名解析。
"""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path


def load_prompt(name: str) -> str:
    return files("kokoro_agent.prompts").joinpath(f"{name}.md").read_text(encoding="utf-8").strip()


GENERAL_PERSONA: str = load_prompt("general")


class PersonaLibrary:
    """按名取人格全文；部署目录优先于内置包资源；未知名返回 None（调用方决定兜底/报错）。"""

    def __init__(self, extra_dir: str | None) -> None:
        self._extra = Path(extra_dir) if extra_dir else None

    def get(self, name: str) -> str | None:
        if self._extra is not None:
            candidate = self._extra / f"{name}.md"
            if candidate.is_file():
                return candidate.read_text(encoding="utf-8").strip()
        resource = files("kokoro_agent.prompts").joinpath(f"{name}.md")
        if resource.is_file():
            return resource.read_text(encoding="utf-8").strip()
        return None


def compose_system_prompt(persona: str, skills_prompt: str | None) -> str:
    """模型可见面的两段组合：人格 + skills 全文。工具用法不进 system prompt——
    它活在各工具的 description，由 LangChain 经工具 schema 交给模型。"""
    return "\n\n".join(part for part in (persona, skills_prompt) if part)
