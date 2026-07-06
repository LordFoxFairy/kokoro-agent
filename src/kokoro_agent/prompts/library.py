"""agent prompt 库：部署快照优先于内置包资源（kokoro_agent/prompts/<name>.md 随包出厂）。"""

from __future__ import annotations

from collections.abc import Mapping
from importlib.resources import files


class PromptLibrary:
    """按名取 agent prompt 全文；未知名返回 None（调用方决定兜底/报错）。"""

    def __init__(self, extra: Mapping[str, str]) -> None:
        self._extra = dict(extra)

    def get(self, name: str) -> str | None:
        hit = self._extra.get(name)
        if hit is not None:
            return hit
        resource = files("kokoro_agent.prompts").joinpath(f"{name}.md")
        if resource.is_file():
            return resource.read_text(encoding="utf-8").strip()
        return None
