"""工具行为指引的形状：文案随工具本体维护，context 拼装点只做条件组合与排序。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ToolGuidance:
    # 段落仅在其全部所需工具真挂载时进入 system prompt（诚实挂载：不指引不存在的工具）。
    requires: frozenset[str]
    text: str
