"""领域层：已注册子代理的不可变描述与来源分类。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# 子代理来源：内建 / 配置自定义 / 运行时动态注册。
SubagentSource = Literal["built-in", "config-custom", "runtime-custom"]


@dataclass(frozen=True, slots=True)
class RegisteredSubagent:
    name: str
    description: str
    system_prompt: str
    source: SubagentSource
