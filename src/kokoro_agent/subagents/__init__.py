"""子代理域：类型、内建定义与目录。"""

from __future__ import annotations

from kokoro_agent.subagents.catalog import (
    BUILT_IN_SUBAGENTS,
    RegisteredSubagent,
    SubagentCatalog,
    build_catalog,
)

__all__ = [
    "BUILT_IN_SUBAGENTS",
    "RegisteredSubagent",
    "SubagentCatalog",
    "build_catalog",
]
