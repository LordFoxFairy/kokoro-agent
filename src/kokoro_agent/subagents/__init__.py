"""子代理域：类型、内建定义与目录。"""

from __future__ import annotations

from kokoro_agent.subagents.assemble import (
    catalog_subagents,
    general_purpose_subagent,
)
from kokoro_agent.subagents.catalog import (
    BUILT_IN_SUBAGENTS,
    RegisteredSubagent,
    SubagentCatalog,
    build_catalog,
)

__all__ = [
    "catalog_subagents",
    "general_purpose_subagent",
    "BUILT_IN_SUBAGENTS",
    "RegisteredSubagent",
    "SubagentCatalog",
    "build_catalog",
]
