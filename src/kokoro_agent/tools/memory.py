"""长期记忆工具：store 前缀 = RunContext.namespace，跨租户结构性不可见。"""

from __future__ import annotations

from langchain_core.tools import StructuredTool
from langgraph.runtime import get_runtime
from langgraph.store.base import BaseStore
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.run.context import RunContext

SAVE_MEMORY_TOOL_NAME = "save_memory"
SEARCH_MEMORY_TOOL_NAME = "search_memory"
_MEMORY_SEGMENT = "memories"
_SEARCH_LIMIT = 8


class SaveMemoryArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    key: str = Field(min_length=1)
    content: str = Field(min_length=1)


class SearchMemoryArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    query: str = Field(min_length=1)


def _store_and_prefix() -> tuple[BaseStore, tuple[str, str]]:
    runtime = get_runtime(RunContext)
    if runtime.store is None:
        raise RuntimeError("memory store is not wired into the graph")
    return runtime.store, (runtime.context.namespace, _MEMORY_SEGMENT)


async def _save_memory(key: str, content: str) -> str:
    if not content.strip():
        raise ValueError("memory content must be non-empty")
    store, prefix = _store_and_prefix()
    await store.aput(prefix, key, {"content": content})
    return f"memory saved under key {key!r}"


async def _search_memory(query: str) -> str:
    store, prefix = _store_and_prefix()
    items = await store.asearch(prefix, query=query, limit=_SEARCH_LIMIT)
    if not items:
        return "no memories found"
    return "\n".join(f"- {item.key}: {item.value.get('content', '')}" for item in items)


SAVE_MEMORY_TOOL = StructuredTool(
    name=SAVE_MEMORY_TOOL_NAME,
    description="Persist a durable memory for this workspace (short kebab-case key).",
    args_schema=SaveMemoryArgs,
    coroutine=_save_memory,
)

SEARCH_MEMORY_TOOL = StructuredTool(
    name=SEARCH_MEMORY_TOOL_NAME,
    description="Search durable memories saved in earlier runs of this workspace.",
    args_schema=SearchMemoryArgs,
    coroutine=_search_memory,
)

MEMORY_TOOLS: tuple[StructuredTool, ...] = (SAVE_MEMORY_TOOL, SEARCH_MEMORY_TOOL)
