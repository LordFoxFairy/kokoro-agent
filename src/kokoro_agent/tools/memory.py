"""长期记忆工具：通用存取原语；归属 scope 在装配时注入，工具体不含租户概念。"""

from __future__ import annotations

from langchain_core.tools import StructuredTool
from langgraph.config import get_store
from pydantic import BaseModel, ConfigDict, Field


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


def make_memory_tools(scope: str) -> tuple[StructuredTool, ...]:
    """scope 是调用方的隔离政策（如租户 namespace）；本模块只负责在其下存取。"""
    prefix = (scope, _MEMORY_SEGMENT)

    async def save_memory(key: str, content: str) -> str:
        if not content.strip():
            raise ValueError("memory content must be non-empty")
        await get_store().aput(prefix, key, {"content": content})
        return f"memory saved under key {key!r}"

    async def search_memory(query: str) -> str:
        items = await get_store().asearch(prefix, query=query, limit=_SEARCH_LIMIT)
        if not items:
            return "no memories found"
        return "\n".join(f"- {item.key}: {item.value.get('content', '')}" for item in items)

    return (
        StructuredTool(
            name=SAVE_MEMORY_TOOL_NAME,
            description=(
                "持久保存本空间的长期记忆（key 用短横线小写）。用户表达持久偏好、"
                "纠正做法、给出可复用事实时及时保存；一次性/临时信息与任何密钥密码绝不入记忆。"
            ),
            args_schema=SaveMemoryArgs,
            coroutine=save_memory,
        ),
        StructuredTool(
            name=SEARCH_MEMORY_TOOL_NAME,
            description=(
                "检索本空间既往保存的长期记忆。请求可能涉及用户偏好或长期背景时，"
                "先查一下再动手。"
            ),
            args_schema=SearchMemoryArgs,
            coroutine=search_memory,
        ),
    )
