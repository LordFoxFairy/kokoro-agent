"""非生产 legacy experiment：旧 Mongo store-backed memory 工具。

ADR-013 M0 禁止任何生产模块导入或装配本模块。它暂时仅用于显式实验测试，不是 Product
Memory、不是兼容 fallback，也没有数据迁移承诺。生产能力只能经未来 Root contract 的窄
Platform MemoryPort 接入。
"""

from __future__ import annotations

from langchain_core.tools import StructuredTool
from langgraph.config import get_store
from pydantic import BaseModel, ConfigDict, Field


SAVE_MEMORY_TOOL_NAME = "save_memory"
SEARCH_MEMORY_TOOL_NAME = "search_memory"
_MEMORY_SEGMENT = "memories"
_SEARCH_LIMIT = 8
# 命名空间列举上限（过滤前拉取的候选量）：远大于返回上限，够本地关键词过滤。
_LIST_LIMIT = 200


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
        # 向量语义检索需 Atlas Vector Search（index_config + embeddings）；plain mongo 上
        # 传 query= 会触发 $vectorSearch 崩溃。改命名空间列举 + 大小写不敏感子串过滤——
        # 任意后端可用，local==prod。真语义检索留待未来 embeddings 档（model 库 spec 邻域）。
        items = await get_store().asearch(prefix, limit=_LIST_LIMIT)
        needle = query.strip().lower()
        matched = [
            item
            for item in items
            if needle in item.key.lower()
            or needle in str(item.value.get("content", "")).lower()
        ]
        if not matched:
            return "no memories found"
        return "\n".join(
            f"- {item.key}: {item.value.get('content', '')}" for item in matched[:_SEARCH_LIMIT]
        )

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
