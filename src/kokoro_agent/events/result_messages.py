"""从 runner 结果 Mapping 抽取 BaseMessage 列表（进程内不透明结果 → 强类型收窄）。"""

from collections.abc import Mapping
from typing import TypeGuard

from langchain_core.messages import BaseMessage


def _is_object_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def result_messages(result: Mapping[str, object]) -> list[BaseMessage]:
    raw = result.get("messages")
    if not _is_object_list(raw):
        return []
    return [msg for msg in raw if isinstance(msg, BaseMessage)]
