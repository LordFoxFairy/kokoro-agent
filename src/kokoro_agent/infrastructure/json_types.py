"""基础设施层：流事件 JSON 载荷的边界校验与深拷贝。"""

from __future__ import annotations

import copy
from typing import TypeAlias

from pydantic import JsonValue, TypeAdapter

JsonObject: TypeAlias = dict[str, JsonValue]

# 边界洗净器：外部 JSON 在此一次性校验为强类型，非法输入直接抛 ValidationError（ValueError 子类）。
_EVENT_ADAPTER: TypeAdapter[JsonObject] = TypeAdapter(JsonObject)


def validate_event(event: object) -> JsonObject:
    return _EVENT_ADAPTER.validate_python(event)


def clone_event(event: JsonObject) -> JsonObject:
    return copy.deepcopy(event)
