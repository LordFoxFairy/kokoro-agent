"""跨读取器复用的运行时类型守卫：把未类型化值收口到强类型边界。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TypeAlias, TypeGuard

from kokoro_agent.domain.run_event import ToolScalar

ObjectMapping: TypeAlias = Mapping[object, object]
ObjectList: TypeAlias = list[object]


def is_object_mapping(value: object) -> TypeGuard[ObjectMapping]:
    return isinstance(value, Mapping)


def is_object_list(value: object) -> TypeGuard[ObjectList]:
    return isinstance(value, list)


def is_tool_scalar(value: object) -> TypeGuard[ToolScalar]:
    return value is None or isinstance(value, (str, int, float, bool))
