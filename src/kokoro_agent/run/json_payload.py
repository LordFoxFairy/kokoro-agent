"""领域层：JSON 对象载荷的基础类型，跨层内向依赖的单一来源。"""

from __future__ import annotations

from typing import TypeAlias

from pydantic import JsonValue

JsonObject: TypeAlias = dict[str, JsonValue]
