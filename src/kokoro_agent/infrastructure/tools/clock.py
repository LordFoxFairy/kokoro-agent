"""内置工具：返回当前本地时间（ISO-8601，含时区）。"""

from __future__ import annotations

from datetime import datetime

from langchain_core.tools import StructuredTool
from pydantic import BaseModel


def now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


# 直接构造而非 from_function：后者的 classmethod 仅部分类型化，pyright strict 会判 Unknown。
class _NowArgs(BaseModel):
    pass


NOW_TOOL = StructuredTool(
    name="now",
    description="获取当前本地日期时间（ISO-8601，含时区）。涉及“今天/现在/几点”等时间问题时使用。",
    args_schema=_NowArgs,
    func=now,
)
