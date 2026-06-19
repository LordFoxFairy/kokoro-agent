"""内置工具：返回当前本地时间（ISO-8601，含时区）。"""

from __future__ import annotations

from datetime import datetime


def now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")
