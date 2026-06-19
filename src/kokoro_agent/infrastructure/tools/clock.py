from __future__ import annotations

from datetime import datetime


def now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")
