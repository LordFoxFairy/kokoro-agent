"""应用层：单条文本通道的增量累加器——逐 delta 累积，落定时一次性取走全量并清零。"""

from __future__ import annotations


class TextAccumulator:
    """一个文本通道(主链路或某子智能体)的累积缓冲：append 增量、take 取全量并清零。"""

    def __init__(self) -> None:
        self._buffer: str | None = None

    def append(self, delta: str) -> str:
        self._buffer = (self._buffer or "") + delta
        return delta

    def started(self) -> bool:
        return self._buffer is not None

    def take(self) -> str | None:
        # None 表示本段未经历流式累积——调用方据此区分「流式落定」与「直出终答」两条路径。
        accumulated = self._buffer
        self._buffer = None
        return accumulated
