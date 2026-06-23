"""run 准入：有界、按插入序逐出的已处理 run id 集合（去重护栏）。"""

from __future__ import annotations

MAX_PROCESSED_RUN_IDS = 4096


class ProcessedRunIds:
    """超过上限即逐出最旧项，限制长驻 worker 的内存增长。"""

    def __init__(self, max_size: int = MAX_PROCESSED_RUN_IDS) -> None:
        self._ids: dict[str, None] = {}
        self._max_size = max_size

    def __contains__(self, run_id: str) -> bool:
        return run_id in self._ids

    def __len__(self) -> int:
        return len(self._ids)

    def add(self, run_id: str) -> None:
        self._ids[run_id] = None
        if len(self._ids) > self._max_size:
            oldest = next(iter(self._ids))
            del self._ids[oldest]
