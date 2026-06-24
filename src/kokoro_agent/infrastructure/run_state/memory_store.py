"""进程内易失实现：dict + set，无持久化，进程退出即丢。"""

from __future__ import annotations

from kokoro_agent.domain.run_request import RunRequest


class MemoryRunStateStore:
    """进程内易失实现：无界（bounding 留持久化后端 TTL 控制，内存版对齐现状 _runs 行为）。"""

    def __init__(self) -> None:
        self._requests: dict[str, RunRequest] = {}
        self._terminals: set[str] = set()

    async def try_register(self, request: RunRequest) -> bool:
        # 原子认领 run_id：首次存 request 返 True，重复返 False。
        if request.run_id in self._requests:
            return False
        self._requests[request.run_id] = request
        return True

    async def get_request(self, run_id: str) -> RunRequest | None:
        # 取原 request 供 resume 重建 agent。
        return self._requests.get(run_id)

    async def try_mark_terminal(self, run_id: str) -> bool:
        # 原子认领终态：首个认领者返 True，已终态返 False。
        if run_id in self._terminals:
            return False
        self._terminals.add(run_id)
        return True

    async def is_terminal(self, run_id: str) -> bool:
        # 只读查：resume stale 闸。
        return run_id in self._terminals
