"""run 测试专用 fake。"""

from __future__ import annotations

from kokoro_agent.domain.run_request import RunRequest


class FakeRunStateStore:
    def __init__(self) -> None:
        self._requests: dict[str, RunRequest] = {}
        self._terminals: set[str] = set()

    async def try_register(self, request: RunRequest) -> bool:
        if request.run_id in self._requests:
            return False
        self._requests[request.run_id] = request
        return True

    async def get_request(self, run_id: str) -> RunRequest | None:
        return self._requests.get(run_id)

    async def try_mark_terminal(self, run_id: str) -> bool:
        if run_id in self._terminals:
            return False
        self._terminals.add(run_id)
        return True

    async def is_terminal(self, run_id: str) -> bool:
        return run_id in self._terminals
