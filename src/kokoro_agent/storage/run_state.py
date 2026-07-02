"""应用层抽象：run 状态存储契约（多 pod 去重、resume 闸、终态认领）。"""

from __future__ import annotations

from typing import Protocol

from kokoro_agent.run.request import RunRequest


class RunStateStore(Protocol):
    async def try_register(self, request: RunRequest) -> bool:
        # 原子认领 run_id：首次存 request 返 True，重复返 False，多 pod 广播请求去重。
        ...

    async def get_request(self, run_id: str) -> RunRequest | None:
        # 取原 request 供 resume 重建 agent。
        ...

    async def try_mark_terminal(self, run_id: str) -> bool:
        # 原子认领终态：首个认领者返 True，已终态返 False，杜绝重复终态事件。
        ...

    async def is_terminal(self, run_id: str) -> bool:
        # 只读查：resume stale 闸，避免对已结束 run 发起恢复。
        ...
