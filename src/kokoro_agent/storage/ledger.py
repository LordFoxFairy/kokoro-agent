"""RunLedger 契约与后端工厂：多 pod 去重、TTL 租约、HITL 暂停哨兵、终态原子认领。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated, Literal, Protocol

import aiosqlite
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.contract import RunRequest
from kokoro_agent.storage.mongo import MongoLedger, make_mongo_collection
from kokoro_agent.storage.sqlite import SqliteLedger

DEFAULT_LEASE_TTL_S = 90


class RunLedger(Protocol):
    async def try_claim(self, request: RunRequest, owner: str) -> bool:
        # 原子认领新 run：首个认领者持有 TTL 租约并返 True，重复广播去重返 False。
        ...

    async def renew(self, run_id: str, owner: str) -> bool:
        # 严格属主续租（fencing）：仅当前 owner 可续；False=所有权已被他处夺走，
        # 调用方必须让渡本地执行（裂脑双跑收窄到一个心跳窗）。
        ...

    async def adopt(self, run_id: str, owner: str) -> None:
        # 所有权交接（resume 收养/过期重拾的赢家）：置 owner 并恢复活跃租约。
        ...

    async def pause(self, run_id: str) -> None:
        # HITL 暂停：租约置哨兵，等人期间不参与过期重拾。
        ...

    async def reclaim_expired(self, owner: str) -> list[RunRequest]:
        # 过期且无终态的 run 原子重认领并连同原始 request 返回，供从 checkpoint 续跑。
        ...

    async def get_request(self, run_id: str) -> RunRequest | None:
        # 取原 request 供 resume 重建 agent。
        ...

    async def list_paused(self) -> list[str]:
        # 哨兵暂停且非终态的 run：control 监听收养的数据源（认领 worker 崩溃后接续 HITL）。
        ...

    async def add_tokens(self, run_id: str, count: int) -> int:
        # token 预算的跨段累计（resume 重建不清零）：原子加并返回累计值。
        ...

    async def add_usage(self, run_id: str, input_tokens: int, output_tokens: int) -> tuple[int, int]:
        # run.completed 用量真源：跨段累计 input/output（与预算计数分开，语义不同）。
        ...

    async def purge_terminal(self, max_age_ms: int) -> int:
        # retention 清扫：终态且超龄的 run 连同附属（tool_results/计数/信箱）整体清除，返回条数。
        ...

    async def try_mark_terminal(self, run_id: str) -> bool:
        # 原子认领终态：首个认领者返 True，杜绝重复终态事件。
        ...

    async def is_terminal(self, run_id: str) -> bool:
        # 只读查：resume stale 闸。
        ...

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None:
        # steering 信箱：keep-first 幂等（message_id 重放不覆盖）；未认领 run 安全丢弃。
        ...

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]:
        # 非破坏读信箱（按到达序返回 (message_id, content)）：模型轮前注入。
        ...

    async def ack_steers(self, run_id: str, message_ids: list[str]) -> None:
        # 确认消费：仅删除已随 checkpoint 落定的插话（下一轮见证原子性——排空绝不先于落盘）。
        ...

    async def put_tool_result(
        self, run_id: str, tool_id: str, result: str, is_error: bool
    ) -> None:
        # 结果审核暂停的双执行防护：首跑结果 keep-first 落盘，resume 重入命中即跳过工具执行。
        ...

    async def get_tool_result(self, run_id: str, tool_id: str) -> tuple[str, bool] | None: ...

    async def put_sandbox_id(self, run_id: str, sandbox_id: str) -> None:
        # e2b run 级箱绑定（keep-first）：HITL resume 重连既往 sandbox，暂停期文件不丢。
        ...

    async def get_sandbox_id(self, run_id: str) -> str | None: ...


class LedgerSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    backend: Literal["sqlite", "mongo"]
    sqlite_path: str
    mongo_url: str
    mongo_db: str
    lease_ttl_ms: Annotated[int, Field(gt=0)]


@asynccontextmanager
async def make_ledger(
    settings: LedgerSettings,
) -> AsyncGenerator[RunLedger, None]:
    if settings.backend == "sqlite":
        async with aiosqlite.connect(settings.sqlite_path) as db:
            store = SqliteLedger(db, ttl_ms=settings.lease_ttl_ms)
            await store.setup()
            yield store
        return
    client, collection = make_mongo_collection(settings.mongo_url, settings.mongo_db)
    try:
        yield MongoLedger(collection, ttl_ms=settings.lease_ttl_ms)
    finally:
        await client.close()
