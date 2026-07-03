"""RunStateStore 契约与后端工厂：多 pod 去重、TTL 租约、HITL 暂停哨兵、终态原子认领。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated, Literal, Protocol

import aiosqlite
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.contract import RunRequest
from kokoro_agent.storage.mongo import MongoRunStateStore, make_mongo_collection
from kokoro_agent.storage.sqlite import SqliteRunStateStore

DEFAULT_LEASE_TTL_S = 90


class RunStateStore(Protocol):
    async def try_claim(self, request: RunRequest) -> bool:
        # 原子认领新 run：首个认领者持有 TTL 租约并返 True，重复广播去重返 False。
        ...

    async def renew(self, run_id: str) -> None:
        # 心跳续租：活跃 pod 周期性延长租约；也把暂停哨兵拉回活跃。
        ...

    async def pause(self, run_id: str) -> None:
        # HITL 暂停：租约置哨兵，等人期间不参与过期重拾。
        ...

    async def reclaim_expired(self) -> list[RunRequest]:
        # 过期且无终态的 run 原子重认领并连同原始 request 返回，供从 checkpoint 续跑。
        ...

    async def get_request(self, run_id: str) -> RunRequest | None:
        # 取原 request 供 resume 重建 agent。
        ...

    async def try_mark_terminal(self, run_id: str) -> bool:
        # 原子认领终态：首个认领者返 True，杜绝重复终态事件。
        ...

    async def is_terminal(self, run_id: str) -> bool:
        # 只读查：resume stale 闸。
        ...


class RunStateSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    backend: Literal["sqlite", "mongo"]
    sqlite_path: str
    mongo_url: str
    mongo_db: str
    lease_ttl_ms: Annotated[int, Field(gt=0)]


@asynccontextmanager
async def make_run_state_store(
    settings: RunStateSettings,
) -> AsyncGenerator[RunStateStore, None]:
    if settings.backend == "sqlite":
        async with aiosqlite.connect(settings.sqlite_path) as db:
            store = SqliteRunStateStore(db, ttl_ms=settings.lease_ttl_ms)
            await store.setup()
            yield store
        return
    client, collection = make_mongo_collection(settings.mongo_url, settings.mongo_db)
    try:
        yield MongoRunStateStore(collection, ttl_ms=settings.lease_ttl_ms)
    finally:
        await client.close()
