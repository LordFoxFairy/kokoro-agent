"""run 状态存储入口：按 KOKORO_RUN_STATE_BACKEND 选 sqlite（落盘）/mongo（跨 pod）。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import aiosqlite

from kokoro_agent.application.protocols.run_state import RunStateStore
from kokoro_agent.infrastructure.config import AppConfig
from kokoro_agent.infrastructure.run_state.mongo_store import (
    MongoRunStateStore,
    make_mongo_collection,
)
from kokoro_agent.infrastructure.run_state.sqlite_store import SqliteRunStateStore


@asynccontextmanager
async def make_run_state_store() -> AsyncGenerator[RunStateStore, None]:
    config = AppConfig.from_env()
    backend = config.run_state.backend
    if backend == "sqlite":
        async with aiosqlite.connect(config.run_state.db_path) as db:
            store = SqliteRunStateStore(db)
            await store.setup()
            yield store
        return
    if backend == "mongo":
        # _id=run_id 的唯一性给原子认领；client 生命周期由本工厂管。
        client, collection = make_mongo_collection(config.mongo.url, config.mongo.db)
        try:
            yield MongoRunStateStore(collection)
        finally:
            await client.close()
        return
    raise ValueError(f"unknown KOKORO_RUN_STATE_BACKEND: {backend!r}")
