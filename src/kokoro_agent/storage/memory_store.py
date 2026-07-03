"""长期记忆 store 工厂：后端与 checkpoint 对齐（memory/sqlite/mongo），全官方实现。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from langgraph.store.base import BaseStore
from langgraph.store.memory import InMemoryStore
from langgraph.store.mongodb import MongoDBStore
from langgraph.store.sqlite.aio import AsyncSqliteStore
from pymongo import MongoClient

from kokoro_agent.storage.checkpoints import CheckpointSettings

MEMORY_COLLECTION = "kokoro_agent_memory"


@asynccontextmanager
async def make_memory_store(
    settings: CheckpointSettings,
) -> AsyncGenerator[BaseStore, None]:
    if settings.backend == "memory":
        yield InMemoryStore()
        return
    if settings.backend == "sqlite":
        # checkpoint 独占 sqlite_path 本体：store 落独立 .store 文件，互不踩表/锁。
        async with AsyncSqliteStore.from_conn_string(f"{settings.sqlite_path}.store") as store:
            await store.setup()
            yield store
        return
    # MongoDBStore 接 sync Collection，async 方法内部 run_in_executor（同 MongoDBSaver 模式）。
    client: MongoClient[dict[str, object]] = MongoClient(settings.mongo_url)
    try:
        yield MongoDBStore(client[settings.mongo_db][MEMORY_COLLECTION])
    finally:
        client.close()
