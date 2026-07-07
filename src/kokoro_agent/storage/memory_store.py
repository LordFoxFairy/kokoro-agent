"""长期记忆 store 工厂：mongo（后端与 checkpoint 对齐，唯一真源）。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from langgraph.store.base import BaseStore
from langgraph.store.mongodb import MongoDBStore
from pymongo import MongoClient

from kokoro_agent.storage.checkpoints import CheckpointSettings

MEMORY_COLLECTION = "kokoro_agent_memory"


@asynccontextmanager
async def make_memory_store(
    settings: CheckpointSettings,
) -> AsyncGenerator[BaseStore, None]:
    # MongoDBStore 接 sync Collection，async 方法内部 run_in_executor（同 MongoDBSaver 模式）。
    client: MongoClient[dict[str, object]] = MongoClient(settings.mongo_url)
    try:
        yield MongoDBStore(client[settings.mongo_db][MEMORY_COLLECTION])
    finally:
        client.close()
