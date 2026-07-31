"""非生产 legacy experiment：旧 Mongo memory store 工厂。

生产 worker 不导入、不创建本 store；Agent 的 Mongo checkpoint/ledger 生命周期由各自工厂
继续拥有。保留本模块只为 ADR-013 允许的明确非生产实验，不代表 Product Memory authority。
"""

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
