"""LangGraph checkpointer 工厂：mongo（跨 pod 唯一真源）。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.mongodb import MongoDBSaver
from pydantic import BaseModel, ConfigDict
from pymongo import MongoClient


class CheckpointSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    mongo_url: str
    mongo_db: str


@asynccontextmanager
async def make_checkpointer(
    settings: CheckpointSettings,
) -> AsyncGenerator[BaseCheckpointSaver[str], None]:
    # MongoDBSaver 用 sync MongoClient，其 async 方法经 run_in_executor 包同步调用不阻塞事件循环。
    client: MongoClient[dict[str, object]] = MongoClient(settings.mongo_url)
    try:
        yield MongoDBSaver(client, db_name=settings.mongo_db)
    finally:
        client.close()
