"""LangGraph checkpointer 工厂：sqlite（落盘）/ mongo（跨 pod）/ memory（易失）。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Literal

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.mongodb import MongoDBSaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from pydantic import BaseModel, ConfigDict
from pymongo import MongoClient


class CheckpointSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    backend: Literal["sqlite", "mongo", "memory"]
    sqlite_path: str
    mongo_url: str
    mongo_db: str


@asynccontextmanager
async def make_checkpointer(
    settings: CheckpointSettings,
) -> AsyncGenerator[BaseCheckpointSaver[str], None]:
    if settings.backend == "memory":
        yield InMemorySaver()
        return
    if settings.backend == "sqlite":
        # from_conn_string 进入即建表；重启/另一进程读同一文件续 pending interrupt。
        async with AsyncSqliteSaver.from_conn_string(settings.sqlite_path) as saver:
            yield saver
        return
    # MongoDBSaver 用 sync MongoClient，其 async 方法经 run_in_executor 包同步调用不阻塞事件循环。
    client: MongoClient[dict[str, object]] = MongoClient(settings.mongo_url)
    try:
        yield MongoDBSaver(client, db_name=settings.mongo_db)
    finally:
        client.close()
