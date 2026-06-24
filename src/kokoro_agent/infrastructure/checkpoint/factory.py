"""Checkpointer 入口：按 KOKORO_CHECKPOINT_BACKEND 选 sqlite（落盘）/mongo（跨 pod）/memory（易失）。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.mongodb import MongoDBSaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from pymongo import MongoClient

from kokoro_agent.infrastructure.config import AppConfig


@asynccontextmanager
async def make_checkpointer() -> AsyncGenerator[BaseCheckpointSaver[str], None]:
    config = AppConfig.from_env()
    backend = config.checkpoint.backend
    if backend == "memory":
        yield InMemorySaver()
        return
    if backend == "sqlite":
        # from_conn_string 进入即建表；重启/另一进程读同一文件续 pending interrupt。
        async with AsyncSqliteSaver.from_conn_string(config.checkpoint.db_path) as saver:
            yield saver
        return
    if backend == "mongo":
        # MongoDBSaver 用 sync MongoClient，其 async 方法经 run_in_executor 包同步调用不阻塞事件循环。
        client: MongoClient[dict[str, object]] = MongoClient(config.mongo.url)
        try:
            yield MongoDBSaver(client, db_name=config.mongo.db)
        finally:
            client.close()
        return
    raise ValueError(f"unknown KOKORO_CHECKPOINT_BACKEND: {backend!r}")
