"""Checkpointer 入口：按 KOKORO_CHECKPOINT_BACKEND 选 sqlite（落盘）/memory（易失）图状态存储。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from kokoro_agent.infrastructure.config import AppConfig


@asynccontextmanager
async def make_checkpointer() -> AsyncGenerator[BaseCheckpointSaver[str], None]:
    settings = AppConfig.from_env().checkpoint
    if settings.backend == "memory":
        yield InMemorySaver()
        return
    if settings.backend == "sqlite":
        # from_conn_string 进入即建表；重启/另一进程读同一文件续 pending interrupt。
        async with AsyncSqliteSaver.from_conn_string(settings.db_path) as saver:
            yield saver
        return
    raise ValueError(f"unknown KOKORO_CHECKPOINT_BACKEND: {settings.backend!r}")


__all__ = ["make_checkpointer"]
