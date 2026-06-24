"""run 状态存储入口：按 KOKORO_RUN_STATE_BACKEND 选 sqlite（落盘）/memory（易失）。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import aiosqlite

from kokoro_agent.application.protocols.run_state import RunStateStore
from kokoro_agent.infrastructure.config import AppConfig
from kokoro_agent.infrastructure.run_state.memory_store import MemoryRunStateStore
from kokoro_agent.infrastructure.run_state.sqlite_store import SqliteRunStateStore


@asynccontextmanager
async def make_run_state_store() -> AsyncGenerator[RunStateStore, None]:
    settings = AppConfig.from_env().run_state
    if settings.backend == "memory":
        yield MemoryRunStateStore()
        return
    if settings.backend == "sqlite":
        async with aiosqlite.connect(settings.db_path) as db:
            store = SqliteRunStateStore(db)
            await store.setup()
            yield store
        return
    raise ValueError(f"unknown KOKORO_RUN_STATE_BACKEND: {settings.backend!r}")


__all__ = [
    "make_run_state_store",
    "MemoryRunStateStore",
    "RunStateStore",
    "SqliteRunStateStore",
]
