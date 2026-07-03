"""memory store 工厂规格：后端与 checkpoint 对齐，sqlite 落盘持久、memory 易失、namespace 前缀隔离。"""

from pathlib import Path
from typing import Literal

import pytest

from kokoro_agent.storage.checkpoints import CheckpointSettings
from kokoro_agent.storage.memory_store import make_memory_store


def _settings(backend: Literal["sqlite", "mongo", "memory"], tmp_path: Path) -> CheckpointSettings:
    return CheckpointSettings(
        backend=backend,
        sqlite_path=str(tmp_path / "memory.sqlite3"),
        mongo_url="mongodb://localhost:27017",
        mongo_db="kokoro_test",
    )


@pytest.mark.asyncio
async def test_memory_backend_is_volatile(tmp_path: Path) -> None:
    async with make_memory_store(_settings("memory", tmp_path)) as store:
        await store.aput(("ns", "memories"), "k", {"content": "v"})
        assert (await store.aget(("ns", "memories"), "k")) is not None
    async with make_memory_store(_settings("memory", tmp_path)) as store:
        assert (await store.aget(("ns", "memories"), "k")) is None


@pytest.mark.asyncio
async def test_sqlite_backend_persists_across_reopen(tmp_path: Path) -> None:
    settings = _settings("sqlite", tmp_path)
    async with make_memory_store(settings) as store:
        await store.aput(("team-a", "memories"), "pref", {"content": "dark mode"})
    async with make_memory_store(settings) as store:
        item = await store.aget(("team-a", "memories"), "pref")
        assert item is not None
        assert item.value == {"content": "dark mode"}


@pytest.mark.asyncio
async def test_sqlite_namespace_prefix_isolation(tmp_path: Path) -> None:
    settings = _settings("sqlite", tmp_path)
    async with make_memory_store(settings) as store:
        await store.aput(("team-a", "memories"), "k", {"content": "secret"})
        assert await store.asearch(("team-b",)) == []


@pytest.mark.asyncio
async def test_sqlite_store_does_not_collide_with_checkpoint_file(tmp_path: Path) -> None:
    # checkpoint 与 store 共享 sqlite_path 配置：store 必须落独立文件，互不踩表。
    settings = _settings("sqlite", tmp_path)
    async with make_memory_store(settings) as store:
        await store.aput(("ns", "memories"), "k", {"content": "v"})
    assert (tmp_path / "memory.sqlite3.store").exists()
