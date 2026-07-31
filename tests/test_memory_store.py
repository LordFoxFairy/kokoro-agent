"""非生产 legacy memory store 实验规格；不构成 Product Memory authority。"""

import os
import uuid

from pymongo import AsyncMongoClient

from kokoro_agent.storage.checkpoints import CheckpointSettings
from kokoro_agent.storage.memory_store import make_memory_store

_MONGO_URL = os.environ.get(
    "KOKORO_MONGO_URL",
    "mongodb://127.0.0.1:27017/?replicaSet=kokoro-rs&directConnection=true",
)


def _settings() -> CheckpointSettings:
    return CheckpointSettings(mongo_url=_MONGO_URL, mongo_db=f"kokoro_test_{uuid.uuid4().hex}")


async def _drop(mongo_db: str) -> None:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(_MONGO_URL)
    try:
        await client.drop_database(mongo_db)
    finally:
        await client.close()


async def test_mongo_backend_persists_across_reopen() -> None:
    settings = _settings()
    async with make_memory_store(settings) as store:
        await store.aput(("team-a", "memories"), "pref", {"content": "dark mode"})
    # 新工厂周期（模拟重启/另一 pod）从同一 mongo 续读——store 持久，非易失。
    async with make_memory_store(settings) as store:
        item = await store.aget(("team-a", "memories"), "pref")
        assert item is not None
        assert item.value == {"content": "dark mode"}
    await _drop(settings.mongo_db)


async def test_mongo_namespace_prefix_isolation() -> None:
    settings = _settings()
    async with make_memory_store(settings) as store:
        await store.aput(("team-a", "memories"), "k", {"content": "secret"})
        assert await store.asearch(("team-b",)) == []
    await _drop(settings.mongo_db)
