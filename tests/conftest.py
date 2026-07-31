"""共享真后端 fixture：redis 传输 + mongo checkpoint/ledger/store。

存储收敛后无内存假件——测试直连真服务（docker 起 redis+mongo）。服务缺失即
fail-loud（不 skip、不灌绿数）。每 fixture 用唯一 db/collection 隔离，串行安全。
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncGenerator

import pytest
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore
from pymongo import AsyncMongoClient

from kokoro_agent.storage.checkpoints import CheckpointSettings, make_checkpointer
from kokoro_agent.storage.ledger import (
    DEFAULT_LEASE_TTL_S,
    LedgerSettings,
    RunLedger,
    make_ledger,
)
from kokoro_agent.storage.memory_store import make_memory_store
from kokoro_agent.streams.factory import StreamSettings, make_stream
from kokoro_agent.streams.redis import RedisStream

REDIS_URL = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:6379/0")
MONGO_URL = os.environ.get(
    "KOKORO_MONGO_URL",
    "mongodb://127.0.0.1:27017/?replicaSet=kokoro-rs&directConnection=true",
)


def _unique_db() -> str:
    return f"kokoro_test_{uuid.uuid4().hex}"


async def require_mongo() -> None:
    """真 mongo 前置：不可达即 fail-loud（RuntimeError），绝不 skip。"""
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(
        MONGO_URL, serverSelectionTimeoutMS=1000
    )
    try:
        await client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001 — 服务缺失显式炸，不静默 skip
        raise RuntimeError(f"mongo required but unreachable at {MONGO_URL}: {exc}") from exc
    finally:
        await client.close()


async def require_redis() -> None:
    """真 redis 前置：不可达即 fail-loud（RuntimeError），绝不 skip。"""
    port = RedisStream(REDIS_URL, block_ms=100)
    try:
        await port.read_all("kokoro-test-ping")  # 公开 API 探活
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"redis required but unreachable at {REDIS_URL}: {exc}") from exc
    finally:
        await port.aclose()


@pytest.fixture
async def stream() -> AsyncGenerator[RedisStream, None]:
    """真 redis 传输；测试用唯一 session/stream 名自隔离。"""
    await require_redis()
    port = make_stream(StreamSettings(redis_url=REDIS_URL))
    assert isinstance(port, RedisStream)
    try:
        yield port
    finally:
        await port.aclose()


@pytest.fixture
async def checkpointer() -> AsyncGenerator[BaseCheckpointSaver[str], None]:
    """真 mongo checkpointer；唯一 db 隔离。"""
    await require_mongo()
    settings = CheckpointSettings(mongo_url=MONGO_URL, mongo_db=_unique_db())
    async with make_checkpointer(settings) as saver:
        yield saver


@pytest.fixture
async def memory_store() -> AsyncGenerator[BaseStore, None]:
    """真 mongo 长期记忆 store；唯一 db 隔离。"""
    await require_mongo()
    settings = CheckpointSettings(mongo_url=MONGO_URL, mongo_db=_unique_db())
    async with make_memory_store(settings) as store:
        yield store


@pytest.fixture
async def ledger() -> AsyncGenerator[RunLedger, None]:
    """真 mongo ledger；唯一 db 隔离。"""
    await require_mongo()
    settings = LedgerSettings(
        mongo_url=MONGO_URL, mongo_db=_unique_db(), lease_ttl_ms=DEFAULT_LEASE_TTL_S * 1000
    )
    async with make_ledger(settings) as run_ledger:
        yield run_ledger
