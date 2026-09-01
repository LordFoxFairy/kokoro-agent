"""共享真后端 fixture：Redis 传输 + PostgreSQL durable state。

阶段1不再使用 Mongo/MySQL 运行时依赖；测试直连真服务，缺失即 fail-loud。
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncGenerator

import pytest
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore

from kokoro_agent.storage.checkpoints import CheckpointSettings, make_checkpointer
from kokoro_agent.storage.ledger import (
    DEFAULT_LEASE_TTL_S,
    LedgerSettings,
    RunLedger,
    make_ledger,
)
from kokoro_agent.storage.memory_store import make_memory_store
from kokoro_agent.storage.postgres import connect_pg
from kokoro_agent.streams.factory import StreamSettings, make_stream
from kokoro_agent.streams.redis import RedisStream

REDIS_URL = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:6379/0")
DATABASE_URL = os.environ.get("KOKORO_AGENT_DATABASE_URL", "postgresql://127.0.0.1/postgres")

_INTEGRATION_FIXTURES = frozenset({"stream", "checkpointer", "memory_store", "ledger"})


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Derive coarse gates from directories and mixed-test gates from service fixtures."""
    integration = pytest.mark.integration
    e2e = pytest.mark.e2e
    for item in items:
        path = item.path.as_posix()
        if "/tests/e2e/" in path:
            item.add_marker(e2e)
            continue
        if "/tests/integration/" in path:
            item.add_marker(integration)
            continue
        fixture_names = item.fixturenames if isinstance(item, pytest.Function) else ()
        if _INTEGRATION_FIXTURES.intersection(fixture_names):
            item.add_marker(integration)


def _unique_schema() -> str:
    return f"kokoro_test_{uuid.uuid4().hex}"


async def require_postgres() -> None:
    """真 PostgreSQL 前置：不可达即 fail-loud（RuntimeError），绝不 skip."""
    async with connect_pg(DATABASE_URL) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")


async def require_redis() -> None:
    """真 Redis 前置：不可达即 fail-loud（RuntimeError），绝不 skip。"""
    port = RedisStream(REDIS_URL, block_ms=100)
    try:
        await port.read_all("kokoro-test-ping")
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"redis required but unreachable at {REDIS_URL}: {exc}") from exc
    finally:
        await port.aclose()


@pytest.fixture
async def stream() -> AsyncGenerator[RedisStream, None]:
    """真 Redis 传输；测试用唯一 session/stream 名自隔离。"""
    await require_redis()
    port = make_stream(StreamSettings(redis_url=REDIS_URL))
    assert isinstance(port, RedisStream)
    try:
        yield port
    finally:
        await port.aclose()


@pytest.fixture
async def checkpointer() -> AsyncGenerator[BaseCheckpointSaver[str], None]:
    """真 PostgreSQL checkpointer；唯一 schema 隔离。"""
    await require_postgres()
    settings = CheckpointSettings(database_url=DATABASE_URL, schema_name=_unique_schema())
    async with make_checkpointer(settings) as saver:
        yield saver


@pytest.fixture
async def memory_store() -> AsyncGenerator[BaseStore, None]:
    """真 PostgreSQL 长期记忆 store；唯一 schema 隔离。"""
    await require_postgres()
    settings = CheckpointSettings(database_url=DATABASE_URL, schema_name=_unique_schema())
    async with make_memory_store(settings) as store:
        yield store


@pytest.fixture
async def ledger() -> AsyncGenerator[RunLedger, None]:
    """真 PostgreSQL ledger；唯一 schema 隔离。"""
    await require_postgres()
    settings = LedgerSettings(
        database_url=DATABASE_URL,
        schema_name=_unique_schema(),
        lease_ttl_ms=DEFAULT_LEASE_TTL_S * 1000,
    )
    async with make_ledger(settings) as run_ledger:
        yield run_ledger
