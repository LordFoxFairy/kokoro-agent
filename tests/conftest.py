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

from kokoro_agent.infrastructure.checkpoints import CheckpointSettings, make_checkpointer
from kokoro_agent.infrastructure.postgres_run_repository import (
    DEFAULT_LEASE_TTL_S,
    RunRepositorySettings,
    make_run_repository,
)
from kokoro_agent.repositories.run_repository import RunRepository
from kokoro_agent.infrastructure.memory_store import make_memory_store
from kokoro_agent.infrastructure.postgres import connect_pg
from kokoro_agent.streams.factory import StreamSettings, make_stream
from kokoro_agent.streams.redis import RedisStream

REDIS_URL = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:6379/0")
DATABASE_URL = os.environ.get("KOKORO_AGENT_DATABASE_URL", "postgresql://127.0.0.1/postgres")

_INTEGRATION_FIXTURES = frozenset({"stream", "checkpointer", "memory_store", "run_repository"})


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Derive explicit contract/service gates from directories and fixtures."""
    integration = pytest.mark.integration
    e2e = pytest.mark.e2e
    acceptance = pytest.mark.acceptance
    contract = pytest.mark.contract
    for item in items:
        path = item.path.as_posix()
        if "/tests/acceptance/" in path:
            item.add_marker(acceptance)
            continue
        if "/tests/e2e/" in path:
            item.add_marker(e2e)
            continue
        if "/tests/integration/" in path:
            item.add_marker(integration)
            continue
        if "/tests/contract/" in path:
            item.add_marker(contract)
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
async def run_repository() -> AsyncGenerator[RunRepository, None]:
    """真 PostgreSQL run_repository；唯一 schema 隔离。"""
    await require_postgres()
    settings = RunRepositorySettings(
        database_url=DATABASE_URL,
        schema_name=_unique_schema(),
        lease_ttl_ms=DEFAULT_LEASE_TTL_S * 1000,
    )
    async with make_run_repository(settings) as run_repository:
        yield run_repository
