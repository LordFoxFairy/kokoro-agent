"""PostgreSQL adapter shape gates that do not require a running database."""

from __future__ import annotations

import inspect

from kokoro_agent.persistence.memory_store import PgMemoryStore


def test_memory_store_implements_sync_and_async_langgraph_contract() -> None:
    """Worker startup must not fail because BaseStore's sync method is abstract."""

    assert not inspect.isabstract(PgMemoryStore)
    assert callable(PgMemoryStore.batch)
    assert callable(PgMemoryStore.abatch)
