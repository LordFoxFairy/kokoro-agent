"""PostgreSQL adapter shape gates that do not require a running database."""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

from kokoro_agent.infrastructure.memory_store import PgMemoryStore
from kokoro_agent.infrastructure.postgres_run_context import OutboxFilter


_SRC = Path(__file__).resolve().parents[2] / "src" / "kokoro_agent"


def test_memory_store_implements_sync_and_async_langgraph_contract() -> None:
    """Worker startup must not fail because BaseStore's sync method is abstract."""

    assert not inspect.isabstract(PgMemoryStore)
    assert callable(PgMemoryStore.batch)
    assert callable(PgMemoryStore.abatch)


def test_outbox_fetch_exposes_a_typed_filter_and_no_sql_fragment() -> None:
    context_path = _SRC / "infrastructure" / "postgres_run_context.py"
    events_path = _SRC / "infrastructure" / "postgres_run_events.py"
    context_source = context_path.read_text(encoding="utf-8")
    events_source = events_path.read_text(encoding="utf-8")
    context_tree = ast.parse(context_source)
    fetch_methods = [
        node
        for node in ast.walk(context_tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "fetch_outbox"
    ]

    assert len(fetch_methods) == 1
    fetch_method = fetch_methods[0]
    assert len(fetch_method.args.args) == 2
    filter_argument = fetch_method.args.args[1]
    assert filter_argument.arg == "outbox_filter"
    assert isinstance(filter_argument.annotation, ast.Name)
    assert filter_argument.annotation.id == "OutboxFilter"
    assert "where_sql" not in context_source
    assert "WHERE {}" not in context_source
    assert "WHERE status = %s" in context_source
    assert "fetch_outbox(OutboxFilter.QUEUED)" in events_source
    assert tuple(OutboxFilter) == (OutboxFilter.QUEUED,)
