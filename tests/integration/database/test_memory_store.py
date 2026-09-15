"""Real PostgreSQL coverage for the LangGraph memory-store contract."""

from __future__ import annotations

from langgraph.store.base import BaseStore


async def test_memory_store_delete_filter_and_namespace_projection(
    memory_store: BaseStore,
) -> None:
    await memory_store.aput(
        ("tenant", "user", "memory"),
        "note",
        {"kind": "note", "priority": 3},
    )
    await memory_store.aput(
        ("tenant", "user", "profile"),
        "profile",
        {"kind": "profile"},
    )
    await memory_store.aput(
        ("tenant", "other", "memory"),
        "note",
        {"kind": "note", "priority": 1},
    )

    matches = await memory_store.asearch(("tenant", "user"), filter={"kind": "note"})
    assert [(item.namespace, item.key) for item in matches] == [
        (("tenant", "user", "memory"), "note")
    ]

    namespaces = await memory_store.alist_namespaces(prefix=("tenant",), max_depth=2)
    assert namespaces == [("tenant", "other"), ("tenant", "user")]

    await memory_store.adelete(("tenant", "user", "memory"), "note")
    assert await memory_store.aget(("tenant", "user", "memory"), "note") is None
