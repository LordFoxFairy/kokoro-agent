"""LangGraph store operation semantics owned by the PostgreSQL adapter."""

from __future__ import annotations

from langgraph.store.base import MatchCondition
from pydantic import JsonValue

from kokoro_agent.infrastructure.memory_store import (
    matches_filter,
    matches_namespace,
    namespace_filters,
)


def test_namespace_filters_and_wildcards_match_langgraph_shape() -> None:
    prefix, suffix = namespace_filters(
        (
            MatchCondition(match_type="prefix", path=("tenant", "*")),
            MatchCondition(match_type="suffix", path=("memory",)),
        )
    )

    assert prefix == ("tenant", "*")
    assert suffix == ("memory",)
    assert matches_namespace(("tenant", "user-1", "memory"), prefix, suffix)
    assert not matches_namespace(("other", "user-1", "memory"), prefix, suffix)


def test_filter_supports_nested_values_and_comparison_operators() -> None:
    value: JsonValue = {
        "kind": "note",
        "metadata": {"priority": 3},
        "labels": ["a", "b"],
    }

    assert matches_filter(
        value,
        {
            "kind": "note",
            "metadata": {"priority": {"$gte": 2}},
            "labels": ["a", "b"],
        },
    )
    assert not matches_filter(value, {"metadata": {"priority": {"$lt": 2}}})
