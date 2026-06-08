from __future__ import annotations

import pytest

from kokoro_agent.subagents import RuntimeSubagentRegistry


def test_runtime_registry_registers_runtime_custom_source() -> None:
    registry = RuntimeSubagentRegistry()
    spec = registry.register(
        "temp-reviewer",
        "临时审稿",
        "检查当前回答是否一致",
    )

    assert spec.source == "runtime-custom"
    assert registry.get("temp-reviewer") == spec


def test_runtime_registry_rejects_duplicate_runtime_name() -> None:
    registry = RuntimeSubagentRegistry()
    registry.register("temp-reviewer", "临时审稿", "检查一致性")

    with pytest.raises(ValueError):
        registry.register("temp-reviewer", "重复", "bad")
