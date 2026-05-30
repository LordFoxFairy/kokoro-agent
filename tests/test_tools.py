from __future__ import annotations

from kokoro_agent.tools import (
    TOOL_CALLABLES,
    TOOL_OBJECTS,
    CLOCK_STAMP,
    run_tool,
)


def test_echo_search_known_tool_returns_ok() -> None:
    status, output = run_tool("echo_search", {"query": "cats"})
    assert status == "ok"
    assert output == "results for cats"


def test_clock_returns_fixed_deterministic_stamp() -> None:
    status, output = run_tool("clock", {})
    assert status == "ok"
    # Deterministic injected stamp, never real wall-clock time.
    assert output == CLOCK_STAMP


def test_unknown_tool_returns_error_no_crash() -> None:
    status, output = run_tool("does_not_exist", {"query": "x"})
    assert status == "error"
    assert "does_not_exist" in output


def test_raising_tool_returns_error_no_crash() -> None:
    # Missing required arg makes echo_search raise; must be caught -> error.
    status, output = run_tool("echo_search", {})
    assert status == "error"
    assert output  # non-empty message


def test_registry_objects_and_callables_aligned() -> None:
    assert set(TOOL_CALLABLES) == {"echo_search", "clock"}
    names = {getattr(obj, "name", None) for obj in TOOL_OBJECTS}
    assert names == {"echo_search", "clock"}
