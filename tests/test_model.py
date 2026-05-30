from __future__ import annotations

from kokoro_agent.infrastructure.model import make_agent


def test_scripted_builds_a_runnable_agent(monkeypatch: "pytest.MonkeyPatch") -> None:  # type: ignore[name-defined]  # noqa: F821
    import pytest  # noqa: PLC0415

    monkeypatch.setenv("KOKORO_MODEL", "scripted")
    agent = make_agent()
    # deep agent is a langgraph CompiledStateGraph: can astream_events.
    assert hasattr(agent, "astream_events")


def test_real_spec_builds_without_network(monkeypatch: "pytest.MonkeyPatch") -> None:  # type: ignore[name-defined]  # noqa: F821
    # Construction only (not invoked); should not touch network; missing key
    # must not raise at construction time.
    monkeypatch.setenv("KOKORO_MODEL", "anthropic:claude-sonnet-4-6")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy-key-not-used")
    agent = make_agent()
    assert hasattr(agent, "astream_events")
