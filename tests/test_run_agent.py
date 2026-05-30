"""Tests for run_agent — the astream_events mapper."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from kokoro_agent.events import AgentEvent, RunRequest
from kokoro_agent.infrastructure.model import make_agent
from kokoro_agent.run_agent import run_agent


def _req(text: str = "plan and search", style: str = "thinking") -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id="run_1",
        session_id="s",
        conversation_id="c",
        input=text,
        execution_style=style,
    )


async def _collect(req: RunRequest) -> list[AgentEvent]:
    agent = make_agent()  # KOKORO_MODEL=scripted via monkeypatch
    return [e async for e in run_agent(req, agent)]


@pytest.mark.asyncio
async def test_scripted_run_emits_generic_tool_text_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KOKORO_MODEL", "scripted")
    events = await _collect(_req())
    kinds = [e.kind for e in events]
    assert kinds[0] == "run.started"
    assert kinds[-1] == "run.completed"
    # agent is fully generic: no plan.updated (recognition is in session)
    assert "plan.updated" not in kinds
    assert "tool.invoked" in kinds and "tool.returned" in kinds
    assert "text.completed" in kinds
    # write_todos treated as an ordinary tool; tool.invoked carries args (todos in args)
    invoked = [e for e in events if e.kind == "tool.invoked"]
    tool_names = [e.payload.get("tool_name") for e in invoked]
    assert "write_todos" in tool_names and "echo_search" in tool_names
    wt = next(e for e in invoked if e.payload.get("tool_name") == "write_todos")
    assert isinstance(wt.payload["args"], dict)
    assert isinstance(wt.payload["args"]["todos"], list)
    assert wt.payload["args"]["todos"][0]["status"] in {
        "pending",
        "in_progress",
        "completed",
    }
    # seq monotonically increasing
    seqs = [e.seq for e in events]
    assert seqs == sorted(seqs)


@pytest.mark.asyncio
async def test_fast_style_suppresses_thinking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KOKORO_MODEL", "scripted")
    events = await _collect(_req(style="fast"))
    assert all(e.kind != "thinking.delta" for e in events)


@pytest.mark.asyncio
async def test_tool_invoked_ref_matches_tool_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """tool_call_ref is stable between tool.invoked and tool.returned pairs."""
    monkeypatch.setenv("KOKORO_MODEL", "scripted")
    events = await _collect(_req())
    invoked = {e.payload["tool_call_ref"]: e for e in events if e.kind == "tool.invoked"}
    returned = {e.payload["tool_call_ref"]: e for e in events if e.kind == "tool.returned"}
    # Every invoked tool should have a matching returned
    assert set(invoked.keys()) == set(returned.keys())
    for ref in invoked:
        assert invoked[ref].payload["tool_name"] == returned[ref].payload["tool_name"]


@pytest.mark.asyncio
async def test_run_failed_on_agent_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Any unhandled exception from the agent stream yields run.failed."""
    monkeypatch.setenv("KOKORO_MODEL", "scripted")

    async def _boom_gen():  # type: ignore[no-untyped-def]
        raise RuntimeError("agent down")
        yield  # make it an async generator

    bad_agent = MagicMock()
    bad_agent.astream_events = MagicMock(return_value=_boom_gen())

    req = _req()
    events = [e async for e in run_agent(req, bad_agent)]  # type: ignore[arg-type]
    assert events[0].kind == "run.started"
    assert events[-1].kind == "run.failed"
    assert events[-1].payload["error_kind"] == "RuntimeError"
    assert "agent down" in str(events[-1].payload["message"])
    assert "run.completed" not in [e.kind for e in events]
