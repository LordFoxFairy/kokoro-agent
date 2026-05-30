from __future__ import annotations

import pytest

from kokoro_agent.events import RunRequest
from kokoro_agent.infrastructure.model import DEFAULT_MODEL, make_chat_model
from kokoro_agent.run_agent import run_agent


def test_make_chat_model_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Default model id when KOKORO_MODEL unset -> still returns a model object.
    # init_chat_model only builds the client object; it performs no network I/O
    # at construction time. A dummy ANTHROPIC_API_KEY satisfies the lazy client
    # builder without any real call.
    monkeypatch.delenv("KOKORO_MODEL", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy-key-not-used")
    model = make_chat_model()
    assert model is not None


def test_make_chat_model_custom_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KOKORO_MODEL", DEFAULT_MODEL)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy-key-not-used")
    model = make_chat_model()
    assert model is not None


def test_make_chat_model_invalid_spec_fails_loud(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An unknown provider must surface loudly, never silently degrade.
    monkeypatch.setenv("KOKORO_MODEL", "not-a-valid-provider-spec-xyz")
    with pytest.raises(Exception):  # noqa: B017, PT011 — fail-loud on any bad spec
        make_chat_model()


@pytest.mark.asyncio
async def test_scripted_brain_yields_full_event_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Offline scripted brain: no network, no key. Through run_agent with the
    # "thinking" style it must produce the full event family.
    monkeypatch.setenv("KOKORO_MODEL", "scripted")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    model = make_chat_model()

    req = RunRequest(
        kind="run.request",
        run_id="run_scripted",
        session_id="s",
        conversation_id="c",
        input="hello",
        execution_style="thinking",
    )
    events = [e async for e in run_agent(req, model)]
    kinds = [e.kind for e in events]

    assert kinds == [
        "run.started",
        "thinking.delta",
        "tool.invoked",
        "tool.returned",
        "text.delta",
        "text.completed",
        "run.completed",
    ]
    returned = next(e for e in events if e.kind == "tool.returned")
    assert returned.payload["tool_name"] == "echo_search"
    assert returned.payload["status"] == "ok"
    text = next(e for e in events if e.kind == "text.completed")
    assert "kokoro" in str(text.payload["text"])
