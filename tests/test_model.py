from __future__ import annotations

import pytest

from kokoro_agent.infrastructure.model import DEFAULT_MODEL, make_chat_model


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
