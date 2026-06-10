from __future__ import annotations

import pytest

from kokoro_agent.infrastructure.local_fake_model import LocalFakeChatModel
from kokoro_agent.infrastructure.chat_model import (
    DEFAULT_MODEL,
    LOCAL_FAKE_MODEL_FLAG,
    make_chat_model,
)


def test_make_chat_model_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Default model id when KOKORO_MODEL unset -> still returns a model object.
    # init_chat_model only builds the client object; it performs no network I/O
    # at construction time. A dummy ANTHROPIC_API_KEY satisfies the lazy client
    # builder without any real call.
    monkeypatch.delenv("KOKORO_MODEL", raising=False)
    monkeypatch.delenv(LOCAL_FAKE_MODEL_FLAG, raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy-key-not-used")
    model = make_chat_model()
    assert model is not None


def test_make_chat_model_custom_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(LOCAL_FAKE_MODEL_FLAG, raising=False)
    monkeypatch.setenv("KOKORO_MODEL", DEFAULT_MODEL)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy-key-not-used")
    model = make_chat_model()
    assert model is not None


def test_make_chat_model_thinking_sets_openai_reasoning_effort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(LOCAL_FAKE_MODEL_FLAG, raising=False)
    monkeypatch.setenv("KOKORO_MODEL", "openai:glm-5")
    monkeypatch.setenv("OPENAI_API_KEY", "dummy-key-not-used")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://example.com")
    monkeypatch.setenv("KOKORO_DISABLE_STREAMING", "1")

    thinking = make_chat_model("thinking")
    fast = make_chat_model("fast")

    assert getattr(thinking, "reasoning_effort", None) == "high"
    assert getattr(fast, "reasoning_effort", None) is None
    assert getattr(thinking, "disable_streaming", None) is True


def test_make_chat_model_uses_local_fake_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv(LOCAL_FAKE_MODEL_FLAG, "1")

    model = make_chat_model()

    assert isinstance(model, LocalFakeChatModel)


def test_make_chat_model_invalid_spec_fails_loud(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An unknown provider must surface loudly, never silently degrade.
    monkeypatch.delenv(LOCAL_FAKE_MODEL_FLAG, raising=False)
    monkeypatch.setenv("KOKORO_MODEL", "not-a-valid-provider-spec-xyz")
    with pytest.raises(Exception):  # noqa: B017, PT011 — fail-loud on any bad spec
        make_chat_model()
