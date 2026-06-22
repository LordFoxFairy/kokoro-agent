from __future__ import annotations

import pytest
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI

from kokoro_agent.infrastructure.model import (
    DEFAULT_MODEL,
    LOCAL_FAKE_MODEL_FLAG,
    make_chat_model,
)
from kokoro_agent.infrastructure.model import LocalFakeChatModel


def test_make_chat_model_reads_default_anthropic_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KOKORO_MODEL", raising=False)
    monkeypatch.delenv(LOCAL_FAKE_MODEL_FLAG, raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy-key-not-used")

    model = make_chat_model()

    assert isinstance(model, ChatAnthropic)
    assert model.model == "claude-sonnet-4-6"
    assert model.effort == "low"


def test_make_chat_model_custom_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(LOCAL_FAKE_MODEL_FLAG, raising=False)
    monkeypatch.setenv("KOKORO_MODEL", DEFAULT_MODEL)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy-key-not-used")

    model = make_chat_model()

    assert isinstance(model, ChatAnthropic)
    assert model.model == "claude-sonnet-4-6"


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

    assert isinstance(thinking, ChatOpenAI)
    assert isinstance(fast, ChatOpenAI)
    assert thinking.model_name == "glm-5"
    assert fast.model_name == "glm-5"
    assert thinking.reasoning_effort == "high"
    assert fast.reasoning_effort is None
    assert thinking.disable_streaming is True


def test_make_chat_model_thinking_sets_distinct_anthropic_effort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(LOCAL_FAKE_MODEL_FLAG, raising=False)
    monkeypatch.setenv("KOKORO_MODEL", DEFAULT_MODEL)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy-key-not-used")

    thinking = make_chat_model("thinking")
    fast = make_chat_model("fast")

    assert isinstance(thinking, ChatAnthropic)
    assert isinstance(fast, ChatAnthropic)
    assert thinking.model == "claude-sonnet-4-6"
    assert fast.model == "claude-sonnet-4-6"
    assert thinking.effort == "medium"
    assert fast.effort == "low"


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
    monkeypatch.delenv(LOCAL_FAKE_MODEL_FLAG, raising=False)
    monkeypatch.setenv("KOKORO_MODEL", "not-a-valid-provider-spec-xyz")
    with pytest.raises(ValueError, match="Invalid KOKORO_MODEL spec"):
        make_chat_model()


@pytest.mark.parametrize(
    "spec",
    ["plainstring", "anthropic:", ":model", ""],
)
def test_make_chat_model_rejects_malformed_spec(
    monkeypatch: pytest.MonkeyPatch, spec: str
) -> None:
    monkeypatch.delenv(LOCAL_FAKE_MODEL_FLAG, raising=False)
    monkeypatch.setenv("KOKORO_MODEL", spec)
    with pytest.raises(ValueError, match="Invalid KOKORO_MODEL spec"):
        make_chat_model()


def test_make_chat_model_rejects_unsupported_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(LOCAL_FAKE_MODEL_FLAG, raising=False)
    monkeypatch.setenv("KOKORO_MODEL", "bogus:model")
    with pytest.raises(ValueError, match="Unsupported model provider"):
        make_chat_model()


def test_make_chat_model_rejects_invalid_execution_style(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(LOCAL_FAKE_MODEL_FLAG, raising=False)
    monkeypatch.setenv("KOKORO_MODEL", DEFAULT_MODEL)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy-key-not-used")
    with pytest.raises(ValueError, match="Invalid execution_style"):
        make_chat_model("default")


@pytest.mark.parametrize(
    "spec,provider,model_name",
    [
        (" openai:gpt-5", "openai", "gpt-5"),
        ("openai:gpt-5 ", "openai", "gpt-5"),
        ("openai: gpt-5", "openai", "gpt-5"),
    ],
)
def test_make_chat_model_strips_whitespace_in_model_spec(
    monkeypatch: pytest.MonkeyPatch,
    spec: str,
    provider: str,
    model_name: str,
) -> None:
    monkeypatch.delenv(LOCAL_FAKE_MODEL_FLAG, raising=False)
    monkeypatch.setenv("KOKORO_MODEL", spec)
    monkeypatch.setenv("OPENAI_API_KEY", "dummy-key-not-used")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://example.com")

    model = make_chat_model()

    assert isinstance(model, ChatOpenAI)
    assert provider == "openai"
    assert model.model_name == model_name
