from __future__ import annotations

import pytest

from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure import observability
from kokoro_agent.infrastructure.observability import trace_config


def _clear_langfuse_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_HOST"):
        monkeypatch.delenv(key, raising=False)


def test_langfuse_unconfigured_when_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_langfuse_env(monkeypatch)
    assert observability.langfuse_configured() is False
    assert observability.build_langfuse_handler() is None


def test_langfuse_unconfigured_when_only_one_key(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_langfuse_env(monkeypatch)
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-only")
    assert observability.langfuse_configured() is False
    assert observability.build_langfuse_handler() is None


def test_build_handler_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    sentinel = "handler-sentinel"
    init_calls: list[int] = []
    monkeypatch.setattr(observability, "Langfuse", lambda: init_calls.append(1))
    monkeypatch.setattr(observability, "CallbackHandler", lambda: sentinel)

    handler = observability.build_langfuse_handler()

    assert handler is sentinel
    assert init_calls == [1]


def _req() -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id="run_1",
        session_id="ses_1",
        conversation_id="conv_1",
        input="hi",
        execution_style="thinking",
    )


def test_trace_config_none_when_unconfigured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(observability, "build_langfuse_handler", lambda: None)
    assert trace_config(_req()) is None


def test_trace_config_tags_run_metadata_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = "handler-sentinel"
    monkeypatch.setattr(observability, "build_langfuse_handler", lambda: handler)
    config = trace_config(_req())

    assert config is not None
    assert "callbacks" in config
    assert config["callbacks"] == [handler]
    assert "metadata" in config
    metadata = config["metadata"]
    assert metadata["langfuse_session_id"] == "ses_1"
    assert metadata["langfuse_tags"] == ["thinking"]
    assert metadata["kokoro_run_id"] == "run_1"
    assert metadata["kokoro_conversation_id"] == "conv_1"
