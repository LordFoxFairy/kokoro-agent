from __future__ import annotations

from typing import Any

import pytest

from kokoro_agent.application.run_agent import trace_config
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure import observability


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
    # 配置齐全：构造单例客户端 + 返回 handler；mock 掉 langfuse 以免触网/起后台线程。
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    sentinel = object()
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
    monkeypatch.setattr("kokoro_agent.application.run_agent.build_langfuse_handler", lambda: None)
    assert trace_config(_req()) is None


def test_trace_config_tags_run_metadata_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = object()
    monkeypatch.setattr(
        "kokoro_agent.application.run_agent.build_langfuse_handler", lambda: handler
    )
    config: Any = trace_config(_req())

    assert config is not None
    assert config["callbacks"] == [handler]
    meta = config["metadata"]
    assert meta["langfuse_session_id"] == "ses_1"
    assert meta["langfuse_tags"] == ["thinking"]
    assert meta["kokoro_run_id"] == "run_1"
    assert meta["kokoro_conversation_id"] == "conv_1"
