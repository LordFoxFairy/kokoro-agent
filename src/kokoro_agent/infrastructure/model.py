from __future__ import annotations

import os
from dataclasses import dataclass

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import BaseChatModel
from langchain_openai import ChatOpenAI
from pydantic import SecretStr

from kokoro_agent.events import ExecutionStyle
from kokoro_agent.infrastructure.local_fake_model import make_local_fake_chat_model

DEFAULT_MODEL = "anthropic:claude-sonnet-4-6"
LOCAL_FAKE_MODEL_FLAG = "KOKORO_LOCAL_FAKE_MODEL"


@dataclass(frozen=True)
class ExecutionConfig:
    style: ExecutionStyle
    provider: str
    model_name: str
    disable_streaming: bool


def _split_model_spec(spec: str) -> tuple[str, str]:
    provider, sep, model_name = spec.partition(":")
    if not provider or not sep or not model_name:
        msg = f"Invalid KOKORO_MODEL spec: {spec!r}"
        raise ValueError(msg)
    return provider, model_name


def resolve_execution_config(execution_style: str) -> ExecutionConfig:
    style: ExecutionStyle = "thinking" if execution_style == "thinking" else "fast"
    provider, model_name = _split_model_spec(os.environ.get("KOKORO_MODEL", DEFAULT_MODEL))
    return ExecutionConfig(
        style=style,
        provider=provider,
        model_name=model_name,
        disable_streaming=os.environ.get("KOKORO_DISABLE_STREAMING") == "1",
    )


def _make_openai_chat_model(config: ExecutionConfig) -> BaseChatModel:
    api_key = os.environ.get("OPENAI_API_KEY")
    return ChatOpenAI(
        model=config.model_name,
        api_key=SecretStr(api_key) if api_key else None,
        base_url=os.environ.get("OPENAI_BASE_URL"),
        disable_streaming=config.disable_streaming,
        reasoning_effort="high" if config.style == "thinking" else None,
    )


def _make_anthropic_chat_model(config: ExecutionConfig) -> BaseChatModel:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        return ChatAnthropic(
            model_name=config.model_name,
            timeout=None,
            stop=None,
            api_key=SecretStr(api_key),
            base_url=os.environ.get("ANTHROPIC_BASE_URL"),
            disable_streaming=config.disable_streaming,
            effort="high" if config.style == "thinking" else None,
        )
    return ChatAnthropic(
        model_name=config.model_name,
        timeout=None,
        stop=None,
        base_url=os.environ.get("ANTHROPIC_BASE_URL"),
        disable_streaming=config.disable_streaming,
        effort="high" if config.style == "thinking" else None,
    )


def make_chat_model(execution_style: str = "fast") -> BaseChatModel:
    """Build the configured chat model for the worker.

    When ``KOKORO_LOCAL_FAKE_MODEL=1`` is set, return a deterministic local
    fake model so the real Redis-backed three-repo chain can be exercised
    without external provider credentials. Otherwise, resolve the runtime model
    per request so fast/thinking can differ without a worker restart.
    """
    if os.environ.get(LOCAL_FAKE_MODEL_FLAG) == "1":
        return make_local_fake_chat_model()

    config = resolve_execution_config(execution_style)
    if config.provider == "openai":
        return _make_openai_chat_model(config)
    if config.provider == "anthropic":
        return _make_anthropic_chat_model(config)
    msg = f"Unsupported model provider: {config.provider!r}"
    raise ValueError(msg)
