from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import BaseChatModel
from langchain_openai import ChatOpenAI
from pydantic import SecretStr

from kokoro_agent.domain.run_request import ExecutionStyle
from kokoro_agent.infrastructure.model.local_fake import make_local_fake_chat_model

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
    provider = provider.strip().lower()
    model_name = model_name.strip()
    if not provider or not sep or not model_name:
        msg = f"Invalid KOKORO_MODEL spec: {spec!r}"
        raise ValueError(msg)
    return provider, model_name


def _validate_execution_style(execution_style: str) -> ExecutionStyle:
    match execution_style:
        case "fast":
            return "fast"
        case "thinking":
            return "thinking"
        case _:
            msg = f"Invalid execution_style: {execution_style!r}"
            raise ValueError(msg)


def resolve_execution_config(execution_style: str) -> ExecutionConfig:
    provider, model_name = _split_model_spec(os.environ.get("KOKORO_MODEL", DEFAULT_MODEL))
    style = _validate_execution_style(execution_style)
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


def _anthropic_effort(style: ExecutionStyle) -> Literal["medium", "low"]:
    return "medium" if style == "thinking" else "low"


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
            effort=_anthropic_effort(config.style),
        )
    return ChatAnthropic(
        model_name=config.model_name,
        timeout=None,
        stop=None,
        base_url=os.environ.get("ANTHROPIC_BASE_URL"),
        disable_streaming=config.disable_streaming,
        effort=_anthropic_effort(config.style),
    )


def make_chat_model(execution_style: str = "fast") -> BaseChatModel:
    """Build the worker's chat model: a credential-free local fake when ``KOKORO_LOCAL_FAKE_MODEL=1``, else resolved per request so fast/thinking differ without a restart."""
    if os.environ.get(LOCAL_FAKE_MODEL_FLAG) == "1":
        return make_local_fake_chat_model()

    config = resolve_execution_config(execution_style)
    match config.provider:
        case "openai":
            return _make_openai_chat_model(config)
        case "anthropic":
            return _make_anthropic_chat_model(config)
        case _:
            msg = f"Unsupported model provider: {config.provider!r}"
            raise ValueError(msg)
