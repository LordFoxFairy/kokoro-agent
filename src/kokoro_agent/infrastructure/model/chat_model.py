from __future__ import annotations

import os
from typing import Literal

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import BaseChatModel
from langchain_openai import ChatOpenAI

from kokoro_agent.domain.run_request import ExecutionStyle
from kokoro_agent.infrastructure.model.local_fake import make_local_fake_chat_model
from kokoro_agent.infrastructure.model.settings import ChatModelSettings

LOCAL_FAKE_MODEL_FLAG = "KOKORO_LOCAL_FAKE_MODEL"


def _validate_execution_style(execution_style: str) -> ExecutionStyle:
    match execution_style:
        case "fast":
            return "fast"
        case "thinking":
            return "thinking"
        case _:
            msg = f"Invalid execution_style: {execution_style!r}"
            raise ValueError(msg)


def _anthropic_effort(style: ExecutionStyle) -> Literal["medium", "low"]:
    return "medium" if style == "thinking" else "low"


def _make_openai_chat_model(settings: ChatModelSettings, style: ExecutionStyle) -> BaseChatModel:
    return ChatOpenAI(
        model=settings.model_name,
        api_key=settings.openai_api_key,
        base_url=settings.openai_base_url,
        disable_streaming=settings.disable_streaming,
        reasoning_effort="high" if style == "thinking" else None,
    )


def _make_anthropic_chat_model(settings: ChatModelSettings, style: ExecutionStyle) -> BaseChatModel:
    effort = _anthropic_effort(style)
    if settings.anthropic_api_key is not None:
        return ChatAnthropic(
            model_name=settings.model_name,
            timeout=None,
            stop=None,
            api_key=settings.anthropic_api_key,
            base_url=settings.anthropic_base_url,
            disable_streaming=settings.disable_streaming,
            effort=effort,
        )
    return ChatAnthropic(
        model_name=settings.model_name,
        timeout=None,
        stop=None,
        base_url=settings.anthropic_base_url,
        disable_streaming=settings.disable_streaming,
        effort=effort,
    )


def make_chat_model(execution_style: str = "fast") -> BaseChatModel:
    """Build the worker's chat model: a credential-free local fake when
    ``KOKORO_LOCAL_FAKE_MODEL=1``, else resolved per request from the
    environment so fast/thinking differ without a restart."""
    if os.environ.get(LOCAL_FAKE_MODEL_FLAG) == "1":
        return make_local_fake_chat_model()
    settings = ChatModelSettings.from_env()
    style = _validate_execution_style(execution_style)
    match settings.provider:
        case "openai":
            return _make_openai_chat_model(settings, style)
        case "anthropic":
            return _make_anthropic_chat_model(settings, style)
        case _:
            msg = f"Unsupported model provider: {settings.provider!r}"
            raise ValueError(msg)
