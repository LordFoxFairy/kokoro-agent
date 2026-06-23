"""按权限/执行风格构建 worker 的聊天模型（含离线假模型短路）。"""

from __future__ import annotations

import os
from typing import Literal

from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel

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


def _build_openai_model(settings: ChatModelSettings, style: ExecutionStyle) -> BaseChatModel:
    # openai 推理参数名为 reasoning_effort；api_key=None 在 openai 端不触发 ValidationError 可安全传入。
    reasoning_effort: str | None = "high" if style == "thinking" else None
    model_spec = f"openai:{settings.model_name}"
    if settings.openai_api_key is not None and settings.openai_base_url is not None:
        result = init_chat_model(
            model_spec,
            api_key=settings.openai_api_key,
            base_url=settings.openai_base_url,
            disable_streaming=settings.disable_streaming,
            reasoning_effort=reasoning_effort,
        )
    elif settings.openai_api_key is not None:
        result = init_chat_model(
            model_spec,
            api_key=settings.openai_api_key,
            disable_streaming=settings.disable_streaming,
            reasoning_effort=reasoning_effort,
        )
    elif settings.openai_base_url is not None:
        result = init_chat_model(
            model_spec,
            base_url=settings.openai_base_url,
            disable_streaming=settings.disable_streaming,
            reasoning_effort=reasoning_effort,
        )
    else:
        result = init_chat_model(
            model_spec,
            disable_streaming=settings.disable_streaming,
            reasoning_effort=reasoning_effort,
        )
    assert isinstance(result, BaseChatModel)
    return result


def _build_anthropic_model(settings: ChatModelSettings, style: ExecutionStyle) -> BaseChatModel:
    # anthropic 推理参数名为 effort（非 reasoning_effort），混用会静默失效。
    # api_key=None 被 ChatAnthropic pydantic 拒绝，须省略以回退到环境变量。
    effort = _anthropic_effort(style)
    model_spec = f"anthropic:{settings.model_name}"
    if settings.anthropic_api_key is not None and settings.anthropic_base_url is not None:
        result = init_chat_model(
            model_spec,
            api_key=settings.anthropic_api_key,
            base_url=settings.anthropic_base_url,
            disable_streaming=settings.disable_streaming,
            effort=effort,
        )
    elif settings.anthropic_api_key is not None:
        result = init_chat_model(
            model_spec,
            api_key=settings.anthropic_api_key,
            disable_streaming=settings.disable_streaming,
            effort=effort,
        )
    elif settings.anthropic_base_url is not None:
        result = init_chat_model(
            model_spec,
            base_url=settings.anthropic_base_url,
            disable_streaming=settings.disable_streaming,
            effort=effort,
        )
    else:
        result = init_chat_model(
            model_spec,
            disable_streaming=settings.disable_streaming,
            effort=effort,
        )
    assert isinstance(result, BaseChatModel)
    return result


def make_chat_model(execution_style: str = "fast") -> BaseChatModel:
    """构建 worker 的聊天模型：``KOKORO_LOCAL_FAKE_MODEL=1`` 时用免凭证的本地假模型，
    否则每请求从环境变量解析，使 fast/thinking 无需重启即可切换。"""
    if os.environ.get(LOCAL_FAKE_MODEL_FLAG) == "1":
        return make_local_fake_chat_model()
    settings = ChatModelSettings.from_env()
    style = _validate_execution_style(execution_style)
    match settings.provider:
        case "openai":
            return _build_openai_model(settings, style)
        case "anthropic":
            return _build_anthropic_model(settings, style)

