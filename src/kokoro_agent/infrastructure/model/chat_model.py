"""按权限/执行风格构建 worker 的聊天模型（含离线假模型短路）。"""

from __future__ import annotations

import os
from typing import Literal

from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel

from kokoro_agent.domain.run_request import ExecutionStyle
from kokoro_agent.infrastructure.model.local_fake import make_local_fake_chat_model
from kokoro_agent.infrastructure.model.settings import LOCAL_FAKE_MODEL_FLAG, ChatModelSettings

__all__ = ["LOCAL_FAKE_MODEL_FLAG", "make_chat_model"]


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
    # openai 接受 api_key=None / base_url=None，无需按 None 分支——实测不触发 ValidationError。
    reasoning_effort: str | None = "high" if style == "thinking" else None
    result = init_chat_model(
        f"openai:{settings.model_name}",
        api_key=settings.openai_api_key,
        base_url=settings.openai_base_url,
        disable_streaming=settings.disable_streaming,
        reasoning_effort=reasoning_effort,
    )
    return result


def _build_anthropic_model(settings: ChatModelSettings, style: ExecutionStyle) -> BaseChatModel:
    # anthropic 推理参数名为 effort（非 reasoning_effort），混用会静默失效。
    # api_key=None 被 ChatAnthropic pydantic 拒绝，须省略以回退环境变量；base_url=None 则可安全传入。
    effort = _anthropic_effort(style)
    model_spec = f"anthropic:{settings.model_name}"
    if settings.anthropic_api_key is not None:
        result = init_chat_model(
            model_spec,
            api_key=settings.anthropic_api_key,
            base_url=settings.anthropic_base_url,
            disable_streaming=settings.disable_streaming,
            effort=effort,
        )
    else:
        result = init_chat_model(
            model_spec,
            base_url=settings.anthropic_base_url,
            disable_streaming=settings.disable_streaming,
            effort=effort,
        )
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

