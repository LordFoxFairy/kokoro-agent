"""聊天模型的进程级稳定参数：启动时从环境变量读一次。"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Literal

from pydantic import BaseModel, ConfigDict, SecretStr

DEFAULT_MODEL = "anthropic:claude-sonnet-4-6"


def _split_model_spec(spec: str) -> tuple[Literal["openai", "anthropic"], str]:
    provider, sep, model_name = spec.partition(":")
    provider = provider.strip().lower()
    model_name = model_name.strip()
    if not provider or not sep or not model_name:
        msg = f"Invalid KOKORO_MODEL spec: {spec!r}"
        raise ValueError(msg)
    if provider == "openai":
        return "openai", model_name
    if provider == "anthropic":
        return "anthropic", model_name
    msg = f"Unsupported model provider: {provider!r}"
    raise ValueError(msg)


class ChatModelSettings(BaseModel):
    """进程级稳定的聊天模型参数，从环境变量读取一次。

    execution_style（fast/thinking）是每请求维度的，仍作为 make_chat_model
    的入参，不归入这份设置。
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    provider: Literal["openai", "anthropic"]
    model_name: str
    disable_streaming: bool
    openai_api_key: SecretStr | None = None
    openai_base_url: str | None = None
    anthropic_api_key: SecretStr | None = None
    anthropic_base_url: str | None = None

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ChatModelSettings:
        source: Mapping[str, str] = env if env is not None else os.environ
        provider, model_name = _split_model_spec(source.get("KOKORO_MODEL", DEFAULT_MODEL))
        openai_key = source.get("OPENAI_API_KEY")
        anthropic_key = source.get("ANTHROPIC_API_KEY")
        return cls(
            provider=provider,
            model_name=model_name,
            disable_streaming=source.get("KOKORO_DISABLE_STREAMING") == "1",
            openai_api_key=SecretStr(openai_key) if openai_key else None,
            openai_base_url=source.get("OPENAI_BASE_URL"),
            anthropic_api_key=SecretStr(anthropic_key) if anthropic_key else None,
            anthropic_base_url=source.get("ANTHROPIC_BASE_URL"),
        )
