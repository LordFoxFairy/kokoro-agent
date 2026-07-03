"""聊天模型工厂：provider/name/effort 每请求经 wire ModelConfig 决定，凭证进程级注入。"""

from __future__ import annotations

from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel
from pydantic import BaseModel, ConfigDict, SecretStr

from kokoro_agent.contract import ModelConfig
from kokoro_agent.model.local_fake import hitl_script, make_local_fake_chat_model


class ChatModelSettings(BaseModel):
    """进程级凭据与流控开关；provider/name/effort 属每请求维度，不归入。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    disable_streaming: bool
    local_fake: bool
    # 假模型脚本档："default"（todo+文本）或 "hitl"（ask_user+审批暂停），供跨栈 e2e。
    local_fake_script: str
    openai_api_key: SecretStr | None
    openai_base_url: str | None
    anthropic_api_key: SecretStr | None
    anthropic_base_url: str | None


def make_chat_model(settings: ChatModelSettings, model: ModelConfig) -> BaseChatModel:
    if settings.local_fake:
        script = hitl_script() if settings.local_fake_script == "hitl" else None
        return make_local_fake_chat_model(script)
    if model.provider == "openai":
        return _build_openai_model(settings, model)
    if model.provider == "anthropic":
        return _build_anthropic_model(settings, model)
    raise ValueError(f"unsupported model provider: {model.provider!r}")


def _build_openai_model(settings: ChatModelSettings, model: ModelConfig) -> BaseChatModel:
    # openai 接受 api_key=None / base_url=None，无需按 None 分支。
    return init_chat_model(
        f"openai:{model.name}",
        api_key=settings.openai_api_key,
        base_url=settings.openai_base_url,
        disable_streaming=settings.disable_streaming,
        reasoning_effort=model.effort,
    )


def _build_anthropic_model(settings: ChatModelSettings, model: ModelConfig) -> BaseChatModel:
    # anthropic 推理参数名为 effort（非 reasoning_effort），混用会静默失效。
    # api_key=None 被 ChatAnthropic pydantic 拒绝，须省略以回退环境变量；base_url=None 可安全传入。
    effort = model.effort or "low"
    model_spec = f"anthropic:{model.name}"
    if settings.anthropic_api_key is not None:
        return init_chat_model(
            model_spec,
            api_key=settings.anthropic_api_key,
            base_url=settings.anthropic_base_url,
            disable_streaming=settings.disable_streaming,
            effort=effort,
        )
    return init_chat_model(
        model_spec,
        base_url=settings.anthropic_base_url,
        disable_streaming=settings.disable_streaming,
        effort=effort,
    )
