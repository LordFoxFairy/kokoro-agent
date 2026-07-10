"""聊天模型工厂：provider/name/effort 每请求经 wire ModelConfig 决定，凭证进程级注入。"""

from __future__ import annotations

from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel
from langchain_deepseek import ChatDeepSeek
from langchain_openai import ChatOpenAI
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
    # openai 兼容端点（GLM/DeepSeek）带 reasoning_content 时置 1：ChatOpenAI 明文拒收该字段。
    openai_reasoning: bool
    anthropic_api_key: SecretStr | None
    anthropic_base_url: str | None
    # litellm 网关档：agent 只持网关地址与网关 key（单点凭据），绝不存任何底层 provider 凭据。
    litellm_base_url: str | None
    litellm_api_key: SecretStr | None


def make_chat_model(settings: ChatModelSettings, model: ModelConfig) -> BaseChatModel:
    if settings.local_fake:
        script = hitl_script() if settings.local_fake_script == "hitl" else None
        return make_local_fake_chat_model(script)
    if model.provider == "openai":
        return _build_openai_model(settings, model)
    if model.provider == "anthropic":
        return _build_anthropic_model(settings, model)
    if model.provider == "litellm":
        return _build_litellm_model(settings, model)
    raise ValueError(f"unsupported model provider: {model.provider!r}")


def _thinking_effort(model: ModelConfig, *, off: str) -> str | None:
    """thinking 意图翻成推理力度档（True→high、False→off、None→回落 model.effort）。
    GLM 走 extra_body 原生开关；openai-plain/anthropic 无该开关，用推理力度近似意图。
    off 因 provider 而异：openai reasoning_effort 收 'minimal'，anthropic effort 用 'low'。"""
    if model.thinking is True:
        return "high"
    if model.thinking is False:
        return off
    return model.effort


def _build_openai_model(settings: ChatModelSettings, model: ModelConfig) -> BaseChatModel:
    if settings.openai_reasoning:
        if not settings.openai_base_url:
            raise ValueError("KOKORO_OPENAI_REASONING=1 requires OPENAI_BASE_URL")
        # ChatDeepSeek = openai-compat wire + 官方 reasoning_content→reasoning 块抽取。
        # thinking 意图翻成 GLM 原生开关：显式 True/False 才覆盖，None=模型默认（GLM 默认开）。
        extra_body = (
            None
            if model.thinking is None
            else {"thinking": {"type": "enabled" if model.thinking else "disabled"}}
        )
        return ChatDeepSeek(
            model=model.name,
            api_key=settings.openai_api_key,
            base_url=settings.openai_base_url,
            disable_streaming=settings.disable_streaming,
            extra_body=extra_body,
        )
    # openai 接受 api_key=None / base_url=None，无需按 None 分支。
    return init_chat_model(
        f"openai:{model.name}",
        api_key=settings.openai_api_key,
        base_url=settings.openai_base_url,
        disable_streaming=settings.disable_streaming,
        reasoning_effort=_thinking_effort(model, off="minimal"),
    )


def _build_anthropic_model(settings: ChatModelSettings, model: ModelConfig) -> BaseChatModel:
    # anthropic 推理参数名为 effort（非 reasoning_effort），混用会静默失效。
    # api_key=None 被 ChatAnthropic pydantic 拒绝，须省略以回退环境变量；base_url=None 可安全传入。
    # thinking 意图翻成 effort 档（thinking 优先于 policy 的 model.effort），否则回落 low。
    effort = _thinking_effort(model, off="low") or "low"
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


def _build_litellm_model(settings: ChatModelSettings, model: ModelConfig) -> BaseChatModel:
    # litellm 网关档 = OpenAI 兼容客户端指向网关。model.name 是网关侧 model_name（路由别名），
    # 非底层真实 model id；凭据只有网关地址 + 网关 key，底层 provider key 由网关自己持有。
    # 缺网关地址或网关 key 即"未配网关"，fail-loud——绝不让 ChatOpenAI 回落 OPENAI_API_KEY 环境变量
    # （否则会把无关的 openai 直连 key 当网关 key 发出，既错也泄）。
    if settings.litellm_base_url is None:
        raise ValueError("model.provider=='litellm' requires KOKORO_LITELLM_BASE_URL")
    if settings.litellm_api_key is None:
        raise ValueError("model.provider=='litellm' requires KOKORO_LITELLM_API_KEY")
    # thinking/effort 按 OpenAI 兼容语义尽力透传 reasoning_effort（网关转发底层 provider）；
    # 底层不支持则网关侧忽略，此处不臆造语义。
    return ChatOpenAI(
        model=model.name,
        api_key=settings.litellm_api_key,
        base_url=settings.litellm_base_url,
        disable_streaming=settings.disable_streaming,
        reasoning_effort=_thinking_effort(model, off="minimal"),
    )
