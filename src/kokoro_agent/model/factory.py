"""模型构造：System 路由确定目标，Agent 声明确定执行参数，凭据进程级注入。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import httpx

from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel
from langchain_deepseek import ChatDeepSeek
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, ConfigDict, SecretStr

from kokoro_agent.policy import ModelConfig

from kokoro_agent.clients.system import ResolvedModel
from kokoro_agent.model.credentials import ModelCredential, credential_for_provider
from kokoro_agent.model.usage_observation import (
    observe_usage_response,
    observe_usage_response_async,
)


from kokoro_agent.model.request_profile import validate_request_profile
from kokoro_agent.model.call_runtime import (
    freeze_final_model_request,
    reject_final_model_request,
)
from kokoro_agent.model.call_models import (
    ModelCallAuthorizationUnavailable,
    ModelCallInputInvalid,
)


class ChatModelSettings(BaseModel):
    """进程级凭据与流控开关；provider/name/effort 属每请求维度，不归入。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    disable_streaming: bool
    openai_api_key: SecretStr | None
    openai_base_url: str | None
    # openai 兼容端点（GLM/DeepSeek）带 reasoning_content 时置 1：ChatOpenAI 明文拒收该字段。
    openai_reasoning: bool
    anthropic_api_key: SecretStr | None
    anthropic_base_url: str | None
    model_credentials: tuple[ModelCredential, ...] = ()
    model_allow_loopback_http: bool = False


@dataclass(frozen=True, slots=True)
class ModelHttpClients:
    """Own one route's transports; validate final wire bytes before network I/O."""

    settings: ChatModelSettings
    route: ResolvedModel
    sync: httpx.Client = field(init=False, repr=False)
    async_client: httpx.AsyncClient = field(init=False, repr=False)
    endpoint: httpx.URL = field(init=False, repr=False)
    credential: SecretStr = field(init=False, repr=False)
    _close_task: asyncio.Task[None] | None = field(init=False, default=None, repr=False)

    def __post_init__(self) -> None:
        self.route.__post_init__()
        base = httpx.URL(self.route.connection.base_url)
        if base.scheme == "http" and not self.settings.model_allow_loopback_http:
            raise ValueError(
                "HTTP model loopback requires explicit deployment permission"
            )
        credential = credential_for_provider(
            self.settings.model_credentials,
            self.route.provider_id,
            self.route.connection.secret_handle_ref,
        )
        object.__setattr__(self, "credential", credential)
        endpoint = httpx.URL(str(base).rstrip("/") + "/chat/completions")
        object.__setattr__(self, "endpoint", endpoint)
        sync = httpx.Client(
            event_hooks={
                "request": [self._validate_sync],
                "response": [observe_usage_response],
            },
            follow_redirects=False,
            trust_env=False,
        )
        object.__setattr__(self, "sync", sync)
        try:
            client = httpx.AsyncClient(
                event_hooks={
                    "request": [self._validate_async],
                    "response": [observe_usage_response_async],
                },
                follow_redirects=False,
                trust_env=False,
            )
        except BaseException:
            sync.close()
            raise
        object.__setattr__(self, "async_client", client)

    def _validate(self, request: httpx.Request, raw: bytes) -> None:
        validate_request_profile(
            raw=raw,
            method=request.method,
            endpoint=str(request.url),
            expected_endpoint=str(self.endpoint),
            headers=request.headers.multi_items(),
            model_name=self.route.request_model,
            max_output_tokens=self.route.max_output_tokens,
        )

    def _validate_sync(self, request: httpx.Request) -> None:
        try:
            raw = request.content
        except httpx.RequestNotRead:
            reject_final_model_request(
                ModelCallInputInvalid("model request must be materialized")
            )
        try:
            self._validate(request, raw)
        except ModelCallInputInvalid as error:
            reject_final_model_request(error)
        reject_final_model_request(
            ModelCallAuthorizationUnavailable(
                "synchronous model authorization is unavailable"
            )
        )

    async def _validate_async(self, request: httpx.Request) -> None:
        try:
            raw = request.content
        except httpx.RequestNotRead:
            reject_final_model_request(
                ModelCallInputInvalid("model request must be materialized")
            )
        await freeze_final_model_request(
            route=self.route,
            raw=raw,
            method=request.method,
            endpoint=str(request.url),
            expected_endpoint=str(self.endpoint),
            headers=request.headers.multi_items(),
        )

    async def _close(self) -> None:
        errors: list[BaseException] = []
        try:
            self.sync.close()
        except BaseException as error:
            errors.append(error)
        try:
            await self.async_client.aclose()
        except BaseException as error:
            errors.append(error)
        if errors:
            raise BaseExceptionGroup("model client cleanup failed", errors)

    async def aclose(self) -> None:
        """Exactly one close task, settled even if callers are cancelled again."""
        if self._close_task is None:
            object.__setattr__(self, "_close_task", asyncio.create_task(self._close()))
        task = self._close_task
        assert task is not None
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        task.result()
        if cancelled:
            raise asyncio.CancelledError()


def make_chat_model(
    settings: ChatModelSettings,
    model: ModelConfig,
    *,
    model_clients: ModelHttpClients | None = None,
) -> BaseChatModel:
    if model_clients is not None:
        if (
            model.provider != model_clients.route.connection.protocol
            or model_clients.settings != settings
            or model_clients.route.request_model != model.name
            or model_clients.sync.is_closed
            or model_clients.async_client.is_closed
        ):
            raise ValueError("model clients conflict with authenticated route")
        return _build_openai_compatible_model(settings, model, model_clients)
    if model.provider == "openai_chat_completions":
        raise ValueError("routed protocol requires owned model clients")
    if model.provider == "openai":
        return _build_openai_model(settings, model)
    if model.provider == "anthropic":
        return _build_anthropic_model(settings, model)
    raise ValueError(f"unsupported model provider: {model.provider!r}")


def model_from_route(route: ResolvedModel, declared: ModelConfig | None) -> ModelConfig:
    """Map an authenticated owner result; a static declaration may only narrow it."""
    if declared is not None and (
        declared.provider != route.connection.protocol
        or declared.name != route.request_model
    ):
        raise ValueError("declared model conflicts with System route")
    return ModelConfig(
        provider=route.connection.protocol,
        name=route.request_model,
        effort=declared.effort if declared is not None else None,
        thinking=declared.thinking if declared is not None else None,
    )


def _thinking_effort(model: ModelConfig, *, off: str) -> str | None:
    """thinking 意图翻成推理力度档（True→high、False→off、None→回落 model.effort）。
    GLM 走 extra_body 原生开关；openai-plain/anthropic 无该开关，用推理力度近似意图。
    off 因 provider 而异：openai reasoning_effort 收 'minimal'，anthropic effort 用 'low'。"""
    if model.thinking is True:
        return "high"
    if model.thinking is False:
        return off
    return model.effort


def _build_openai_model(
    settings: ChatModelSettings, model: ModelConfig
) -> BaseChatModel:
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
            max_retries=0,
            extra_body=extra_body,
        )
    # openai 接受 api_key=None / base_url=None，无需按 None 分支。
    return init_chat_model(
        f"openai:{model.name}",
        api_key=settings.openai_api_key,
        base_url=settings.openai_base_url,
        disable_streaming=settings.disable_streaming,
        max_retries=0,
        reasoning_effort=_thinking_effort(model, off="minimal"),
    )


def _build_anthropic_model(
    settings: ChatModelSettings, model: ModelConfig
) -> BaseChatModel:
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
            max_retries=0,
            effort=effort,
        )
    return init_chat_model(
        model_spec,
        base_url=settings.anthropic_base_url,
        disable_streaming=settings.disable_streaming,
        max_retries=0,
        effort=effort,
    )


def _build_openai_compatible_model(
    settings: ChatModelSettings, model: ModelConfig, clients: ModelHttpClients
) -> BaseChatModel:
    """One protocol implementation for direct and optional LiteLLM connections."""
    return ChatOpenAI(
        model=model.name,
        api_key=clients.credential,
        base_url=clients.route.connection.base_url,
        disable_streaming=settings.disable_streaming,
        max_retries=0,
        max_completion_tokens=clients.route.max_output_tokens,
        n=1,
        http_client=clients.sync,
        http_async_client=clients.async_client,
        use_responses_api=False,
        # 流式下也回 usage（stream_options.include_usage=true）：否则 usage_metadata 空 →
        # 终态 token_usage=None → session settle 捕获 0 → 计费不扣。对所有经网关的 OpenAI 兼容
        # 后端(生产 GLM/openai 及 dev ollama)一致生效，是计费闭环所需，非本地专用。
        stream_usage=True,
        reasoning_effort=_thinking_effort(model, off="minimal"),
    )
