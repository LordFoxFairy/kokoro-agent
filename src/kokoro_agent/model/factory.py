"""Per-run chat model factory.

Production GA never selects or authenticates a provider.  Admission supplies an
opaque authorization handle and Platform resolves the authorized gateway alias.
"""

from __future__ import annotations

from langchain_core.language_models import BaseChatModel
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.contract import ModelConfig
from kokoro_agent.model.local_fake import hitl_script, make_local_fake_chat_model
from kokoro_agent.model.platform_gateway import PlatformModelGatewayChatModel


class ChatModelSettings(BaseModel):
    """Process transport settings; model/provider policy remains per RunRequest."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    local_fake: bool
    local_fake_script: str
    model_gateway_url: str | None
    model_gateway_ca_file: str | None
    model_gateway_cert_file: str | None
    model_gateway_key_file: str | None
    model_gateway_timeout_ms: int = Field(ge=100, le=120_000)
    model_gateway_max_output_tokens: int = Field(ge=1, le=1_000_000)
    producer_generation: int = Field(gt=0)


def make_chat_model(
    settings: ChatModelSettings,
    model: ModelConfig,
    *,
    run_id: str | None = None,
) -> BaseChatModel:
    if settings.local_fake:
        script = hitl_script() if settings.local_fake_script == "hitl" else None
        return make_local_fake_chat_model(script)
    if run_id is None:
        raise ValueError("MODEL_GATEWAY_RUN_ID_REQUIRED")
    return PlatformModelGatewayChatModel(
        model_name=model.name,
        authorization_handle=model.authorization_handle,
        run_id=run_id,
        producer_generation=settings.producer_generation,
        maximum_output_tokens=settings.model_gateway_max_output_tokens,
        timeout_ms=settings.model_gateway_timeout_ms,
        gateway_url=settings.model_gateway_url,
        ca_file=settings.model_gateway_ca_file,
        cert_file=settings.model_gateway_cert_file,
        key_file=settings.model_gateway_key_file,
    )
