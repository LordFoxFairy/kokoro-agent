import pytest

from kokoro_agent.clients.system import ResolvedModel
from kokoro_agent.model.factory import model_from_route
from kokoro_agent.policy import ModelConfig


def route() -> ResolvedModel:
    return ResolvedModel(
        model_id="model",
        provider_id="provider",
        revision_id="revision",
        revision=1,
        digest="a" * 64,
        generation="1",
        tenant_generation="1",
        provider_model_name="provider-name",
        gateway_model_name="gateway-name",
        feature_key="chat",
        label_key="fast",
    )


def test_owner_route_selects_gateway_target_not_caller_provider_name() -> None:
    assert model_from_route(route(), None) == ModelConfig(
        provider="litellm", name="gateway-name"
    )


def test_matching_static_declaration_preserves_execution_settings() -> None:
    declared = ModelConfig(
        provider="litellm", name="gateway-name", effort="low", thinking=False
    )
    assert model_from_route(route(), declared) == declared


@pytest.mark.parametrize(
    "declared",
    [
        ModelConfig(provider="openai", name="gateway-name"),
        ModelConfig(provider="litellm", name="other"),
    ],
)
def test_conflicting_static_declaration_fails_closed(declared: ModelConfig) -> None:
    with pytest.raises(ValueError, match="declared model conflicts"):
        model_from_route(route(), declared)
