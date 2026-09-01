from kokoro_agent.model.factory import select_model_label
from kokoro_agent.policy import ModelConfig


def test_select_model_label_uses_provider_and_name() -> None:
    fallback = ModelConfig(provider="anthropic", name="claude", effort="low")
    assert select_model_label("anthropic:claude-code", fallback) == ModelConfig(
        provider="anthropic", name="claude-code", effort="low"
    )


def test_select_model_label_naked_name_keeps_feature_provider() -> None:
    fallback = ModelConfig(provider="openai", name="gpt-4o")
    assert select_model_label("gpt-4.1", fallback) == ModelConfig(
        provider="openai", name="gpt-4.1"
    )


def test_select_model_label_cross_provider_rejected() -> None:
    fallback = ModelConfig(provider="anthropic", name="claude")
    try:
        select_model_label("litellm:claude-code", fallback)
    except ValueError as exc:
        assert "provider must match" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_select_model_label_missing_keeps_fallback() -> None:
    fallback = ModelConfig(provider="anthropic", name="claude")
    assert select_model_label(None, fallback) is fallback
