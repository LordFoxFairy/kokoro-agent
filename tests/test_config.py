"""AppConfig 单一配置入口：默认值、env 解析、approval env 迁移、边界校验。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from kokoro_agent.infrastructure.config import AppConfig, ApprovalPolicy


def test_empty_env_yields_defaults() -> None:
    config = AppConfig.from_env({})

    assert config.model.provider == "anthropic"
    assert config.model.model_name == "claude-sonnet-4-6"
    assert config.model.disable_streaming is False
    assert config.stream.backend == "memory"
    assert config.stream.redis_url == "redis://127.0.0.1:6379/0"
    assert config.observability.langfuse_configured is False
    assert config.approval.requires_approval_tools == frozenset({"fetch_url"})
    assert config.local_fake_model is False


def test_stream_redis_from_env() -> None:
    config = AppConfig.from_env(
        {"KOKORO_STREAM_BACKEND": "REDIS", "KOKORO_REDIS_URL": "redis://h:1/2"}
    )
    assert config.stream.backend == "redis"
    assert config.stream.redis_url == "redis://h:1/2"


def test_observability_configured_requires_both_keys() -> None:
    assert AppConfig.from_env({"LANGFUSE_PUBLIC_KEY": "pk"}).observability.langfuse_configured is False
    both = AppConfig.from_env({"LANGFUSE_PUBLIC_KEY": "pk", "LANGFUSE_SECRET_KEY": "sk"})
    assert both.observability.langfuse_configured is True
    assert both.observability.langfuse_secret_key is not None
    assert both.observability.langfuse_secret_key.get_secret_value() == "sk"


def test_local_fake_flag() -> None:
    assert AppConfig.from_env({"KOKORO_LOCAL_FAKE_MODEL": "1"}).local_fake_model is True
    assert AppConfig.from_env({"KOKORO_LOCAL_FAKE_MODEL": "0"}).local_fake_model is False


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("fetch_url", {"fetch_url"}),
        ("a,b,c", {"a", "b", "c"}),
        (" a , b ,, c ", {"a", "b", "c"}),  # 去空白 + 丢空段
        ("", {"fetch_url"}),  # 空串视同未设 → 回退默认
    ],
)
def test_approval_tools_from_env(raw: str, expected: set[str]) -> None:
    config = AppConfig.from_env({"KOKORO_REQUIRES_APPROVAL_TOOLS": raw})
    assert config.approval.requires_approval_tools == frozenset(expected)


def test_approval_policy_rejects_blank_tool() -> None:
    with pytest.raises(ValidationError):
        ApprovalPolicy.model_validate({"requires_approval_tools": [""]})


def test_approval_policy_rejects_unknown_keys() -> None:
    with pytest.raises(ValidationError):
        ApprovalPolicy.model_validate({"requires_approval_tools": ["x"], "rogue": True})


def test_app_config_rejects_unknown_keys() -> None:
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"rogue": 1})
