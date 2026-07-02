"""interrupt_config 单元测试：build_interrupt_on 映射与 ApprovalPolicy 单模型。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from kokoro_agent.config import AppConfig, ApprovalPolicy
from kokoro_agent.tools.names import ASK_USER_TOOL_NAME
from kokoro_agent.tools.permissions import build_interrupt_on


def test_build_interrupt_on_auto_returns_empty() -> None:
    result = build_interrupt_on("auto")
    assert set(result) == {ASK_USER_TOOL_NAME}
    assert result[ASK_USER_TOOL_NAME]["allowed_decisions"] == ["respond"]


def test_build_interrupt_on_default_contains_env_tools() -> None:
    policy = AppConfig.from_env().approval
    result = build_interrupt_on("default")
    assert set(result.keys()) == set(policy.requires_approval_tools) | {ASK_USER_TOOL_NAME}
    for tool, config in result.items():
        if tool == ASK_USER_TOOL_NAME:
            assert config["allowed_decisions"] == ["respond"]
        else:
            assert config["allowed_decisions"] == ["approve", "edit", "reject"]


def test_approval_policy_single_model() -> None:
    # 正常：list 被 validator 转 frozenset
    policy = ApprovalPolicy.model_validate({"requires_approval_tools": ["external_action"]})
    assert "external_action" in policy.requires_approval_tools

    # 异常：非法类型（字符串而非列表）必须抛 ValidationError
    with pytest.raises(ValidationError):
        ApprovalPolicy.model_validate({"requires_approval_tools": "bad"})
