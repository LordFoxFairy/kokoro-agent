"""interrupt_config 单元测试：build_interrupt_on 映射与 ApprovalPolicy 单模型。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from kokoro_agent.infrastructure.config import AppConfig, ApprovalPolicy
from kokoro_agent.infrastructure.permission.interrupt_config import build_interrupt_on


def test_build_interrupt_on_auto_keeps_input_required_tool() -> None:
    result = build_interrupt_on("auto")
    assert set(result.keys()) == {"ask_user_question"}
    assert result["ask_user_question"]["allowed_decisions"] == ["reject", "respond"]


def test_build_interrupt_on_default_contains_env_tools() -> None:
    policy = AppConfig.from_env().approval
    result = build_interrupt_on("default")
    assert set(result.keys()) == {*policy.requires_approval_tools, "ask_user_question"}
    assert result["ask_user_question"]["allowed_decisions"] == ["reject", "respond"]
    result.pop("ask_user_question")
    for config in result.values():
        assert config["allowed_decisions"] == ["approve", "edit", "reject", "respond"]


def test_approval_policy_single_model() -> None:
    # 正常：list 被 validator 转 frozenset
    policy = ApprovalPolicy.model_validate({"requires_approval_tools": ["web_fetch"]})
    assert "web_fetch" in policy.requires_approval_tools

    # 异常：非法类型（字符串而非列表）必须抛 ValidationError
    with pytest.raises(ValidationError):
        ApprovalPolicy.model_validate({"requires_approval_tools": "bad"})
