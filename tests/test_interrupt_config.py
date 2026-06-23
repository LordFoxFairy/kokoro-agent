"""interrupt_config 单元测试：build_interrupt_on 映射与 ApprovalPolicy 单模型加载。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from kokoro_agent.infrastructure.permission.interrupt_config import build_interrupt_on
from kokoro_agent.infrastructure.permission.policy import ApprovalPolicy, approval_policy


def test_build_interrupt_on_auto_returns_empty() -> None:
    assert build_interrupt_on("auto") == {}


def test_build_interrupt_on_default_contains_yaml_tools() -> None:
    policy = approval_policy()
    result = build_interrupt_on("default")
    assert set(result.keys()) == policy.requires_approval_tools
    for config in result.values():
        assert config["allowed_decisions"] == ["approve", "edit", "reject"]


def test_approval_policy_single_model_loads_yaml() -> None:
    # 正常：list 被 validator 转 frozenset
    policy = ApprovalPolicy.model_validate({"requires_approval_tools": ["fetch_url"]})
    assert "fetch_url" in policy.requires_approval_tools

    # 异常：非法类型（字符串而非列表）必须抛 ValidationError
    with pytest.raises(ValidationError):
        ApprovalPolicy.model_validate({"requires_approval_tools": "bad"})
