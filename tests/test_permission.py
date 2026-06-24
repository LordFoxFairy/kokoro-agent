from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.permission import approval_policy, build_interrupt_on, load_approval_policy


def test_run_request_defaults_permission_mode_auto() -> None:
    req = RunRequest(
        kind="run.request",
        run_id="run_1",
        session_id="ses_1",
        conversation_id="conv_1",
        input="hi",
    )
    assert req.permission_mode == "auto"


def test_build_interrupt_on_auto_returns_empty() -> None:
    result = build_interrupt_on("auto")
    assert result == {}


def test_build_interrupt_on_default_contains_approval_tools() -> None:
    policy = approval_policy()
    result = build_interrupt_on("default")
    assert set(result.keys()) == policy.requires_approval_tools
    for config in result.values():
        # InterruptOnConfig 是 TypedDict（dict 子类），用下标访问而非属性。
        assert set(config["allowed_decisions"]) == {"approve", "edit", "reject", "respond"}


# 边界：畸形 policy 必须被 Pydantic strict/forbid 拦下，绝不静默放过脏配置。
@pytest.mark.parametrize(
    "yaml_text",
    [
        "ignored: true\n",  # 缺 requires_approval_tools
        "requires_approval_tools: [a]\nextra: 1\n",  # 多余字段
        "requires_approval_tools: fetch_url\n",  # 类型错（非列表）
        'requires_approval_tools: [""]\n',  # 空工具名违反 min_length
        "- a\n- b\n",  # 根非映射
    ],
)
def test_load_approval_policy_rejects_malformed(
    tmp_path: Path, yaml_text: str
) -> None:
    path = tmp_path / "approval_policy.yaml"
    path.write_text(yaml_text, encoding="utf-8")
    with pytest.raises(ValidationError):
        load_approval_policy(path)
