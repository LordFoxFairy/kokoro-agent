from __future__ import annotations

from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.config import AppConfig
from kokoro_agent.infrastructure.permission import build_interrupt_on


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
    policy = AppConfig.from_env().approval
    result = build_interrupt_on("default")
    assert set(result.keys()) == policy.requires_approval_tools
    for config in result.values():
        # InterruptOnConfig 是 TypedDict（dict 子类），用下标访问而非属性。
        assert set(config["allowed_decisions"]) == {"approve", "edit", "reject", "respond"}
