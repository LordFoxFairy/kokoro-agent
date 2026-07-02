from __future__ import annotations

from kokoro_agent.run.request import RunRequest
from kokoro_agent.config import AppConfig
from kokoro_agent.tools.names import ASK_USER_TOOL_NAME
from kokoro_agent.tools.permissions import build_filesystem_permissions, build_interrupt_on


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
    assert set(result) == {ASK_USER_TOOL_NAME}
    assert result[ASK_USER_TOOL_NAME]["allowed_decisions"] == ["respond"]


def test_build_interrupt_on_default_contains_approval_tools() -> None:
    policy = AppConfig.from_env().approval
    result = build_interrupt_on("default")
    assert set(result.keys()) == set(policy.requires_approval_tools) | {ASK_USER_TOOL_NAME}
    for tool, config in result.items():
        # InterruptOnConfig 是 TypedDict（dict 子类），用下标访问而非属性。
        if tool == ASK_USER_TOOL_NAME:
            assert config["allowed_decisions"] == ["respond"]
        else:
            assert set(config["allowed_decisions"]) == {"approve", "edit", "reject"}


def test_filesystem_permissions_auto_unrestricted() -> None:
    assert build_filesystem_permissions("auto") == []


def test_filesystem_permissions_default_denies_writes() -> None:
    permissions = build_filesystem_permissions("default")
    assert len(permissions) == 1
    permission = permissions[0]
    assert permission.operations == ["write"]
    assert permission.paths == ["/**"]
    assert permission.mode == "deny"
