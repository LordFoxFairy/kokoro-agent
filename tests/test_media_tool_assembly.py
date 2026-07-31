"""Media tool assembly is the conjunction of catalog grant and opaque run authority."""

from __future__ import annotations

import pytest

from fakes import request
from kokoro_agent.config import AppConfig
from kokoro_agent.contract import MediaRuntimeGrant
from kokoro_agent.platform import AgentImageCreateCommand, MediaCommandAccepted, MediaOperationPort
from kokoro_agent.tools.media import CREATE_IMAGE_TOOL_NAME
from kokoro_agent.tools.registry import JOURNAL_EXEMPT_TOOLS, media_tools_for_run


class UnusedMediaPort(MediaOperationPort):
    async def create_image(self, command: AgentImageCreateCommand) -> MediaCommandAccepted:
        raise AssertionError(f"unexpected call: {command.agent_media_command_ref}")


def _with_media(*, allowed: bool, authority: bool):
    value = request("run-media")
    updates: dict[str, object] = {
        "tools": [CREATE_IMAGE_TOOL_NAME] if allowed else [],
        "media": (
            MediaRuntimeGrant(
                media_access_handle="media-access:" + "a" * 48,
                media_projection_reservation_handle="projection-reservation:" + "b" * 48,
            )
            if authority
            else None
        ),
    }
    return value.model_copy(update={"runtime": value.runtime.model_copy(update=updates)})


def test_media_tool_is_absent_without_catalog_permission() -> None:
    assert media_tools_for_run(_with_media(allowed=False, authority=True), UnusedMediaPort()) == ()


def test_media_tool_is_absent_without_run_authority() -> None:
    with pytest.raises(ValueError, match="MEDIA_RUNTIME_GRANT_REQUIRED"):
        media_tools_for_run(_with_media(allowed=True, authority=False), UnusedMediaPort())


def test_media_tool_mounts_only_when_both_gates_are_present() -> None:
    tools = media_tools_for_run(_with_media(allowed=True, authority=True), UnusedMediaPort())
    assert [tool.name for tool in tools] == [CREATE_IMAGE_TOOL_NAME]


def test_authorized_media_tool_fails_loud_without_transport() -> None:
    with pytest.raises(ValueError, match="MEDIA_RUNTIME_TRANSPORT_REQUIRED"):
        media_tools_for_run(_with_media(allowed=True, authority=True), None)


def test_owner_journaled_create_replays_through_command_recovery() -> None:
    assert CREATE_IMAGE_TOOL_NAME in JOURNAL_EXEMPT_TOOLS


def test_media_process_transport_is_optional_but_never_partial() -> None:
    assert AppConfig.from_env({}).media_runtime is None
    with pytest.raises(ValueError, match="MEDIA_RUNTIME_MTLS_CONFIGURATION_INCOMPLETE"):
        _ = AppConfig.from_env({"KOKORO_MEDIA_RPC_URL": "https://media.internal"}).media_runtime
    settings = AppConfig.from_env(
        {
            "KOKORO_MEDIA_RPC_URL": "https://media.internal",
            "KOKORO_MEDIA_RPC_CA_FILE": "/pki/ca.pem",
            "KOKORO_MEDIA_RPC_CERT_FILE": "/pki/agent.pem",
            "KOKORO_MEDIA_RPC_KEY_FILE": "/pki/agent-key.pem",
        }
    ).media_runtime
    assert settings is not None
    assert settings.timeout_ms == 30_000
