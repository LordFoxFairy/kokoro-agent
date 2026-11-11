"""Current Platform bindings precede MCP I/O, without models or deployment commands."""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from kokoro_agent.agents.music import MUSIC_AGENT
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as p
from kokoro_agent.generated.kokoro.common.v1 import common_pb as c


def test_agent_requires_explicit_connection_references() -> None:
    from kokoro_agent.clients.mcp import McpConnectionRef

    ref = McpConnectionRef(connection_id="connection-1")
    assert replace(MUSIC_AGENT, mcp=(ref,)).mcp == (ref,)
    with pytest.raises(ValueError):
        replace(MUSIC_AGENT, mcp=("connection-1",))
    with pytest.raises(ValueError):
        McpConnectionRef(connection_id="https://endpoint.test/mcp")


class Owner:
    def __init__(self) -> None:
        self.requests: list[object] = []
        self.revoked = False
        self.wrong_connection = False

    async def send(self, request: object, *, timeout_s: float = 10) -> object:
        self.requests.append(request)
        if isinstance(request, p.GetMcpConnectionRequest):
            return p.GetMcpConnectionResponse(
                connection=p.McpConnection(
                    connection_id=p.McpConnectionId(
                        value="other" if self.wrong_connection else "connection-1"
                    ),
                    connector_id=p.McpConnectorId(value="connector-1"),
                    server_id=p.McpServerId(value="server-1"),
                    allowed_selectors=["echo"],
                    status="revoked" if self.revoked else "active",
                    policy_digest="b" * 64,
                )
            )
        if isinstance(request, p.GetMcpConnectorRequest):
            return p.GetMcpConnectorResponse(
                mcp_connector=p.McpConnector(
                    connector_id=p.McpConnectorId(value="connector-1"),
                    provider_key="owned",
                    status=p.McpConnectorStatus.ACTIVE,
                )
            )
        if isinstance(request, p.ListMcpServerDeclarationsRequest):
            assert request.page is not None and request.page.limit == 1
            return p.ListMcpServerDeclarationsResponse(
                servers=[
                    p.McpServer(
                        server_id=p.McpServerId(value="server-1"),
                        provider_key="owned",
                        transport=p.McpTransport.STREAMABLE_HTTP,
                        status=p.McpServerStatus.REGISTERED,
                        declaration_digest="a" * 64,
                    )
                ],
                capabilities=[
                    p.McpCapabilitySummary(
                        selector="echo",
                        kind=p.McpPrimitiveKind.TOOL,
                        display_name="Echo",
                        input_schema_digest=hashlib.sha256(b"{}").hexdigest(),
                    )
                ],
                page=c.PageResult(),
            )
        raise AssertionError("no runtime material or tool invocation expected")


async def test_current_connection_reader_rejects_wrong_target_and_revocation() -> None:
    from kokoro_agent.clients.mcp import McpConnectionRef, McpClientError
    from kokoro_agent.clients.mcp_runtime import PlatformMcpClient

    owner = Owner()
    client = PlatformMcpClient(owner, packages=None, run_id="run-1")
    ref = McpConnectionRef(connection_id="connection-1")
    await client.check((ref,))
    assert len(owner.requests) == 3
    owner.revoked = True
    with pytest.raises(McpClientError):
        await client.check((ref,))
    assert len(owner.requests) == 4
    owner.revoked = False
    owner.wrong_connection = True
    with pytest.raises(McpClientError):
        await client.check((ref,))
    assert len(owner.requests) == 5


async def test_unapproved_tool_scope_does_not_invalidate_other_tools() -> None:
    from kokoro_agent.clients.mcp import McpConnectionRef
    from kokoro_agent.clients.mcp_runtime import PlatformMcpClient

    class MixedScopeOwner(Owner):
        async def send(self, request: object, *, timeout_s: float = 10) -> object:
            response = await super().send(request, timeout_s=timeout_s)
            if (
                isinstance(response, p.GetMcpConnectionResponse)
                and response.connection is not None
            ):
                response.connection.allowed_selectors.append("blocked")
            if isinstance(response, p.ListMcpServerDeclarationsResponse):
                response.capabilities.append(
                    p.McpCapabilitySummary(
                        selector="blocked",
                        kind=p.McpPrimitiveKind.TOOL,
                        display_name="Blocked",
                        input_schema_digest="c" * 64,
                        required_scopes=["extra"],
                    )
                )
            return response

    owner = MixedScopeOwner()
    client = PlatformMcpClient(owner, packages=None, run_id="run")
    await client.check((McpConnectionRef(connection_id="connection-1"),))
    assert len(owner.requests) == 3


async def test_check_returns_only_current_owner_public_declarations_without_extra_io() -> (
    None
):
    from kokoro_agent.clients.mcp import McpConnectionRef
    from kokoro_agent.clients.mcp_runtime import PlatformMcpClient

    owner = Owner()
    declarations = await PlatformMcpClient(owner, packages=None, run_id="run").check(
        (McpConnectionRef(connection_id="connection-1"),)
    )
    assert declarations is not None
    assert [
        (item.connection_id, item.selector, item.display_name) for item in declarations
    ] == [("connection-1", "echo", "echo")]
    assert len(owner.requests) == 3


@pytest.mark.parametrize(
    "name", ["", "secret.txt", "e\u0301", "x\u202e", "x\ud800", "x" * 121]
)
async def test_check_omits_invalid_public_metadata_without_changing_authorization(
    name: str,
) -> None:
    from kokoro_agent.clients.mcp import McpConnectionRef
    from kokoro_agent.clients.mcp_runtime import PlatformMcpClient

    class BadNameOwner(Owner):
        async def send(self, request: object, *, timeout_s: float = 10) -> object:
            response = await super().send(request, timeout_s=timeout_s)
            if isinstance(response, p.ListMcpServerDeclarationsResponse):
                response.capabilities[0].display_name = name
            return response

    owner = BadNameOwner()
    client = PlatformMcpClient(owner, packages=None, run_id="r")
    refs = (McpConnectionRef(connection_id="connection-1"),)
    if "\ud800" in name:
        # Invalid UTF-8 is already a rejected owner transport, not valid metadata.
        from kokoro_agent.clients.mcp import McpClientError

        with pytest.raises(McpClientError):
            await client.check(refs)
    else:
        declarations = await client.check(refs)
        assert [(item.selector, item.display_name) for item in declarations] == [
            ("echo", "echo")
        ]
        assert all(item.display_name != name for item in declarations)
    assert len(owner.requests) == 3


async def test_real_public_selector_not_long_capability_description_is_the_tool_name() -> (
    None
):
    from kokoro_agent.clients.mcp import McpConnectionRef
    from kokoro_agent.clients.mcp_runtime import PlatformMcpClient

    class DescriptiveOwner(Owner):
        async def send(self, request: object, *, timeout_s: float = 10) -> object:
            response = await super().send(request, timeout_s=timeout_s)
            if isinstance(response, p.ListMcpServerDeclarationsResponse):
                response.capabilities[
                    0
                ].display_name = (
                    "Get a list of documentation topics for a GitHub repository."
                )
                response.capabilities[0].selector = "read_wiki_structure"
            if isinstance(response, p.GetMcpConnectionResponse):
                assert response.connection is not None
                response.connection.allowed_selectors = ["read_wiki_structure"]
            return response

    owner = DescriptiveOwner()
    declarations = await PlatformMcpClient(owner, packages=None, run_id="run").check(
        (McpConnectionRef(connection_id="connection-1"),)
    )
    assert [(item.selector, item.display_name) for item in declarations] == [
        ("read_wiki_structure", "read_wiki_structure")
    ]
    assert len(owner.requests) == 3
