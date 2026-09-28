from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path


ROOT = Path(__file__).parents[2]
PIN_PATH = ROOT / "contract" / "platform" / "v1" / "provenance.json"
EXPECTED_SOURCE_DIGESTS = {
    "contract/platform/v1/proto/kokoro/common/v1/common.proto": "65025b86a89119954bfbc7ad8eb89d59109ae7f390db5ee1a68f016eefa7da08",
    "contract/platform/v1/proto/kokoro/platform/v1/platform_runtime.proto": "7c55fcadf5ba0753ca5d1bb304ccb96bf0318a4fb5c37aae4f96781c4ea53466",
}
EXPECTED_RPC_SHAPES = {
    "SkillCatalogService": {
        "CreateSkillDraft": ("CreateSkillDraftRequest", "CreateSkillDraftResponse"),
        "CreateSkillVersion": (
            "CreateSkillVersionRequest",
            "CreateSkillVersionResponse",
        ),
        "ValidateSkillDraft": (
            "ValidateSkillDraftRequest",
            "ValidateSkillDraftResponse",
        ),
        "PublishSkill": ("PublishSkillRequest", "PublishSkillResponse"),
        "WithdrawSkill": ("WithdrawSkillRequest", "WithdrawSkillResponse"),
        "SetSkillStatus": ("SetSkillStatusRequest", "SetSkillStatusResponse"),
    },
    "SkillSourceService": {
        "DiscoverVisibleSkills": (
            "DiscoverVisibleSkillsRequest",
            "DiscoverVisibleSkillsResponse",
        ),
        "ResolveVisibleSkill": (
            "ResolveVisibleSkillRequest",
            "ResolveVisibleSkillResponse",
        ),
        "GetApprovedSkillPackageReference": (
            "GetApprovedSkillPackageReferenceRequest",
            "GetApprovedSkillPackageReferenceResponse",
        ),
    },
    "SkillInstallationService": {
        "InstallSkill": ("InstallSkillRequest", "InstallSkillResponse"),
        "SetSkillInstallationEnabled": (
            "SetSkillInstallationEnabledRequest",
            "SetSkillInstallationEnabledResponse",
        ),
        "RemoveSkillInstallation": (
            "RemoveSkillInstallationRequest",
            "RemoveSkillInstallationResponse",
        ),
        "GetSkillInstallation": (
            "GetSkillInstallationRequest",
            "GetSkillInstallationResponse",
        ),
        "ListSkillInstallations": (
            "ListSkillInstallationsRequest",
            "ListSkillInstallationsResponse",
        ),
    },
    "McpConnectorService": {
        "CreateMcpConnector": (
            "CreateMcpConnectorRequest",
            "CreateMcpConnectorResponse",
        ),
        "BeginMcpConnectorAuthorization": (
            "BeginMcpConnectorAuthorizationRequest",
            "BeginMcpConnectorAuthorizationResponse",
        ),
        "CompleteMcpConnectorAuthorization": (
            "CompleteMcpConnectorAuthorizationRequest",
            "CompleteMcpConnectorAuthorizationResponse",
        ),
        "ListMcpConnectors": ("ListMcpConnectorsRequest", "ListMcpConnectorsResponse"),
        "GetMcpConnector": ("GetMcpConnectorRequest", "GetMcpConnectorResponse"),
        "RevokeMcpConnector": (
            "RevokeMcpConnectorRequest",
            "RevokeMcpConnectorResponse",
        ),
    },
    "McpServerService": {
        "RegisterMcpServer": ("RegisterMcpServerRequest", "RegisterMcpServerResponse"),
        "GetMcpServer": ("GetMcpServerRequest", "GetMcpServerResponse"),
        "ListMcpServers": ("ListMcpServersRequest", "ListMcpServersResponse"),
    },
    "McpConnectionService": {
        "CreateMcpConnection": (
            "CreateMcpConnectionRequest",
            "CreateMcpConnectionResponse",
        ),
        "ListMcpServerDeclarations": (
            "ListMcpServerDeclarationsRequest",
            "ListMcpServerDeclarationsResponse",
        ),
        "GetMcpConnection": ("GetMcpConnectionRequest", "GetMcpConnectionResponse"),
        "ListMcpConnections": (
            "ListMcpConnectionsRequest",
            "ListMcpConnectionsResponse",
        ),
        "RevokeMcpConnection": (
            "RevokeMcpConnectionRequest",
            "RevokeMcpConnectionResponse",
        ),
    },
    "McpAuthorizationService": {
        "ListMcpConnectorCapabilities": (
            "ListMcpConnectorCapabilitiesRequest",
            "ListMcpConnectorCapabilitiesResponse",
        ),
        "AuthorizeMcpTool": ("AuthorizeMcpToolRequest", "AuthorizeMcpToolResponse"),
    },
    "McpConnectorProviderService": {
        "ListMcpConnectorProviders": (
            "ListMcpConnectorProvidersRequest",
            "ListMcpConnectorProvidersResponse",
        ),
    },
}


def test_platform_vendor_inputs_match_the_pinned_owner_digests() -> None:
    pin = json.loads(PIN_PATH.read_text(encoding="utf-8"))
    assert pin["owner_commit"] == "ee25c1f4d6df08be183ca10f7f5e852e0b21f641"
    assert {
        source["path"]: source["sha256"] for source in pin["sources"]
    } == EXPECTED_SOURCE_DIGESTS
    for path, expected in EXPECTED_SOURCE_DIGESTS.items():
        assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == expected


def test_platform_generated_async_client_contains_all_owner_rpcs() -> None:
    from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_connect

    methods = {
        name
        for name, value in vars(platform_runtime_connect).items()
        if name.endswith("ServiceClient") and isinstance(value, type)
        for name, value in vars(value).items()
        if callable(value) and not name.startswith("_") and name != "close"
    }
    assert len(methods) == 31


def test_pinned_platform_proto_matches_the_exact_owner_rpc_shapes() -> None:
    source = (
        ROOT / "contract/platform/v1/proto/kokoro/platform/v1/platform_runtime.proto"
    ).read_text(encoding="utf-8")
    actual: dict[str, dict[str, tuple[str, str]]] = {}
    service: str | None = None
    for line in source.splitlines():
        if match := re.fullmatch(r"service (\w+) \{", line):
            service_name = match.group(1)
            service = service_name
            actual[service_name] = {}
        elif match := re.fullmatch(
            r"  rpc (\w+)\(kokoro\.platform\.v1\.(\w+)\) returns "
            r"\(kokoro\.platform\.v1\.(\w+)\);",
            line,
        ):
            assert service is not None
            actual[service][match.group(1)] = (match.group(2), match.group(3))
    assert actual == EXPECTED_RPC_SHAPES


def test_platform_generated_messages_and_async_client_are_importable() -> None:
    from kokoro_agent.generated.kokoro.common.v1 import common_pb
    from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_connect
    from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb

    assert common_pb.ExecutionIdentity
    assert platform_runtime_pb.AuthorizeMcpToolRequest
    assert platform_runtime_connect.McpAuthorizationServiceClient
