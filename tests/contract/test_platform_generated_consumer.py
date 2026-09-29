from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import shutil
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[2]
PIN_PATH = ROOT / "contract" / "platform" / "v1" / "provenance.json"
EXPECTED_SOURCE_DIGESTS = {
    "contract/platform/v1/proto/kokoro/common/v1/common.proto": "65025b86a89119954bfbc7ad8eb89d59109ae7f390db5ee1a68f016eefa7da08",
    "contract/platform/v1/proto/kokoro/platform/v1/platform_runtime.proto": "282bf886ea9648f7ce5208abd36ab47d879b2002a036d90aada2af59e74b4020",
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
    assert pin["owner_commit"] == "5b6eb2c1532b23b9747bc4bf6ac99f69ad453de0"
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
    from kokoro_agent.generated import platform_request_projector
    from kokoro_agent.generated.kokoro.common.v1 import common_pb
    from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_connect
    from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb

    assert common_pb.ExecutionIdentity
    assert platform_runtime_pb.AuthorizeMcpToolRequest
    assert platform_runtime_connect.McpAuthorizationServiceClient
    assert len(platform_request_projector.FQ_METHOD_BY_OPERATION) == 24


@pytest.mark.parametrize(
    "stale_path",
    ["kokoro/platform/v1/legacy_platform_pb.py", "platform_request_projector_v1.py"],
)
def test_platform_codegen_check_rejects_stale_platform_output_but_keeps_storage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stale_path: str
) -> None:
    spec = importlib.util.spec_from_file_location(
        "generate_platform_consumer", ROOT / "scripts/generate_platform_consumer.py"
    )
    assert spec is not None and spec.loader is not None
    generate_platform_consumer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generate_platform_consumer)
    generated = ROOT / "src/kokoro_agent/generated"
    output = tmp_path / "output"
    candidate = tmp_path / "candidate"
    shutil.copytree(generated, output)
    for relative in (
        *generate_platform_consumer._PLATFORM_OUTPUTS,
        *generate_platform_consumer._SHARED_OUTPUTS,
    ):
        destination = candidate / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(generated / relative, destination)
    monkeypatch.setattr(generate_platform_consumer, "OUTPUT", output)

    # The shared generated tree also contains Storage; it is not Platform drift.
    assert (output / "kokoro/storage").is_dir()
    generate_platform_consumer._check_platform_outputs(candidate)

    stale = output / stale_path
    stale.write_text("# obsolete Platform output\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="generated Platform output inventory drift"):
        generate_platform_consumer._check_platform_outputs(candidate)
