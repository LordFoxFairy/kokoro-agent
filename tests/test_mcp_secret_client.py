"""Agent-only Hub ConnectRPC secret consumer: exactness, bounds and fail-closed trust config."""

from __future__ import annotations

from pathlib import Path

import pytest

import kokoro_agent.mcp.secret_client as secret_client
from kokoro.platform.capability.v1 import capability_catalog_pb2 as capability_pb
from kokoro_agent.mcp.secret_client import (
    HubSecretResolver,
    HubSecretSettings,
    SecretResolveError,
    hub_secret_resolver_from_env,
)

_HANDLE_A = "srt_" + "a1b2c3d4" * 4
_HANDLE_B = "srt_" + "12345678" * 4


class FakeHubRuntimeClient:
    def __init__(
        self,
        response: capability_pb.ResolveMcpSecretsResponse | None = None,
        error: Exception | None = None,
    ) -> None:
        self.response = response or capability_pb.ResolveMcpSecretsResponse()
        self.error = error
        self.calls: list[tuple[capability_pb.ResolveMcpSecretsRequest, int | None]] = []

    async def resolve_mcp_secrets(
        self,
        request: capability_pb.ResolveMcpSecretsRequest,
        *,
        timeout_ms: int | None = None,
    ) -> capability_pb.ResolveMcpSecretsResponse:
        self.calls.append((request, timeout_ms))
        if self.error is not None:
            raise self.error
        return self.response


def _settings() -> HubSecretSettings:
    return HubSecretSettings(
        rpc_url="https://hub.internal:4251",
        server_name="hub.internal",
        ca_file="/not-read-with-injected-client/ca.pem",
        cert_file="/not-read-with-injected-client/agent.pem",
        key_file="/not-read-with-injected-client/agent-key.pem",
        timeout_ms=731,
    )


def _resolver(client: FakeHubRuntimeClient) -> HubSecretResolver:
    return HubSecretResolver(_settings(), client=client)


async def test_sends_exact_namespace_handles_and_deadline() -> None:
    client = FakeHubRuntimeClient(
        capability_pb.ResolveMcpSecretsResponse(
            secrets=[
                capability_pb.McpSecretMaterial(handle=_HANDLE_A, value="Bearer first"),
                capability_pb.McpSecretMaterial(handle=_HANDLE_B, value="Bearer second"),
            ]
        )
    )

    resolved = await _resolver(client).resolve("namespace-exact", [_HANDLE_A, _HANDLE_B])

    assert resolved == {_HANDLE_A: "Bearer first", _HANDLE_B: "Bearer second"}
    assert len(client.calls) == 1
    request, timeout_ms = client.calls[0]
    assert request.namespace == "namespace-exact"
    assert list(request.handles) == [_HANDLE_A, _HANDLE_B]
    assert timeout_ms == 731


@pytest.mark.parametrize(
    "materials",
    [
        [],
        [capability_pb.McpSecretMaterial(handle=_HANDLE_B, value="extra")],
        [
            capability_pb.McpSecretMaterial(handle=_HANDLE_A, value="one"),
            capability_pb.McpSecretMaterial(handle=_HANDLE_A, value="two"),
        ],
        [capability_pb.McpSecretMaterial(handle="malformed", value="secret")],
        [capability_pb.McpSecretMaterial(handle=_HANDLE_A, value="")],
        [capability_pb.McpSecretMaterial(handle=_HANDLE_A, value="界" * 2731)],
    ],
)
async def test_rejects_non_exact_or_unbounded_response_without_leaking(
    materials: list[capability_pb.McpSecretMaterial],
) -> None:
    client = FakeHubRuntimeClient(capability_pb.ResolveMcpSecretsResponse(secrets=materials))
    with pytest.raises(SecretResolveError) as excinfo:
        await _resolver(client).resolve("namespace-exact", [_HANDLE_A])
    assert str(excinfo.value) == "HUB_SECRET_RESOLVE_RESPONSE_INVALID"


async def test_transport_failure_has_stable_non_leaking_error() -> None:
    client = FakeHubRuntimeClient(error=RuntimeError("leak-me-response-body-and-secret"))
    with pytest.raises(SecretResolveError) as excinfo:
        await _resolver(client).resolve("namespace-exact", [_HANDLE_A])
    assert str(excinfo.value) == "HUB_SECRET_RESOLVE_FAILED"
    assert "leak-me" not in str(excinfo.value)


@pytest.mark.parametrize(
    ("namespace", "handles"),
    [
        ("", [_HANDLE_A]),
        (" namespace", [_HANDLE_A]),
        ("n" * 257, [_HANDLE_A]),
        ("namespace", [_HANDLE_A, _HANDLE_A]),
        ("namespace", ["not-a-handle"]),
        ("namespace", [_HANDLE_A] * 129),
    ],
)
async def test_invalid_request_is_rejected_before_rpc(
    namespace: str, handles: list[str]
) -> None:
    client = FakeHubRuntimeClient()
    with pytest.raises(SecretResolveError, match="^HUB_SECRET_RESOLVE_REQUEST_INVALID$"):
        await _resolver(client).resolve(namespace, handles)
    assert client.calls == []


async def test_empty_handles_makes_no_rpc() -> None:
    client = FakeHubRuntimeClient()
    assert await _resolver(client).resolve("namespace", []) == {}
    assert client.calls == []


def test_rpc_identity_requires_https_origin_and_exact_server_name() -> None:
    for rpc_url, server_name in [
        ("http://hub.internal:4251", "hub.internal"),
        ("https://hub.internal:4251/path", "hub.internal"),
        ("https://user@hub.internal:4251", "hub.internal"),
        ("https://hub.internal:4251", "other.internal"),
    ]:
        with pytest.raises(ValueError, match="^HUB_SECRET_RPC_IDENTITY_INVALID$"):
            HubSecretResolver(_settings().model_copy(update={"rpc_url": rpc_url, "server_name": server_name}), client=FakeHubRuntimeClient())


def test_private_key_file_must_be_private_and_not_a_symlink(tmp_path: Path) -> None:
    ca = tmp_path / "ca.pem"
    cert = tmp_path / "agent.pem"
    key = tmp_path / "agent-key.pem"
    ca.write_text("-----BEGIN CERTIFICATE-----\nnot-real\n-----END CERTIFICATE-----\n")
    cert.write_text("-----BEGIN CERTIFICATE-----\nnot-real\n-----END CERTIFICATE-----\n")
    key.write_text("-----BEGIN PRIVATE KEY-----\nnot-real\n-----END PRIVATE KEY-----\n")
    settings = _settings().model_copy(
        update={"ca_file": str(ca), "cert_file": str(cert), "key_file": str(key)}
    )
    key.chmod(0o644)
    with pytest.raises(ValueError, match="^HUB_SECRET_TLS_KEY_INVALID$"):
        HubSecretResolver(settings)
    key.chmod(0o600)

    symlink = tmp_path / "key-link.pem"
    symlink.symlink_to(key)
    with pytest.raises(ValueError, match="^HUB_SECRET_TLS_KEY_INVALID$"):
        HubSecretResolver(settings.model_copy(update={"key_file": str(symlink)}))


def test_env_factory_is_all_or_none_and_uses_only_mtls_rpc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert hub_secret_resolver_from_env({}) is None
    with pytest.raises(ValueError, match="^HUB_SECRET_MTLS_CONFIGURATION_INCOMPLETE$"):
        hub_secret_resolver_from_env({"KOKORO_HUB_RPC_URL": "https://hub.internal:4251"})

    captured: list[HubSecretSettings] = []

    def build(settings: HubSecretSettings) -> object:
        captured.append(settings)
        return object()

    monkeypatch.setattr(secret_client, "HubSecretResolver", build)
    result = hub_secret_resolver_from_env(
        {
            "KOKORO_HUB_RPC_URL": "https://hub.internal:4251",
            "KOKORO_HUB_RPC_SERVER_NAME": "hub.internal",
            "KOKORO_HUB_RPC_CA_FILE": "/pki/ca.pem",
            "KOKORO_HUB_RPC_CERT_FILE": "/pki/agent.pem",
            "KOKORO_HUB_RPC_KEY_FILE": "/pki/agent-key.pem",
            "KOKORO_HUB_RPC_TIMEOUT_MS": "1200",
        }
    )
    assert result is not None
    assert captured == [
        HubSecretSettings(
            rpc_url="https://hub.internal:4251",
            server_name="hub.internal",
            ca_file="/pki/ca.pem",
            cert_file="/pki/agent.pem",
            key_file="/pki/agent-key.pem",
            timeout_ms=1200,
        )
    ]


def test_env_factory_rejects_invalid_timeout_before_network() -> None:
    env = {
        "KOKORO_HUB_RPC_URL": "https://hub.internal:4251",
        "KOKORO_HUB_RPC_SERVER_NAME": "hub.internal",
        "KOKORO_HUB_RPC_CA_FILE": "/pki/ca.pem",
        "KOKORO_HUB_RPC_CERT_FILE": "/pki/agent.pem",
        "KOKORO_HUB_RPC_KEY_FILE": "/pki/agent-key.pem",
        "KOKORO_HUB_RPC_TIMEOUT_MS": "not-an-int",
    }
    with pytest.raises(ValueError, match="^HUB_SECRET_TIMEOUT_INVALID$"):
        hub_secret_resolver_from_env(env)
