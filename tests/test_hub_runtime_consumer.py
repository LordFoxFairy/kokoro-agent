"""Child-owned live compatibility probe for Hub runtime secret resolution."""

from __future__ import annotations

import hashlib
import io
from pathlib import Path

import pytest

import scripts.compat.hub_runtime_consumer as consumer
from kokoro_agent.mcp.secret_client import HubSecretSettings, SecretResolveError
from scripts.compat.hub_runtime_consumer import ProbeConfig, main, run_probe

_HANDLE = "srt_" + "a1b2c3d4" * 4
_PLAINTEXT = "compatibility-resolved-value-not-real"
_EXPECTED_SHA256 = hashlib.sha256(_PLAINTEXT.encode()).hexdigest()


def _config() -> ProbeConfig:
    return ProbeConfig(
        rpc_url="https://hub.internal:4251",
        server_name="hub.internal",
        ca_file="/compat/ca.pem",
        cert_file="/compat/agent.pem",
        key_file="/compat/agent-key.pem",
        namespace="namespace-compatibility",
        handle=_HANDLE,
        expected_sha256=_EXPECTED_SHA256,
        timeout_ms=1_200,
    )


def _probe_args() -> list[str]:
    config = _config()
    return [
        "--rpc-url",
        config.rpc_url,
        "--server-name",
        config.server_name,
        "--ca-file",
        config.ca_file,
        "--cert-file",
        config.cert_file,
        "--key-file",
        config.key_file,
        "--namespace",
        config.namespace,
        "--handle",
        config.handle,
        "--expected-sha256",
        config.expected_sha256,
        "--timeout-ms",
        str(config.timeout_ms),
    ]


async def test_run_probe_wraps_the_production_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    class FakeResolver:
        def __init__(self, settings: HubSecretSettings) -> None:
            seen["settings"] = settings
            seen["resolver"] = self

        async def resolve(self, namespace: str, handles: list[str]) -> dict[str, str]:
            seen["namespace"] = namespace
            seen["handles"] = handles
            return {_HANDLE: _PLAINTEXT}

    monkeypatch.setattr(consumer, "HubSecretResolver", FakeResolver)
    result = await run_probe(_config())

    assert isinstance(seen["resolver"], FakeResolver)
    assert seen["settings"] == HubSecretSettings(
        rpc_url="https://hub.internal:4251",
        server_name="hub.internal",
        ca_file="/compat/ca.pem",
        cert_file="/compat/agent.pem",
        key_file="/compat/agent-key.pem",
        timeout_ms=1_200,
    )
    assert seen["namespace"] == "namespace-compatibility"
    assert seen["handles"] == [_HANDLE]
    assert result == {"schemaVersion": 1, "resolvedHandles": 1}


@pytest.mark.parametrize(
    "resolved",
    [
        {},
        {_HANDLE: _PLAINTEXT, "srt_unrequested": "other"},
        {_HANDLE: "wrong-resolved-value-not-real"},
    ],
)
async def test_run_probe_rejects_missing_extra_or_mismatched_results_without_leaking_values(
    monkeypatch: pytest.MonkeyPatch, resolved: dict[str, str]
) -> None:
    class FakeResolver:
        def __init__(self, settings: HubSecretSettings) -> None:
            self.settings = settings

        async def resolve(self, namespace: str, handles: list[str]) -> dict[str, str]:
            return resolved

    monkeypatch.setattr(consumer, "HubSecretResolver", FakeResolver)
    with pytest.raises(RuntimeError) as excinfo:
        await run_probe(_config())

    assert _PLAINTEXT not in str(excinfo.value)
    assert "wrong-resolved-value-not-real" not in str(excinfo.value)


def test_main_emits_one_closed_json_line_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResolver:
        def __init__(self, settings: HubSecretSettings) -> None:
            self.settings = settings

        async def resolve(self, namespace: str, handles: list[str]) -> dict[str, str]:
            return {_HANDLE: _PLAINTEXT}

    monkeypatch.setattr(consumer, "HubSecretResolver", FakeResolver)
    stdout = io.StringIO()
    stderr = io.StringIO()
    exit_code = main(_probe_args(), stdout=stdout, stderr=stderr)

    assert exit_code == 0
    assert stdout.getvalue() == '{"schemaVersion":1,"resolvedHandles":1}\n'
    assert stderr.getvalue() == ""


def test_main_fails_closed_without_echoing_exception_or_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeResolver:
        def __init__(self, settings: HubSecretSettings) -> None:
            self.settings = settings

        async def resolve(self, namespace: str, handles: list[str]) -> dict[str, str]:
            raise SecretResolveError("response contained leak-me-resolved-value")

    monkeypatch.setattr(consumer, "HubSecretResolver", FakeResolver)
    stdout = io.StringIO()
    stderr = io.StringIO()
    exit_code = main(_probe_args(), stdout=stdout, stderr=stderr)

    assert exit_code == 1
    assert stdout.getvalue() == ""
    assert stderr.getvalue() == "hub_runtime_consumer_failed\n"
    assert "leak-me" not in stdout.getvalue() + stderr.getvalue()


def test_probe_delegates_wire_protocol_to_production_code_without_legacy_credentials() -> None:
    source = Path("scripts/compat/hub_runtime_consumer.py").read_text()

    assert "HubSecretResolver" in source
    assert "HubSecretSettings" in source
    assert "--server-name" in source
    assert "--ca-file" in source
    assert "--cert-file" in source
    assert "--key-file" in source
    assert "/hub/" + "runtime/mcp/secrets/resolve" not in source
    assert "x-kokoro-service" not in source
    assert "x-kokoro-internal-secret" not in source
    assert "KOKORO_" + "INTERNAL_SECRET_AGENT" not in source
