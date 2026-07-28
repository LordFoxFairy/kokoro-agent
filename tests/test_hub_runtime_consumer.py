"""Child-owned live compatibility probe for Hub runtime secret resolution."""

from __future__ import annotations

import hashlib
import io
from pathlib import Path

import pytest

from kokoro_agent.mcp.secret_client import HubSecretResolver, SecretResolveError
from scripts.compat.hub_runtime_consumer import ProbeConfig, main, run_probe

_HANDLE = "srt_" + "a1b2c3d4" * 4
_PLAINTEXT = "compatibility-resolved-value-not-real"
_EXPECTED_SHA256 = hashlib.sha256(_PLAINTEXT.encode()).hexdigest()


async def test_run_probe_wraps_the_production_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    async def resolve(
        self: HubSecretResolver, namespace: str, handles: list[str]
    ) -> dict[str, str]:
        seen["resolver"] = self
        seen["namespace"] = namespace
        seen["handles"] = handles
        return {_HANDLE: _PLAINTEXT}

    monkeypatch.setattr(HubSecretResolver, "resolve", resolve)
    result = await run_probe(
        ProbeConfig(
            base_url="http://127.0.0.1:4251",
            namespace="namespace-compatibility",
            handle=_HANDLE,
            expected_sha256=_EXPECTED_SHA256,
        ),
        {"KOKORO_INTERNAL_SECRET_AGENT": "compatibility-caller-secret-not-real"},
    )

    assert isinstance(seen["resolver"], HubSecretResolver)
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
    async def resolve(
        self: HubSecretResolver, namespace: str, handles: list[str]
    ) -> dict[str, str]:
        return resolved

    monkeypatch.setattr(HubSecretResolver, "resolve", resolve)
    with pytest.raises(RuntimeError) as excinfo:
        await run_probe(
            ProbeConfig(
                base_url="http://127.0.0.1:4251",
                namespace="namespace-compatibility",
                handle=_HANDLE,
                expected_sha256=_EXPECTED_SHA256,
            ),
            {"KOKORO_INTERNAL_SECRET_AGENT": "compatibility-caller-secret-not-real"},
        )

    assert _PLAINTEXT not in str(excinfo.value)
    assert "wrong-resolved-value-not-real" not in str(excinfo.value)


def test_main_emits_one_closed_json_line_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    async def resolve(
        self: HubSecretResolver, namespace: str, handles: list[str]
    ) -> dict[str, str]:
        return {_HANDLE: _PLAINTEXT}

    monkeypatch.setattr(HubSecretResolver, "resolve", resolve)
    stdout = io.StringIO()
    stderr = io.StringIO()
    exit_code = main(
        [
            "--base-url",
            "http://127.0.0.1:4251",
            "--namespace",
            "namespace-compatibility",
            "--handle",
            _HANDLE,
            "--expected-sha256",
            _EXPECTED_SHA256,
        ],
        environ={"KOKORO_INTERNAL_SECRET_AGENT": "compatibility-caller-secret-not-real"},
        stdout=stdout,
        stderr=stderr,
    )

    assert exit_code == 0
    assert stdout.getvalue() == '{"schemaVersion":1,"resolvedHandles":1}\n'
    assert stderr.getvalue() == ""


def test_main_fails_closed_without_echoing_exception_or_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def resolve(
        self: HubSecretResolver, namespace: str, handles: list[str]
    ) -> dict[str, str]:
        raise SecretResolveError("response contained leak-me-resolved-value")

    monkeypatch.setattr(HubSecretResolver, "resolve", resolve)
    stdout = io.StringIO()
    stderr = io.StringIO()
    exit_code = main(
        [
            "--base-url",
            "http://127.0.0.1:4251",
            "--namespace",
            "namespace-compatibility",
            "--handle",
            _HANDLE,
            "--expected-sha256",
            _EXPECTED_SHA256,
        ],
        environ={"KOKORO_INTERNAL_SECRET_AGENT": "leak-me-caller-secret"},
        stdout=stdout,
        stderr=stderr,
    )

    assert exit_code == 1
    assert stdout.getvalue() == ""
    assert stderr.getvalue() == "hub_runtime_consumer_failed\n"
    assert "leak-me" not in stdout.getvalue() + stderr.getvalue()


def test_probe_delegates_the_wire_protocol_to_production_code() -> None:
    source = Path("scripts/compat/hub_runtime_consumer.py").read_text()

    assert "HubSecretResolver" in source
    assert "/hub/runtime/mcp/secrets/resolve" not in source
    assert "x-kokoro-service" not in source
    assert "x-kokoro-internal-secret" not in source
