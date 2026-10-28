from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from kokoro_agent.protocol import ExecutionIdentity, IdentityRef


def identity() -> ExecutionIdentity:
    return ExecutionIdentity(
        tenant_ref="tenant",
        subject=IdentityRef(kind="user", opaque_ref="user"),
        actor=IdentityRef(kind="user", opaque_ref="user"),
        identity_assertion_ref="assertion",
    )


@pytest.mark.asyncio
async def test_display_read_rejects_foreign_run_and_invalid_transport_before_checkpoint_io() -> (
    None
):
    from kokoro_agent.interfaces.http.run_display import display_resource

    repository: Any = SimpleNamespace(get_request_scoped=AsyncMock(return_value=None))
    reader = AsyncMock()
    from kokoro_agent.interfaces.http.ingress import IngressError

    with pytest.raises(IngressError) as caught:
        await display_resource("run", identity(), {}, {}, None, repository, reader)
    assert caught.value.status == 404
    reader.assert_not_called()
    assert repository.get_request_scoped.await_args.args[:2] == ("run", "tenant")
    for headers, query in [
        ({"content-length": "0"}, {}),
        ({"idempotency-key": "key"}, {}),
        ({}, {"x": ["1"]}),
        ({"transfer-encoding": "chunked"}, {}),
    ]:
        with pytest.raises(IngressError) as caught:
            await display_resource(
                "run", identity(), headers, query, None, repository, reader
            )
        assert caught.value.status == 400
    assert repository.get_request_scoped.await_count == 1


@pytest.mark.asyncio
async def test_real_display_dispatch_checks_framing_before_any_dependency_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.interfaces.http import server
    from pydantic import SecretStr

    config: Any = SimpleNamespace(
        internal_secret_agent=SecretStr("fixture"), run_repository=None
    )
    calls: list[str] = []

    def dependency(*_args: object, **_kwargs: object) -> Any:
        calls.append("dependency")
        raise AssertionError("invalid display request must not open dependencies")

    monkeypatch.setattr(server, "make_run_repository", dependency)
    headers = {
        "authorization": "Bearer fixture",
        "x-kokoro-tenant-ref": "tenant",
        "x-kokoro-subject-ref": "user",
        "x-kokoro-subject-kind": "user",
        "x-kokoro-actor-ref": "user",
        "x-kokoro-actor-kind": "user",
        "x-kokoro-identity-assertion-ref": "assertion",
        "content-length": "0",
    }
    status, payload = await server.dispatch_request(
        config, "GET", "/v1/runs/run/display", {}, headers, None
    )
    assert status == 400
    error: Any = payload["error"]
    assert error["code"] == "invalid_run_display_request"
    assert calls == []


def test_real_stdlib_host_preserves_absent_and_explicit_empty_display_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json
    import socket
    import threading
    from contextlib import asynccontextmanager
    from pydantic import SecretStr
    from kokoro_agent.interfaces.http import server
    from kokoro_agent.interfaces.http.execution_proof_jwks import (
        ExecutionProofJwksState,
    )
    from kokoro_agent.application.run_display import RunDisplay
    from support.fakes import request

    @asynccontextmanager
    async def repository(_settings: object):
        yield SimpleNamespace(
            get_request_scoped=AsyncMock(return_value=request("display-host"))
        )

    reader = AsyncMock(
        return_value=RunDisplay(run_id="display-host", display_skills=[], tool_paths=[])
    )
    monkeypatch.setattr(server, "make_run_repository", repository)
    monkeypatch.setattr(server, "read_run_display", reader)
    config: Any = SimpleNamespace(
        internal_secret_agent=SecretStr("fixture"),
        billing_model_call_reader_secret=None,
        run_repository=None,
        database_url="unused",
        database_schema="unused",
    )
    instance = server.create_http_server(
        config,
        "127.0.0.1",
        0,
        execution_proof_jwks=ExecutionProofJwksState(
            available=True,
            body=b'{"keys":[{"alg":"EdDSA","crv":"Ed25519","kid":"a","kty":"OKP","use":"sig","x":"11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"}]}',
        ),
    )
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    headers = [
        "Authorization: Bearer fixture",
        "X-Kokoro-Tenant-Ref: tenant",
        "X-Kokoro-Subject-Kind: user",
        "X-Kokoro-Subject-Ref: user",
        "X-Kokoro-Actor-Kind: user",
        "X-Kokoro-Actor-Ref: user",
        "X-Kokoro-Identity-Assertion-Ref: assertion",
    ]

    def get(
        extra: list[str], path: str = "/v1/runs/display-host/display", auth: bool = True
    ) -> tuple[int, Any]:
        lines = [
            f"GET {path} HTTP/1.1",
            "Host: fixture",
            "Connection: close",
            *(headers if auth else []),
            *extra,
            "",
            "",
        ]
        with socket.create_connection(
            (str(instance.server_address[0]), int(instance.server_address[1])),
            timeout=3,
        ) as conn:
            conn.sendall("\r\n".join(lines).encode("ascii"))
            chunks: list[bytes] = []
            while value := conn.recv(65536):
                chunks.append(value)
        head, body = b"".join(chunks).split(b"\r\n\r\n", 1)
        return int(head.split(b" ")[1]), json.loads(body)

    try:
        status, body = get([])
        assert status == 200
        assert body["data"]["run_id"] == "display-host"
        assert reader.await_count == 1
        for name in ["Content-Length", "Transfer-Encoding", "Idempotency-Key"]:
            for value in ["", "0"]:
                status, body = get([f"{name}: {value}"])
                assert status == 400
                assert body["error"]["code"] == "invalid_run_display_request"
        assert reader.await_count == 1
        assert get([], auth=False)[0] == 401
        assert get([], path="/v1/execution-proof/jwks", auth=False)[0] == 200
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive()
