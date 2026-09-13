"""Raw HTTP wire behavior for the anonymous execution-proof JWKS route."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import logging
import socket
import threading
from typing import Iterator

import pytest
from pydantic import SecretStr

from kokoro_agent.infrastructure.postgres_run_repository import RunRepositorySettings
from kokoro_agent.interfaces.http.execution_proof_jwks import ExecutionProofJwksState
from kokoro_agent.interfaces.http.server import create_http_server
import kokoro_agent.interfaces.http.server as http_server
from kokoro_agent.streams.factory import StreamSettings

BODY = b'{"keys":[{"alg":"EdDSA","crv":"Ed25519","kid":"a","kty":"OKP","use":"sig","x":"11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"}]}'


@dataclass(frozen=True)
class Config:
    stream: StreamSettings = StreamSettings(redis_url="redis://unused")
    run_repository: RunRepositorySettings = RunRepositorySettings(
        database_url="postgres://unused", schema_name="unused", lease_ttl_ms=1
    )
    database_url: str = "postgres://unused"
    database_schema: str = "unused"
    internal_secret_agent: SecretStr | None = SecretStr("secret")


@pytest.fixture
def server() -> Iterator[tuple[str, int]]:
    instance = create_http_server(
        Config(),
        "127.0.0.1",
        0,
        execution_proof_jwks=ExecutionProofJwksState(available=True, body=BODY),
    )
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    try:
        address = instance.server_address
        yield str(address[0]), int(address[1])
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


def _raw(server: tuple[str, int], request: bytes) -> bytes:
    with socket.create_connection(server, timeout=1) as connection:
        connection.settimeout(1)
        connection.sendall(request)
        connection.shutdown(socket.SHUT_WR)
        chunks: list[bytes] = []
        while True:
            try:
                chunk = connection.recv(65536)
            except TimeoutError:
                break
            if not chunk:
                break
            chunks.append(chunk)
    return b"".join(chunks)


def _parts(response: bytes) -> tuple[int, dict[str, str], bytes]:
    head, _, body = response.partition(b"\r\n\r\n")
    lines = head.decode("iso-8859-1").split("\r\n")
    headers = {
        name.lower(): value.strip()
        for name, value in (line.split(":", 1) for line in lines[1:] if ":" in line)
    }
    return int(lines[0].split()[1]), headers, body


def test_get_and_head_have_exact_representation_headers(
    server: tuple[str, int],
) -> None:
    get = _parts(
        _raw(
            server,
            b"GET /v1/execution-proof/jwks HTTP/1.1\r\nHost: local\r\nX-Request-Id: fixed\r\nConnection: close\r\n\r\n",
        )
    )
    head = _parts(
        _raw(
            server,
            b"HEAD /v1/execution-proof/jwks HTTP/1.1\r\nHost: local\r\nX-Request-Id: fixed\r\nConnection: close\r\n\r\n",
        )
    )
    assert get[0] == head[0] == 200
    assert get[1]["content-type"] == "application/jwk-set+json"
    assert get[1]["cache-control"] == "no-store"
    assert int(get[1]["content-length"]) == len(BODY)
    assert {
        key: get[1][key] for key in ("content-type", "cache-control", "content-length")
    } == {
        key: head[1][key] for key in ("content-type", "cache-control", "content-length")
    }
    assert get[2] == BODY
    assert head[2] == b""
    for forbidden in ("etag", "location"):
        assert forbidden not in get[1]


@pytest.mark.parametrize(
    "target", [b"/v1/execution-proof/jwks?", b"/v1/execution-proof/jwks?x=1"]
)
def test_any_raw_query_marker_is_invalid(
    server: tuple[str, int], target: bytes
) -> None:
    status, headers, body = _parts(
        _raw(
            server,
            b"GET " + target + b" HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
        )
    )
    assert status == 400
    assert headers["cache-control"] == "no-store"
    assert json.loads(body)["error"]["code"] == "execution_proof_jwks_invalid_request"


@pytest.mark.parametrize("length", [127, 128, 129])
def test_request_id_ascii_length_boundary(server: tuple[str, int], length: int) -> None:
    supplied = "r" * length
    _, _, body = _parts(
        _raw(
            server,
            b"GET /v1/execution-proof/jwks?bad HTTP/1.1\r\nHost: local\r\nX-Request-Id: "
            + supplied.encode("ascii")
            + b"\r\nConnection: close\r\n\r\n",
        )
    )
    actual = json.loads(body)["meta"]["request_id"]
    assert actual == supplied


@pytest.mark.parametrize("supplied", ["é", "tenant/request", "r" * 129])
def test_nonempty_request_id_is_trimmed_then_preserved(supplied: str) -> None:
    request_id = getattr(http_server, "_request_id")
    assert request_id({"x-request-id": f"  {supplied}  "}) == supplied


def test_request_id_str_subclass_keeps_head_trimmed_value() -> None:
    class Derived(str):
        pass

    request_id = getattr(http_server, "_request_id")
    assert request_id({"x-request-id": Derived("  apparently-valid  ")}) == (
        "apparently-valid"
    )


@pytest.mark.parametrize(
    "header",
    [
        b"Content-Length: -1",
        b"Content-Length: +0",
        b"Content-Length: 00",
        b"Content-Length: 0,0",
        b"Content-Length:",
        b"Content-Length: x",
        b"Content-Length: 0\r\nContent-Length: 0",
        b"Transfer-Encoding: chunked",
        b"Expect: 100-continue",
        b"X-Kokoro-Tenant-Ref:",
        b"X-Kokoro-Actor-Ref: actor",
        b"X-Kokoro-Subject-Kind: user",
    ],
)
def test_invalid_framing_and_identity_presence_is_400_without_100(
    server: tuple[str, int], header: bytes
) -> None:
    response = _raw(
        server,
        b"GET /v1/execution-proof/jwks HTTP/1.1\r\nHost: local\r\n"
        + header
        + b"\r\nConnection: close\r\n\r\n",
    )
    assert b"100 Continue" not in response
    status, _, body = _parts(response)
    assert status == 400
    assert json.loads(body)["error"]["code"] == "execution_proof_jwks_invalid_request"


@pytest.mark.parametrize(
    "header",
    [
        b"X-Kokoro-Tenant-Ref:",
        b"X-Kokoro-Subject-Ref:",
        b"X-Kokoro-Actor-Ref:",
        b"X-Kokoro-Subject-Kind:",
        b"X-Kokoro-Actor-Kind:",
        b"X-Kokoro-Identity-Assertion-Ref:",
    ],
)
def test_each_identity_header_presence_is_rejected(
    server: tuple[str, int], header: bytes
) -> None:
    status, _, body = _parts(
        _raw(
            server,
            b"GET /v1/execution-proof/jwks HTTP/1.1\r\nHost: local\r\n"
            + header
            + b"\r\nConnection: close\r\n\r\n",
        )
    )
    assert status == 400
    assert json.loads(body)["error"]["code"] == "execution_proof_jwks_invalid_request"


def test_exact_content_length_zero_and_authorization_are_ignored(
    server: tuple[str, int],
) -> None:
    response = _parts(
        _raw(
            server,
            b"GET /v1/execution-proof/jwks HTTP/1.1\r\nHost: local\r\nContent-Length: \t0 \t\r\nAuthorization: Bearer wrong\r\nIf-None-Match: anything\r\nConnection: close\r\n\r\n",
        )
    )
    assert response[0] == 200
    assert response[2] == BODY


@pytest.mark.parametrize(
    "method", [b"POST", b"PUT", b"PATCH", b"DELETE", b"OPTIONS", b"TRACE", b"BREW"]
)
def test_all_other_methods_return_405_without_reading_declared_body(
    server: tuple[str, int], method: bytes
) -> None:
    response = _raw(
        server,
        method
        + b" /v1/execution-proof/jwks?bad HTTP/1.1\r\nHost: local\r\nContent-Length: 999999\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
    )
    assert b"100 Continue" not in response
    status, headers, body = _parts(response)
    assert status == 405
    assert headers["allow"] == "GET, HEAD"
    assert headers["cache-control"] == "no-store"
    assert int(headers["content-length"]) == len(body)
    assert len(body) <= 65_536
    assert (
        json.loads(body)["error"]["code"] == "execution_proof_jwks_method_not_allowed"
    )


def test_unavailable_ring_is_stable_503() -> None:
    instance = create_http_server(
        Config(),
        "127.0.0.1",
        0,
        execution_proof_jwks=ExecutionProofJwksState.unavailable(),
    )
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    try:
        response = _parts(
            _raw(
                (str(instance.server_address[0]), int(instance.server_address[1])),
                b"GET /v1/execution-proof/jwks HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
            )
        )
        assert response[0] == 503
        assert (
            json.loads(response[2])["error"]["code"]
            == "execution_proof_jwks_unavailable"
        )
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("body", [b"{}", BODY + b" ", b"x" * 65_537])
def test_mutated_state_injection_is_revalidated_and_degrades_to_503(
    body: bytes,
) -> None:
    state = ExecutionProofJwksState(available=True, body=BODY)
    object.__setattr__(state, "body", body)
    instance = create_http_server(Config(), "127.0.0.1", 0, execution_proof_jwks=state)
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    try:
        status, _, response_body = _parts(
            _raw(
                (str(instance.server_address[0]), int(instance.server_address[1])),
                b"GET /v1/execution-proof/jwks HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
            )
        )
        assert status == 503
        assert (
            json.loads(response_body)["error"]["code"]
            == "execution_proof_jwks_unavailable"
        )
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("invalid", [True, False])
def test_get_head_error_parity_and_head_zero_body(invalid: bool) -> None:
    state = (
        ExecutionProofJwksState(available=True, body=BODY)
        if invalid
        else ExecutionProofJwksState.unavailable()
    )
    instance = create_http_server(Config(), "127.0.0.1", 0, execution_proof_jwks=state)
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    try:
        address = (str(instance.server_address[0]), int(instance.server_address[1]))
        suffix = b"?bad" if invalid else b""
        request_id = b"X-Request-Id: parity\r\n"
        get = _parts(
            _raw(
                address,
                b"GET /v1/execution-proof/jwks"
                + suffix
                + b" HTTP/1.1\r\nHost: local\r\n"
                + request_id
                + b"Connection: close\r\n\r\n",
            )
        )
        head = _parts(
            _raw(
                address,
                b"HEAD /v1/execution-proof/jwks"
                + suffix
                + b" HTTP/1.1\r\nHost: local\r\n"
                + request_id
                + b"Connection: close\r\n\r\n",
            )
        )
        assert get[0] == head[0] == (400 if invalid else 503)
        assert json.loads(get[2])["error"]["code"] == (
            "execution_proof_jwks_invalid_request"
            if invalid
            else "execution_proof_jwks_unavailable"
        )
        assert int(get[1]["content-length"]) == len(get[2])
        assert len(get[2]) <= 65_536
        assert {
            k: get[1][k] for k in ("content-type", "cache-control", "content-length")
        } == {
            k: head[1][k] for k in ("content-type", "cache-control", "content-length")
        }
        assert head[2] == b""
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


def test_legal_jwks_bypasses_bearer_identity_and_dependency_factories(
    server: tuple[str, int], monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"config": 0, "identity": 0, "redis": 0, "run": 0, "chat": 0}

    def bomb(name: str):
        def fail(*_args: object, **_kwargs: object) -> object:
            calls[name] += 1
            raise AssertionError(name)

        return fail

    monkeypatch.setattr(http_server.AgentRequestHandler, "_config", bomb("config"))
    monkeypatch.setattr(http_server, "_identity", bomb("identity"))
    monkeypatch.setattr(http_server, "make_stream", bomb("redis"))
    monkeypatch.setattr(http_server, "make_run_repository", bomb("run"))
    monkeypatch.setattr(http_server, "make_chat_repository", bomb("chat"))
    status, _, body = _parts(
        _raw(
            server,
            b"GET /v1/execution-proof/jwks HTTP/1.1\r\nHost: local\r\nAuthorization: Bearer wrong\r\nConnection: close\r\n\r\n",
        )
    )
    assert status == 200 and body == BODY
    assert calls == {"config": 0, "identity": 0, "redis": 0, "run": 0, "chat": 0}


def test_unavailable_ring_does_not_affect_raw_health_and_health_does_not_read_ring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance = create_http_server(
        Config(),
        "127.0.0.1",
        0,
        execution_proof_jwks=ExecutionProofJwksState.unavailable(),
    )

    def bomb_ring(_self: object) -> ExecutionProofJwksState:
        raise AssertionError("ring read")

    monkeypatch.setattr(http_server.AgentRequestHandler, "_jwks_state", bomb_ring)
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    try:
        address = (str(instance.server_address[0]), int(instance.server_address[1]))
        status, headers, body = _parts(
            _raw(
                address,
                b"GET /healthz HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
            )
        )
        assert status == 200
        assert headers["cache-control"] == "no-store"
        assert json.loads(body)["status"] == "ok"
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("method", [b"GET", b"POST"])
def test_unknown_route_requires_configured_auth_before_404(method: bytes) -> None:
    instance = create_http_server(
        Config(internal_secret_agent=None),
        "127.0.0.1",
        0,
        execution_proof_jwks=ExecutionProofJwksState(available=True, body=BODY),
    )
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    try:
        status, _, body = _parts(
            _raw(
                (str(instance.server_address[0]), int(instance.server_address[1])),
                method
                + b" /v1/execution-proof/jwks-near HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
            )
        )
        assert status == 503
        assert json.loads(body)["error"]["code"] == "service_auth_not_configured"
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("method", [b"GET", b"POST"])
@pytest.mark.parametrize("authorization", [b"", b"Authorization: Bearer wrong\r\n"])
def test_unknown_route_requires_valid_bearer_before_404(
    server: tuple[str, int], method: bytes, authorization: bytes
) -> None:
    status, _, body = _parts(
        _raw(
            server,
            method
            + b" /v1/unknown HTTP/1.1\r\nHost: local\r\n"
            + authorization
            + b"Connection: close\r\n\r\n",
        )
    )
    assert status == 401
    assert json.loads(body)["error"]["code"] == "service_auth_failed"


@pytest.mark.parametrize("method", [b"GET", b"POST"])
def test_unknown_v1_route_requires_identity_before_404(
    server: tuple[str, int], method: bytes
) -> None:
    status, _, body = _parts(
        _raw(
            server,
            method
            + b" /v1/unknown HTTP/1.1\r\nHost: local\r\nAuthorization: Bearer secret\r\nConnection: close\r\n\r\n",
        )
    )
    assert status == 401
    assert json.loads(body)["error"]["code"] == "identity_required"


def test_unknown_route_auth_precedence_does_not_read_jwks_ring(
    server: tuple[str, int], monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0

    def bomb_ring(_self: object) -> ExecutionProofJwksState:
        nonlocal calls
        calls += 1
        raise AssertionError("ring must not be read for an unknown route")

    monkeypatch.setattr(http_server.AgentRequestHandler, "_jwks_state", bomb_ring)
    status, _, body = _parts(
        _raw(
            server,
            b"GET /v1/unknown HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
        )
    )
    assert status == 401
    assert json.loads(body)["error"]["code"] == "service_auth_failed"
    assert calls == 0


def test_access_log_uses_safe_route_class_without_raw_target_or_host(
    server: tuple[str, int], caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO):
        status, _, _ = _parts(
            _raw(
                server,
                b"GET /v1/unknown?QUERY_SENTINEL HTTP/1.1\r\n"
                b"Host: HOST_SENTINEL\r\nX-Request-Id: safe-request\r\n"
                b"Connection: close\r\n\r\n",
            )
        )
    assert status == 401
    assert "QUERY_SENTINEL" not in caplog.text
    assert "HOST_SENTINEL" not in caplog.text
    for expected in ("method", "route_class", "status", "safe-request"):
        assert expected in caplog.text


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (b"", None),
        (b"X-Request-Id: valid.request-1\r\n", "valid.request-1"),
    ],
)
def test_response_and_access_log_reuse_one_normalized_request_id(
    server: tuple[str, int],
    caplog: pytest.LogCaptureFixture,
    header: bytes,
    expected: str | None,
) -> None:
    with caplog.at_level(logging.INFO):
        _, _, body = _parts(
            _raw(
                server,
                b"GET /v1/execution-proof/jwks?bad HTTP/1.1\r\nHost: local\r\n"
                + header
                + b"Connection: close\r\n\r\n",
            )
        )
    response_id = json.loads(body)["meta"]["request_id"]
    access = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("agent_http_access ")
    ]
    assert len(access) == 1
    assert f"request_id={response_id}" in access[0]
    assert (
        response_id == expected
        if expected is not None
        else response_id.startswith("req_agent_")
    )


@pytest.mark.parametrize("supplied", ["tenant/request", "é", "r" * 129])
def test_unsafe_request_id_is_preserved_in_response_but_hashed_in_access_log(
    server: tuple[str, int],
    caplog: pytest.LogCaptureFixture,
    supplied: str,
) -> None:
    with caplog.at_level(logging.INFO):
        _, _, body = _parts(
            _raw(
                server,
                b"GET /v1/execution-proof/jwks?bad HTTP/1.1\r\nHost: local\r\n"
                + b"X-Request-Id: "
                + supplied.encode("iso-8859-1")
                + b"\r\nConnection: close\r\n\r\n",
            )
        )
    assert json.loads(body)["meta"]["request_id"] == supplied
    access = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("agent_http_access ")
    ]
    assert len(access) == 1
    reference = hashlib.sha256(supplied.encode("utf-8")).hexdigest()
    assert f"request_id=sha256:{reference}" in access[0]
    assert supplied not in access[0]


@pytest.mark.parametrize(
    ("method", "logged_method"),
    [
        (b"CUSTOM_METHOD_SENTINEL", "OTHER"),
        (b"X" * 200, "OTHER"),
        (b"PUT", "PUT"),
    ],
)
def test_access_log_classifies_method_without_logging_custom_token(
    server: tuple[str, int],
    caplog: pytest.LogCaptureFixture,
    method: bytes,
    logged_method: str,
) -> None:
    caplog.clear()
    with caplog.at_level(logging.INFO):
        _raw(
            server,
            method
            + b" /v1/unknown HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
        )
    access = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("agent_http_access ")
    ]
    assert len(access) == 1
    assert f"method={logged_method}" in access[0]
    assert "CUSTOM_METHOD_SENTINEL" not in access[0]
    assert "route_class=unknown" in access[0]


@pytest.mark.parametrize("method", [b"GET", b"POST"])
def test_authenticated_identified_unknown_route_is_404_before_dependencies(
    server: tuple[str, int], monkeypatch: pytest.MonkeyPatch, method: bytes
) -> None:
    calls = {"redis": 0, "run": 0, "chat": 0}

    def bomb(name: str):
        def fail(*_args: object, **_kwargs: object) -> object:
            calls[name] += 1
            raise AssertionError(name)

        return fail

    monkeypatch.setattr(http_server, "make_stream", bomb("redis"))
    monkeypatch.setattr(http_server, "make_run_repository", bomb("run"))
    monkeypatch.setattr(http_server, "make_chat_repository", bomb("chat"))
    status, headers, body = _parts(
        _raw(
            server,
            method + b" /v1/unknown HTTP/1.1\r\nHost: local\r\n"
            b"Authorization: Bearer secret\r\n"
            b"X-Kokoro-Tenant-Ref: tenant\r\n"
            b"X-Kokoro-Subject-Ref: subject\r\n"
            b"X-Kokoro-Actor-Ref: actor\r\n"
            b"X-Kokoro-Subject-Kind: user\r\n"
            b"X-Kokoro-Actor-Kind: service\r\n"
            b"X-Kokoro-Identity-Assertion-Ref: assertion\r\n"
            b"Connection: close\r\n\r\n",
        )
    )
    assert status == 404
    assert headers["content-type"] == "application/json; charset=utf-8"
    assert headers["cache-control"] == "no-store"
    assert int(headers["content-length"]) == len(body)
    assert json.loads(body)["error"]["code"] == "route_not_found"
    assert calls == {"redis": 0, "run": 0, "chat": 0}
