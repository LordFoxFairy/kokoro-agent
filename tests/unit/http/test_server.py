"""HTTP host authentication behavior."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time
from typing import Never

import pytest
from pydantic import SecretStr

from kokoro_agent.config import AppConfig
from kokoro_agent.infrastructure.postgres_run_repository import RunRepositorySettings
from kokoro_agent.interfaces.http.execution_proof_jwks import ExecutionProofJwksState
import kokoro_agent.interfaces.http.server as http_server
from kokoro_agent.interfaces.http.server import create_http_server, dispatch_request
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


def _raw(server: tuple[str, int], request: bytes) -> bytes:
    with socket.create_connection(server, timeout=1) as connection:
        connection.settimeout(1)
        connection.sendall(request)
        connection.shutdown(socket.SHUT_WR)
        chunks: list[bytes] = []
        while True:
            try:
                chunk = connection.recv(65_536)
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", "/v1/runs"), ("GET", "/readyz")],
)
async def test_non_health_requests_reject_missing_service_auth_configuration(
    method: str, path: str
) -> None:
    status, payload = await dispatch_request(
        AppConfig(), method, path, {}, {"x-request-id": "request-1"}, {}
    )

    assert status == 503
    assert payload == {
        "error": {
            "code": "service_auth_not_configured",
            "message": "Agent ingress service authentication is not configured",
        },
        "meta": {"request_id": "request-1"},
    }


@pytest.mark.asyncio
async def test_healthz_remains_available_without_service_auth_configuration() -> None:
    status, payload = await dispatch_request(
        AppConfig(), "GET", "/healthz", {}, {}, None
    )

    assert status == 200
    assert payload == {"status": "ok", "service": "kokoro-agent"}


@pytest.mark.asyncio
async def test_standard_authorization_is_case_insensitive_and_request_id_is_preserved() -> (
    None
):
    config = AppConfig(internal_secret_agent=SecretStr("secret"))

    status, payload = await dispatch_request(
        config,
        "GET",
        "/readyz",
        {},
        {"Authorization": "Basic secret", "X-Request-Id": "request-1"},
        None,
    )

    assert status == 401
    assert payload["meta"] == {"request_id": "request-1"}


@pytest.mark.asyncio
async def test_control_requires_idempotency_key_after_standard_auth() -> None:
    config = AppConfig(internal_secret_agent=SecretStr("secret"))

    status, payload = await dispatch_request(
        config,
        "POST",
        "/v1/runs/run-1/control",
        {},
        {"Authorization": "Bearer secret", "X-Request-Id": "request-1"},
        {"kind": "run.cancel", "session_id": "session-1"},
    )

    assert status == 400
    assert payload["error"] == {
        "code": "idempotency_key_required",
        "message": "Control requests require Idempotency-Key",
    }


@pytest.mark.asyncio
async def test_launch_requires_trusted_identity_before_opening_dependencies() -> None:
    config = AppConfig(internal_secret_agent=SecretStr("secret"))

    status, payload = await dispatch_request(
        config,
        "POST",
        "/v1/runs",
        {},
        {"Authorization": "Bearer secret", "X-Request-Id": "request-1"},
        {
            "request_id": "request-1",
            "run_id": "run-1",
            "session_id": "session-1",
            "feature_key": "chat",
            "message_id": "message-1",
            "content": "hello",
        },
    )

    assert status == 401
    assert payload["error"] == {
        "code": "identity_required",
        "message": "Trusted execution identity headers are required",
    }


@pytest.mark.asyncio
async def test_readyz_fails_for_unavailable_ring_before_dependency_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.interfaces.http.execution_proof_jwks import (
        ExecutionProofJwksState,
    )
    from kokoro_agent.interfaces.http import server as server_module

    def forbidden_factory(*args: object, **kwargs: object) -> object:
        raise AssertionError("dependency factory must not run")

    monkeypatch.setattr(server_module, "make_stream", forbidden_factory)
    config = AppConfig(internal_secret_agent=SecretStr("secret"))
    status, payload = await dispatch_request(
        config,
        "GET",
        "/readyz",
        {},
        {"Authorization": "Bearer secret", "X-Request-Id": "request-1"},
        None,
        execution_proof_jwks=ExecutionProofJwksState.unavailable(),
    )
    assert status == 503
    assert payload["error"] == {
        "code": "agent_unavailable",
        "message": "Agent dependencies are unavailable",
    }


def test_threaded_http_server_has_bounded_handler_policy() -> None:
    assert http_server.AgentHttpServer.daemon_threads is True
    assert http_server.AgentHttpServer.block_on_close is False


def test_draining_keeps_health_live_and_rejects_readiness_before_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance = create_http_server(
        Config(),
        "127.0.0.1",
        0,
        execution_proof_jwks=ExecutionProofJwksState(available=True, body=BODY),
    )

    def fail_dependency(*_args: object, **_kwargs: object) -> Never:
        raise AssertionError("draining readiness must not construct dependencies")

    monkeypatch.setattr(http_server, "make_stream", fail_dependency)
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    try:
        getattr(instance, "start_draining")()
        address = (str(instance.server_address[0]), int(instance.server_address[1]))
        health = _parts(
            _raw(
                address,
                b"GET /healthz HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
            )
        )
        ready = _parts(
            _raw(
                address,
                b"GET /readyz HTTP/1.1\r\nHost: local\r\n"
                b"Authorization: Bearer secret\r\nConnection: close\r\n\r\n",
            )
        )
        assert health[0] == 200
        assert ready[0] == 503
        assert json.loads(ready[2])["error"]["code"] == "agent_draining"
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


def test_active_handler_registry_waits_for_complete_declared_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    original = getattr(http_server.AgentRequestHandler, "_send_representation")

    def delayed(
        self: http_server.AgentRequestHandler,
        status: int,
        content_type: str,
        encoded: bytes,
        *,
        allow: str | None = None,
    ) -> None:
        entered.set()
        assert release.wait(1)
        original(self, status, content_type, encoded, allow=allow)

    monkeypatch.setattr(
        http_server.AgentRequestHandler, "_send_representation", delayed
    )
    instance = create_http_server(
        Config(),
        "127.0.0.1",
        0,
        execution_proof_jwks=ExecutionProofJwksState(available=True, body=BODY),
    )
    server_thread = threading.Thread(target=instance.serve_forever, daemon=True)
    server_thread.start()
    response: list[bytes] = []
    client = threading.Thread(
        target=lambda: response.append(
            _raw(
                (str(instance.server_address[0]), int(instance.server_address[1])),
                b"GET /healthz HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
            )
        )
    )
    client.start()
    try:
        assert entered.wait(1)
        getattr(instance, "start_draining")()
        started = time.monotonic()
        assert getattr(instance, "wait_for_active_handlers")(0.05) is False
        assert time.monotonic() - started < 0.5
        release.set()
        assert getattr(instance, "wait_for_active_handlers")(1) is True
        client.join(timeout=1)
        assert not client.is_alive()
        status, headers, body = _parts(response[0])
        assert status == 200
        assert int(headers["content-length"]) == len(body)
        assert json.loads(body)["status"] == "ok"
    finally:
        release.set()
        instance.shutdown()
        instance.server_close()
        server_thread.join(timeout=2)
        client.join(timeout=1)


def test_active_registry_counts_request_before_worker_thread_can_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance = create_http_server(
        Config(),
        "127.0.0.1",
        0,
        execution_proof_jwks=ExecutionProofJwksState(available=True, body=BODY),
    )
    request, peer = socket.socketpair()
    try:

        def do_not_start(_self: threading.Thread) -> None:
            return None

        monkeypatch.setattr(threading.Thread, "start", do_not_start)
        instance.process_request(request, ("127.0.0.1", 1))
        assert instance.wait_for_active_handlers(0) is False
    finally:
        request.close()
        peer.close()
        instance.server_close()


def test_preconnected_requests_admit_after_auth_and_after_drain_linearization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def dispatch_bomb(
        *_args: object, **_kwargs: object
    ) -> tuple[int, dict[str, object]]:
        nonlocal calls
        calls += 1
        return 200, {"unexpected": True}

    monkeypatch.setattr(http_server, "dispatch_request", dispatch_bomb)
    instance = create_http_server(
        Config(),
        "127.0.0.1",
        0,
        execution_proof_jwks=ExecutionProofJwksState(available=True, body=BODY),
    )
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    address = (str(instance.server_address[0]), int(instance.server_address[1]))
    connections = [socket.create_connection(address, timeout=1) for _ in range(4)]
    try:
        deadline = time.monotonic() + 1
        while getattr(instance, "_active_handlers") != len(connections):
            assert time.monotonic() < deadline
            time.sleep(0.01)
        instance.start_draining()
        requests = (
            b"GET /readyz HTTP/1.1\r\nHost: local\r\nAuthorization: Bearer secret\r\nConnection: close\r\n\r\n",
            b"POST /v1/runs HTTP/1.1\r\nHost: local\r\nAuthorization: Bearer secret\r\nContent-Length: 999999\r\nConnection: close\r\n\r\n",
            b"GET /readyz HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
            b"GET /v1/unknown HTTP/1.1\r\nHost: local\r\nAuthorization: Bearer wrong\r\nConnection: close\r\n\r\n",
        )
        results: list[tuple[int, dict[str, str], bytes]] = []
        for connection, request in zip(connections, requests, strict=True):
            connection.settimeout(1)
            connection.sendall(request)
            connection.shutdown(socket.SHUT_WR)
            chunks: list[bytes] = []
            while chunk := connection.recv(65_536):
                chunks.append(chunk)
            results.append(_parts(b"".join(chunks)))
        assert [result[0] for result in results] == [503, 503, 401, 401]
        assert [json.loads(result[2])["error"]["code"] for result in results] == [
            "agent_draining",
            "agent_draining",
            "service_auth_failed",
            "service_auth_failed",
        ]
        assert calls == 0
    finally:
        for connection in connections:
            connection.close()
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


def test_request_linearized_before_drain_completes_after_drain_begins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance = create_http_server(
        Config(),
        "127.0.0.1",
        0,
        execution_proof_jwks=ExecutionProofJwksState(available=True, body=BODY),
    )
    original = getattr(instance, "try_admit_request")
    admitted = threading.Event()
    release = threading.Event()

    def pause_after_linearization() -> bool:
        result = original()
        admitted.set()
        assert release.wait(1)
        return result

    monkeypatch.setattr(instance, "try_admit_request", pause_after_linearization)
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    response: list[bytes] = []
    client = threading.Thread(
        target=lambda: response.append(
            _raw(
                (str(instance.server_address[0]), int(instance.server_address[1])),
                b"GET /unknown HTTP/1.1\r\nHost: local\r\n"
                b"Authorization: Bearer secret\r\nConnection: close\r\n\r\n",
            )
        )
    )
    client.start()
    try:
        assert admitted.wait(1)
        instance.start_draining()
        release.set()
        client.join(timeout=1)

        assert not client.is_alive()
        status, _, body = _parts(response[0])
        assert status == 404
        assert json.loads(body)["error"]["code"] == "route_not_found"
    finally:
        release.set()
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)
        client.join(timeout=1)


def _post_health_response(
    instance: http_server.AgentHttpServer,
    *,
    authorization: bytes = b"",
    content_length: bytes | None = None,
) -> tuple[int, dict[str, str], bytes]:
    address = (str(instance.server_address[0]), int(instance.server_address[1]))
    with socket.create_connection(address, timeout=1) as connection:
        connection.settimeout(1)
        request = b"POST /healthz HTTP/1.1\r\nHost: local\r\n"
        if authorization:
            request += b"Authorization: " + authorization + b"\r\n"
        if content_length is not None:
            request += b"Content-Length: " + content_length + b"\r\n"
        connection.sendall(request + b"Connection: close\r\n\r\n")
        chunks: list[bytes] = []
        while chunk := connection.recv(65_536):
            chunks.append(chunk)
    return _parts(b"".join(chunks))


def test_post_health_without_bearer_rejects_before_body_and_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"body": 0, "dispatch": 0}

    def body_bomb(_self: object) -> None:
        calls["body"] += 1
        raise AssertionError("body must not be read")

    async def dispatch_bomb(
        *_args: object, **_kwargs: object
    ) -> tuple[int, dict[str, object]]:
        calls["dispatch"] += 1
        raise AssertionError("request must not dispatch")

    monkeypatch.setattr(http_server.AgentRequestHandler, "_body", body_bomb)
    monkeypatch.setattr(http_server, "dispatch_request", dispatch_bomb)
    instance = create_http_server(Config(), "127.0.0.1", 0)
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    try:
        status, _, body = _post_health_response(instance)
        assert status == 401
        assert json.loads(body)["error"]["code"] == "service_auth_failed"
        assert calls == {"body": 0, "dispatch": 0}
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


def test_post_health_during_drain_rejects_before_body_and_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"body": 0, "dispatch": 0}

    def body_bomb(_self: object) -> None:
        calls["body"] += 1
        raise AssertionError("body must not be read")

    async def dispatch_bomb(
        *_args: object, **_kwargs: object
    ) -> tuple[int, dict[str, object]]:
        calls["dispatch"] += 1
        raise AssertionError("request must not dispatch")

    monkeypatch.setattr(http_server.AgentRequestHandler, "_body", body_bomb)
    monkeypatch.setattr(http_server, "dispatch_request", dispatch_bomb)
    instance = create_http_server(Config(), "127.0.0.1", 0)
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    try:
        instance.start_draining()
        status, _, body = _post_health_response(
            instance, authorization=b"Bearer secret"
        )
        assert status == 503
        assert json.loads(body)["error"]["code"] == "agent_draining"
        assert calls == {"body": 0, "dispatch": 0}
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize(
    ("authorization", "draining", "expected_status", "expected_code"),
    [
        (b"", False, 401, "service_auth_failed"),
        (b"Bearer secret", True, 503, "agent_draining"),
    ],
)
def test_post_health_auth_and_drain_precede_unfinished_oversized_body(
    authorization: bytes,
    draining: bool,
    expected_status: int,
    expected_code: str,
) -> None:
    instance = create_http_server(Config(), "127.0.0.1", 0)
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    try:
        if draining:
            instance.start_draining()
        status, _, body = _post_health_response(
            instance,
            authorization=authorization,
            content_length=b"999999999",
        )
        assert status == expected_status
        assert json.loads(body)["error"]["code"] == expected_code
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("stop_signal", [signal.SIGINT, signal.SIGTERM])
def test_installed_http_entrypoint_signal_exits_bounded_and_releases_port(
    stop_signal: signal.Signals,
) -> None:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = int(reservation.getsockname()[1])
    executable = Path(sys.executable).parent / "kokoro-agent-http"
    assert executable.is_file()
    environment = os.environ.copy()
    environment.update(
        {
            "KOKORO_AGENT_HTTP_HOST": "127.0.0.1",
            "KOKORO_AGENT_HTTP_PORT": str(port),
            "KOKORO_AGENT_CONFIG": "",
        }
    )
    process = subprocess.Popen(
        [str(executable)],
        cwd=Path(__file__).resolve().parent,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 3
        while True:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise AssertionError("HTTP entrypoint did not listen in time")
                time.sleep(0.02)
        started = time.monotonic()
        process.send_signal(stop_signal)
        assert process.wait(timeout=3) == 0
        assert time.monotonic() - started < 3
        with socket.socket() as rebound:
            rebound.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            rebound.bind(("127.0.0.1", port))
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=1)


@pytest.mark.parametrize("stop_signal", [signal.SIGINT, signal.SIGTERM])
@pytest.mark.parametrize("mode", ["complete", "timeout"])
def test_signal_drains_active_handler_or_exits_after_fixed_deadline(
    tmp_path: Path, stop_signal: signal.Signals, mode: str
) -> None:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = int(reservation.getsockname()[1])
    entered = tmp_path / "entered"
    release = tmp_path / "release"
    script = """
import os
from pathlib import Path
import time
from kokoro_agent.interfaces.http import main as http_main
from kokoro_agent.interfaces.http import server as http_server

entered = Path(os.environ["A2B_ENTERED"])
release = Path(os.environ["A2B_RELEASE"])
mode = os.environ["A2B_MODE"]
original = http_server.AgentRequestHandler._send_representation
def delayed(self, status, content_type, encoded, *, allow=None):
    entered.write_text("entered", encoding="utf-8")
    if mode == "complete":
        deadline = time.monotonic() + 5
        while not release.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
    else:
        time.sleep(10)
    original(self, status, content_type, encoded, allow=allow)
http_server.AgentRequestHandler._send_representation = delayed
http_main.main()
"""
    environment = os.environ.copy()
    environment.update(
        {
            "KOKORO_AGENT_HTTP_HOST": "127.0.0.1",
            "KOKORO_AGENT_HTTP_PORT": str(port),
            "KOKORO_AGENT_CONFIG": "",
            "A2B_ENTERED": str(entered),
            "A2B_RELEASE": str(release),
            "A2B_MODE": mode,
        }
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    connection: socket.socket | None = None
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                connection = socket.create_connection(("127.0.0.1", port), timeout=0.1)
                break
            except OSError:
                time.sleep(0.02)
        assert connection is not None
        connection.settimeout(4)
        connection.sendall(
            b"GET /healthz HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
        )
        deadline = time.monotonic() + 1
        while not entered.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert entered.exists()
        started = time.monotonic()
        process.send_signal(stop_signal)
        if mode == "complete":
            time.sleep(0.05)
            assert process.poll() is None
            release.write_text("release", encoding="utf-8")
            chunks: list[bytes] = []
            while chunk := connection.recv(65_536):
                chunks.append(chunk)
            response = b"".join(chunks)
            head, _, body = response.partition(b"\r\n\r\n")
            length = next(
                int(line.split(b":", 1)[1])
                for line in head.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            assert b" 200 " in head
            assert len(body) == length
        assert process.wait(timeout=3) == 0
        elapsed = time.monotonic() - started
        assert elapsed < 3
        if mode == "timeout":
            assert elapsed >= 1.5
        with socket.socket() as rebound:
            rebound.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            rebound.bind(("127.0.0.1", port))
    finally:
        if connection is not None:
            connection.close()
        if process.poll() is None:
            process.kill()
            process.wait(timeout=1)
