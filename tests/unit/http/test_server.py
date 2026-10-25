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

from kokoro_agent.interfaces.http.main import AgentHttpConfig
from kokoro_agent.infrastructure.postgres_run_repository import RunRepositorySettings
from kokoro_agent.interfaces.http.execution_proof_jwks import ExecutionProofJwksState
import kokoro_agent.interfaces.http.server as http_server
import kokoro_agent.interfaces.http.main as http_main
from kokoro_agent.model.call_models import ModelCallBoundExpired
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
    billing_model_call_reader_secret: SecretStr | None = None


_EVIDENCE_PATH = (
    "/v1/model-call-attempts/00000000-0000-4000-8000-000000000001/"
    "usage-evidence/00000000-0000-4000-8000-000000000002"
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("config", "headers", "path", "expected"),
    [
        (Config(), {}, _EVIDENCE_PATH, 503),
        (
            Config(billing_model_call_reader_secret=SecretStr("billing")),
            {"authorization": "Basic billing"},
            _EVIDENCE_PATH,
            401,
        ),
        (
            Config(billing_model_call_reader_secret=SecretStr("billing")),
            {"authorization": "Bearer wrong"},
            "/v1/model-call-attempts/not-a-uuid/usage-evidence/not-a-uuid",
            401,
        ),
        (
            Config(billing_model_call_reader_secret=SecretStr("billing")),
            {"authorization": "Bearer secret"},
            _EVIDENCE_PATH,
            401,
        ),
        (
            Config(billing_model_call_reader_secret=SecretStr("billing")),
            {"authorization": "Bearer billing"},
            _EVIDENCE_PATH,
            403,
        ),
        (
            Config(billing_model_call_reader_secret=SecretStr("billing")),
            {
                "authorization": "Bearer billing",
                "x-kokoro-tenant-ref": "tenant with space",
            },
            _EVIDENCE_PATH,
            403,
        ),
        (
            Config(billing_model_call_reader_secret=SecretStr("billing")),
            {
                "authorization": "Bearer billing",
                "x-kokoro-tenant-ref": "tenant",
                "content-length": "2",
            },
            _EVIDENCE_PATH,
            403,
        ),
        (
            Config(billing_model_call_reader_secret=SecretStr("billing")),
            {
                "authorization": "Bearer billing",
                "x-kokoro-tenant-ref": "tenant",
                "x-kokoro-subject-ref": "override",
            },
            _EVIDENCE_PATH,
            403,
        ),
        (
            Config(billing_model_call_reader_secret=SecretStr("billing")),
            {"authorization": "Bearer billing", "x-kokoro-tenant-ref": "tenant"},
            "/v1/model-call-attempts/not-a-uuid/usage-evidence/not-a-uuid",
            400,
        ),
    ],
)
async def test_billing_evidence_reader_fails_closed_before_database(
    config: Config, headers: dict[str, str], path: str, expected: int
) -> None:
    status, _ = await dispatch_request(config, "GET", path, {}, headers, None)
    assert status == expected


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
        AgentHttpConfig(), method, path, {}, {"x-request-id": "request-1"}, {}
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
        AgentHttpConfig(), "GET", "/healthz", {}, {}, None
    )

    assert status == 200
    assert payload == {"status": "ok", "service": "kokoro-agent"}


@pytest.mark.asyncio
async def test_standard_authorization_is_case_insensitive_and_request_id_is_preserved() -> (
    None
):
    config = AgentHttpConfig(internal_secret_agent=SecretStr("secret"))

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
    config = AgentHttpConfig(internal_secret_agent=SecretStr("secret"))

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
    config = AgentHttpConfig(internal_secret_agent=SecretStr("secret"))

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
            "project_ref": None,
            "selected_skill_source_refs": [],
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
    config = AgentHttpConfig(internal_secret_agent=SecretStr("secret"))
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


@pytest.mark.asyncio
async def test_bound_reader_missing_dedicated_secret_fails_before_shared_auth() -> None:
    status, payload = await dispatch_request(
        Config(),
        "GET",
        "/v1/model-call-bounds/mcb_00000000-0000-5000-8000-000000000001",
        {},
        {"x-request-id": "bound-reader-test"},
        None,
    )
    assert status == 503
    assert payload == {
        "error": {
            "code": "agent_reader_unavailable",
            "message": "Agent bound reader is unavailable",
        },
        "meta": {"request_id": "bound-reader-test"},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "query", "body", "status", "code"),
    [
        ({}, {}, None, 401, "agent_reader_auth_failed"),
        ({"authorization": "Bearer secret"}, {}, None, 401, "agent_reader_auth_failed"),
        ({"authorization": "Bearer billing"}, {}, None, 403, "agent_reader_forbidden"),
        (
            {"authorization": "Bearer billing", "x-kokoro-tenant-ref": " tenant"},
            {},
            None,
            403,
            "agent_reader_forbidden",
        ),
        (
            {
                "authorization": "Bearer billing",
                "x-kokoro-tenant-ref": "tenant",
                "x-kokoro-subject-ref": "",
            },
            {},
            None,
            403,
            "agent_reader_forbidden",
        ),
        (
            {
                "authorization": "Bearer billing",
                "x-kokoro-tenant-ref": "tenant",
                "content-length": "1",
            },
            {},
            None,
            403,
            "agent_reader_forbidden",
        ),
        (
            {"authorization": "Bearer billing", "x-kokoro-tenant-ref": "tenant"},
            {},
            {},
            403,
            "agent_reader_forbidden",
        ),
        (
            {"authorization": "Bearer billing", "x-kokoro-tenant-ref": "tenant"},
            {"extra": ["1"]},
            None,
            400,
            "invalid_model_call_bound_identity",
        ),
    ],
)
async def test_bound_reader_closed_admission_never_queries_database(
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
    query: dict[str, list[str]],
    body: dict[str, object] | None,
    status: int,
    code: str,
) -> None:
    def unexpected(*args: object, **kwargs: object) -> Never:
        raise AssertionError("rejected reader must not construct a repository")

    monkeypatch.setattr(http_server, "PostgresModelCallRepository", unexpected)
    actual, payload = await dispatch_request(
        Config(billing_model_call_reader_secret=SecretStr("billing")),
        "GET",
        "/v1/model-call-bounds/mcb_00000000-0000-5000-8000-000000000001",
        query,
        headers,
        body,
    )
    assert actual == status
    assert isinstance(payload["error"], dict) and payload["error"]["code"] == code


def test_bound_reader_real_host_preserves_absence_and_empty_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.model.call_models import CurrentModelCallBound

    reads: list[tuple[str, str]] = []

    class Repository:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def read_bound(self, tenant: str, ref: str) -> CurrentModelCallBound:
            reads.append((tenant, ref))
            return CurrentModelCallBound(
                planned_binding_ref="mpb_00000000-0000-5000-8000-000000000001",
                planned_binding_digest="a" * 64,
                planned_binding_bytes=b"{}",
                execution_bound_ref=ref,
                execution_bound_digest="b" * 64,
                execution_bound_artifact_version="1.0.0",
                execution_bound_revision="1",
                execution_bound_bytes=b"{}",
            )

    monkeypatch.setattr(http_server, "PostgresModelCallRepository", Repository)
    instance = create_http_server(
        Config(billing_model_call_reader_secret=SecretStr("billing")), "127.0.0.1", 0
    )
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    ref = "mcb_00000000-0000-5000-8000-000000000001"
    base = f"GET /v1/model-call-bounds/{ref} HTTP/1.1\r\nHost: local\r\nAuthorization: Bearer billing\r\nX-Kokoro-Tenant-Ref: tenant\r\nX-Request-Id: host-bound\r\nConnection: close\r\n"
    try:
        address = (str(instance.server_address[0]), int(instance.server_address[1]))
        good = _parts(_raw(address, (base + "\r\n").encode()))
        assert good[0] == 200
        assert good[1]["cache-control"] == "no-store"
        assert good[1]["x-request-id"] == "host-bound"
        assert len(json.loads(good[2])["data"]) == 8
        assert reads == [("tenant", ref)]
        for extra in ("X-Kokoro-Subject-Ref: \r\n", "Transfer-Encoding: \r\n"):
            denied = _parts(_raw(address, (base + extra + "\r\n").encode()))
            assert denied[0] == 403
            assert json.loads(denied[2])["error"]["code"] == "agent_reader_forbidden"
        assert reads == [("tenant", ref)]
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("phase", ["listener", "stop_lock"])
@pytest.mark.parametrize("stop_signal", [signal.SIGINT, signal.SIGTERM])
def test_http_signal_before_poll_is_lock_free_and_releases_listener(
    tmp_path: Path, phase: str, stop_signal: signal.Signals
) -> None:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = int(reservation.getsockname()[1])
    script = r"""
import inspect
import os
from pathlib import Path
import signal
import socket
import sys
import threading
from kokoro_agent.interfaces.http import main as owner

phase = os.environ["SIGNAL_PHASE"]
stop_signal = int(os.environ["SIGNAL_NUMBER"])
injected = Path(os.environ["SIGNAL_INJECTED"])
original_create = owner.create_http_server
fired = False

def create(*args, **kwargs):
    server = original_create(*args, **kwargs)
    if phase == "listener":
        with socket.create_connection(server.server_address, timeout=0.2):
            pass
        injected.write_text("listener", encoding="utf-8")
        os.kill(os.getpid(), stop_signal)
    return server
owner.create_http_server = create

def trace(frame, event, arg):
    global fired
    if event == "line" and not fired and frame.f_code is owner.main.__code__:
        running = any(thread.name == "kokoro-agent-http-server" and thread.is_alive()
                      for thread in threading.enumerate())
        if phase == "stop_lock" and running:
            fired = True
            # The old callback closes over an Event whose non-reentrant lock
            # may already be held when a Python signal interrupts main.
            callback = signal.getsignal(stop_signal)
            cells = () if not inspect.isfunction(callback) else callback.__closure__ or ()
            events = [cell.cell_contents for cell in cells
                      if isinstance(cell.cell_contents, threading.Event)]
            lock = events[0]._cond if events else threading.Condition(threading.Lock())
            with lock:
                injected.write_text("stop_lock", encoding="utf-8")
                os.kill(os.getpid(), stop_signal)
    return trace
sys.settrace(trace)
try:
    owner.main()
finally:
    sys.settrace(None)
"""
    environment = os.environ.copy()
    marker = tmp_path / "injected"
    environment.update(
        KOKORO_AGENT_HTTP_HOST="127.0.0.1",
        KOKORO_AGENT_HTTP_PORT=str(port),
        KOKORO_AGENT_CONFIG="",
        SIGNAL_PHASE=phase,
        SIGNAL_NUMBER=str(int(stop_signal)),
        SIGNAL_INJECTED=str(marker),
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _, stderr = process.communicate(timeout=3)
        assert marker.read_text(encoding="utf-8") == phase
        assert process.returncode == 0, stderr.decode()
        with socket.socket() as rebound:
            rebound.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            rebound.bind(("127.0.0.1", port))
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=1)


def test_http_constructor_failure_restores_signal_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {item: signal.getsignal(item) for item in (signal.SIGINT, signal.SIGTERM)}

    def failed(*args: object, **kwargs: object) -> Never:
        assert all(signal.getsignal(item) != before[item] for item in before)
        raise OSError("owned bind failure")

    monkeypatch.setattr(http_main, "create_http_server", failed)
    with pytest.raises(OSError, match="owned bind failure"):
        http_main.main()
    assert {item: signal.getsignal(item) for item in before} == before


@pytest.mark.parametrize(
    ("extra", "authorization", "expected"),
    [
        ("Content-Length: 0\r\nContent-Length: 1\r\n", "Bearer billing", 403),
        ("Content-Length: 0\r\nContent-Length: 0\r\n", "Bearer billing", 403),
        ("Content-Length: 0, 1\r\n", "Bearer billing", 403),
        ("X-Kokoro-Tenant-Ref: foreign\r\n", "Bearer billing", 403),
        ("Authorization: Bearer another\r\n", "Bearer billing", 403),
        ("Content-Length: 0\r\nContent-Length: 1\r\n", "Bearer wrong", 401),
        ("Authorization: Bearer billing\r\n", "Bearer wrong", 401),
    ],
)
def test_bound_reader_rejects_ambiguous_wire_after_auth_without_read(
    monkeypatch: pytest.MonkeyPatch,
    extra: str,
    authorization: str,
    expected: int,
) -> None:
    reads: list[tuple[str, str]] = []

    class Repository:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def read_bound(self, tenant: str, ref: str) -> None:
            reads.append((tenant, ref))

    monkeypatch.setattr(http_server, "PostgresModelCallRepository", Repository)
    instance = create_http_server(
        Config(billing_model_call_reader_secret=SecretStr("billing")), "127.0.0.1", 0
    )
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    base = (
        "GET /v1/model-call-bounds/mcb_00000000-0000-5000-8000-000000000001 HTTP/1.1\r\n"
        f"Host: local\r\nAuthorization: {authorization}\r\n"
        "X-Kokoro-Tenant-Ref: tenant\r\nX-Request-Id: ambiguous-bound\r\nConnection: close\r\n"
    )
    try:
        address = (str(instance.server_address[0]), int(instance.server_address[1]))
        status, headers, body = _parts(_raw(address, (base + extra + "\r\nx").encode()))
        assert status == expected
        assert json.loads(body)["error"]["code"] == (
            "agent_reader_auth_failed" if expected == 401 else "agent_reader_forbidden"
        )
        assert headers["cache-control"] == "no-store"
        assert headers["x-request-id"] == "ambiguous-bound"
        assert reads == []
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


def test_bound_reader_expiry_http_mapping_keeps_request_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Repository:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def read_bound(self, tenant: str, ref: str) -> Never:
            raise ModelCallBoundExpired("expired")

    monkeypatch.setattr(http_server, "PostgresModelCallRepository", Repository)
    instance = create_http_server(
        Config(billing_model_call_reader_secret=SecretStr("billing")), "127.0.0.1", 0
    )
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    try:
        address = (str(instance.server_address[0]), int(instance.server_address[1]))
        status, headers, body = _parts(
            _raw(
                address,
                b"GET /v1/model-call-bounds/mcb_00000000-0000-5000-8000-000000000001 HTTP/1.1\r\n"
                b"Host: local\r\nAuthorization: Bearer billing\r\nX-Kokoro-Tenant-Ref: tenant\r\n"
                b"X-Request-Id: expiry-bound\r\nConnection: close\r\n\r\n",
            )
        )
        assert status == 410
        assert headers["cache-control"] == "no-store"
        assert headers["x-request-id"] == "expiry-bound"
        assert json.loads(body) == {
            "error": {
                "code": "model_call_bound_expired",
                "message": "Model-call bound is expired",
            },
            "meta": {"request_id": "expiry-bound"},
        }
    finally:
        instance.shutdown()
        instance.server_close()
        thread.join(timeout=2)


def test_signal_flag_closes_request_gate_before_main_can_start_draining(
    tmp_path: Path,
) -> None:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = int(reservation.getsockname()[1])
    script = r"""
import http.client
import json
import os
import signal
from kokoro_agent.interfaces.http import main as owner
from kokoro_agent.interfaces.http import server as host

port = int(os.environ["KOKORO_AGENT_HTTP_PORT"])
original_sleep = owner.time.sleep
observed = False
body_calls = 0
dispatch_calls = 0

def body(self):
    global body_calls
    body_calls += 1
    raise AssertionError("stopped request read body")
async def dispatch(*args, **kwargs):
    global dispatch_calls
    dispatch_calls += 1
    raise AssertionError("stopped request reached dependency boundary")
host.AgentRequestHandler._body = body
host.dispatch_request = dispatch

def poll(seconds):
    global observed
    if not observed:
        observed = True
        # Callback returns while main is deliberately kept inside this poll.
        os.kill(os.getpid(), signal.SIGINT)
        for method, path, bearer, expected, code in (
            ("GET", "/readyz", "secret", 503, "agent_draining"),
            ("POST", "/healthz", "secret", 503, "agent_draining"),
            ("POST", "/healthz", "wrong", 401, "service_auth_failed"),
        ):
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
            connection.request(method, path, headers={"Authorization": "Bearer " + bearer,
                               "Content-Length": "999999999", "Connection": "close"})
            response = connection.getresponse()
            document = json.loads(response.read())
            assert response.status == expected, document
            assert document["error"]["code"] == code, document
            connection.close()
        assert body_calls == dispatch_calls == 0
    original_sleep(seconds)
owner.time.sleep = poll
owner.main()
assert observed
"""
    environment = os.environ.copy()
    environment.update(
        KOKORO_AGENT_HTTP_HOST="127.0.0.1",
        KOKORO_AGENT_HTTP_PORT=str(port),
        KOKORO_AGENT_CONFIG="",
        KOKORO_INTERNAL_SECRET_AGENT="secret",
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        timeout=3,
    )
    assert result.returncode == 0, result.stderr.decode()


def test_http_thread_start_failure_closes_listener_and_restores_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {item: signal.getsignal(item) for item in (signal.SIGINT, signal.SIGTERM)}
    created: list[http_server.AgentHttpServer] = []
    original_create = http_main.create_http_server

    def create(*args: object, **kwargs: object) -> http_server.AgentHttpServer:
        instance = original_create(Config(), "127.0.0.1", 0)
        created.append(instance)
        return instance

    def failed_start(self: threading.Thread) -> Never:
        raise RuntimeError("owned thread start failure")

    monkeypatch.setattr(http_main, "create_http_server", create)
    monkeypatch.setattr(threading.Thread, "start", failed_start)
    with pytest.raises(RuntimeError, match="owned thread start failure"):
        http_main.main()
    assert len(created) == 1
    assert created[0].socket.fileno() == -1
    assert {item: signal.getsignal(item) for item in before} == before
