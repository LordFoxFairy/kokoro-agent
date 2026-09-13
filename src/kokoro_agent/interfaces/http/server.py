"""Small standard-library HTTP host for the Agent business ingress.

The worker and this ingress are separate processes from one package.  The
host intentionally has no framework-specific route magic: every public path
is explicit, bounded, and delegates to :class:`AgentIngress`.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import socket
import threading
from uuid import uuid4
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Protocol
from urllib.parse import parse_qs, urlsplit

from pydantic import SecretStr, TypeAdapter

from kokoro_agent.application.chat.dto import ChatQueryRequest, ChatSessionListRequest
from kokoro_agent.application.chat.service import ChatService
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, REQUESTS_STREAM
from kokoro_agent.protocol.control import IdentityKind
from kokoro_agent.interfaces.http.ingress import AgentIngress, IngressError
from kokoro_agent.interfaces.http.execution_proof_jwks import ExecutionProofJwksState
from kokoro_agent.infrastructure.postgres_run_repository import (
    RunRepositorySettings,
    make_run_repository,
)
from kokoro_agent.infrastructure.postgres_chat_repository import (
    PostgresChatRepositorySettings,
    make_chat_repository,
)
from kokoro_agent.streams.factory import StreamSettings, make_stream

LOGGER = logging.getLogger(__name__)
_Request = socket.socket | tuple[bytes, socket.socket]
_ClientAddress = tuple[str, int] | str | bytes
_RUN_CONTROL = re.compile(r"^/v1/runs/([^/]+)/control$")
_RUN_EVENTS = re.compile(r"^/v1/runs/([^/]+)/events$")
_SESSION_MESSAGES = re.compile(r"^/v1/sessions/([^/]+)/messages$")
_SESSION_EVENTS = re.compile(r"^/v1/sessions/([^/]+)/events$")
_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", re.ASCII)
_MAX_BODY = 1024 * 1024
_JWKS_PATH = "/v1/execution-proof/jwks"
_JWKS_IDENTITY_HEADERS = (
    "x-kokoro-tenant-ref",
    "x-kokoro-subject-ref",
    "x-kokoro-actor-ref",
    "x-kokoro-subject-kind",
    "x-kokoro-actor-kind",
    "x-kokoro-identity-assertion-ref",
)


def _known_business_route(method: str, path: str) -> bool:
    if method == "GET":
        return path in {"/healthz", "/readyz", "/v1/sessions"} or any(
            pattern.fullmatch(path) is not None
            for pattern in (_RUN_EVENTS, _SESSION_MESSAGES, _SESSION_EVENTS)
        )
    if method == "POST":
        return path == "/v1/runs" or _RUN_CONTROL.fullmatch(path) is not None
    return False


def _safe_method(method: str) -> str:
    return (
        method
        if method
        in {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE"}
        else "OTHER"
    )


class AgentConfig(Protocol):
    @property
    def stream(self) -> StreamSettings: ...

    @property
    def run_repository(self) -> RunRepositorySettings: ...

    @property
    def database_url(self) -> str: ...

    @property
    def database_schema(self) -> str: ...

    @property
    def internal_secret_agent(self) -> SecretStr | None: ...


_JSON_OBJECT = TypeAdapter(dict[str, object])


def _request_id(headers: Mapping[str, str]) -> str:
    value = headers.get("x-request-id", "").strip()
    return value or f"req_agent_{uuid4().hex}"


def _request_id_log_reference(value: str) -> str:
    if _REQUEST_ID.fullmatch(value) is not None:
        return value
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _idempotency_key(headers: Mapping[str, str]) -> str:
    return headers.get("idempotency-key", "").strip()


def _envelope(data: object, request_id: str) -> dict[str, object]:
    return {"data": data, "meta": {"request_id": request_id}}


def _error(code: str, message: str, request_id: str) -> dict[str, object]:
    return {
        "error": {"code": code, "message": message},
        "meta": {"request_id": request_id},
    }


def _service_auth_failure(
    config: AgentConfig, headers: Mapping[str, str], request_id: str
) -> tuple[int, dict[str, object]] | None:
    secret = config.internal_secret_agent
    secret_value = "" if secret is None else secret.get_secret_value().strip()
    if not secret_value:
        return 503, _error(
            "service_auth_not_configured",
            "Agent ingress service authentication is not configured",
            request_id,
        )
    authorization = headers.get("authorization", "").strip()
    scheme, separator, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not separator or token.strip() != secret_value:
        return 401, _error(
            "service_auth_failed", "Agent ingress authentication failed", request_id
        )
    return None


def _identity(headers: Mapping[str, str]) -> ExecutionIdentity:
    tenant = headers.get("x-kokoro-tenant-ref", "").strip()
    subject = headers.get("x-kokoro-subject-ref", "").strip()
    actor = headers.get("x-kokoro-actor-ref", "").strip()
    assertion = headers.get("x-kokoro-identity-assertion-ref", "").strip()
    if not tenant or not subject or not actor or not assertion:
        raise IngressError(
            401, "identity_required", "Trusted execution identity headers are required"
        )

    def kind(name: str) -> IdentityKind:
        value = headers.get(name, "").strip() or "user"
        if value not in {"user", "project", "service"}:
            raise IngressError(400, "invalid_identity", f"{name} is invalid")
        if value == "user":
            return "user"
        if value == "project":
            return "project"
        return "service"

    return ExecutionIdentity(
        tenant_ref=tenant,
        actor=IdentityRef(kind=kind("x-kokoro-actor-kind"), opaque_ref=actor),
        subject=IdentityRef(kind=kind("x-kokoro-subject-kind"), opaque_ref=subject),
        identity_assertion_ref=assertion,
    )


def _page(
    headers: Mapping[str, str],
    query: Mapping[str, list[str]],
    session_id: str,
    execution_identity: ExecutionIdentity | None = None,
) -> ChatQueryRequest:
    def integer(name: str, default: int) -> int:
        raw = query.get(name, [str(default)])[0]
        try:
            return int(raw)
        except (TypeError, ValueError) as error:
            raise IngressError(
                400, "invalid_page", f"{name} must be an integer"
            ) from error

    return ChatQueryRequest(
        execution_identity=execution_identity or _identity(headers),
        session_id=session_id,
        after_seq=integer("after_seq", 0),
        limit=integer("limit", 200),
    )


def _session_list_page(
    headers: Mapping[str, str],
    query: Mapping[str, list[str]],
    execution_identity: ExecutionIdentity | None = None,
) -> ChatSessionListRequest:
    def integer(name: str, default: int) -> int:
        raw = query.get(name, [str(default)])[0]
        try:
            return int(raw)
        except (TypeError, ValueError) as error:
            raise IngressError(
                400, "invalid_page", f"{name} must be an integer"
            ) from error

    project_ref = query.get("project_ref", [None])[0]
    if project_ref == "":
        project_ref = None
    return ChatSessionListRequest(
        execution_identity=execution_identity or _identity(headers),
        project_ref=project_ref,
        cursor=query.get("cursor", [None])[0],
        limit=integer("limit", 50),
    )


async def dispatch_request(
    config: AgentConfig,
    method: str,
    path: str,
    query: Mapping[str, list[str]],
    headers: Mapping[str, str],
    body: Mapping[str, object] | None,
    *,
    execution_proof_jwks: ExecutionProofJwksState | None = None,
) -> tuple[int, dict[str, object]]:
    """Execute one request with short-lived owner connections.

    Short-lived connections keep the synchronous stdlib host independent from
    an asyncio event loop owned by another process and make shutdown reliable.
    Redis/PG are still used only through Agent-owned ports.
    """
    headers = {name.lower(): value for name, value in headers.items()}
    request_id = _request_id(headers)
    if method == "GET" and path == "/healthz":
        return 200, {"status": "ok", "service": "kokoro-agent"}
    auth_failure = _service_auth_failure(config, headers, request_id)
    if auth_failure is not None:
        return auth_failure
    if (
        method == "GET"
        and path == "/readyz"
        and execution_proof_jwks is not None
        and not execution_proof_jwks.available
    ):
        return 503, _error(
            "agent_unavailable", "Agent dependencies are unavailable", request_id
        )
    if (
        method == "POST"
        and _RUN_CONTROL.fullmatch(path) is not None
        and not _idempotency_key(headers)
    ):
        return 400, _error(
            "idempotency_key_required",
            "Control requests require Idempotency-Key",
            request_id,
        )
    try:
        execution_identity = _identity(headers) if path.startswith("/v1/") else None
    except IngressError as error:
        return error.status, _error(error.code, error.message, request_id)
    if not _known_business_route(method, path):
        return 404, _error("route_not_found", "Agent route was not found", request_id)
    bus = make_stream(config.stream)
    try:
        async with (
            make_run_repository(config.run_repository) as run_repository,
            make_chat_repository(
                PostgresChatRepositorySettings(
                    database_url=config.database_url,
                    schema_name=config.database_schema,
                )
            ) as chat_repository,
        ):
            if method == "GET" and path == "/readyz":
                await bus.read_all(REQUESTS_STREAM)
                return 200, {"status": "ready", "service": "kokoro-agent"}
            ingress = AgentIngress(
                bus=bus,
                run_repository=run_repository,
                chat_service=ChatService(chat_repository),
            )
            if method == "GET" and path == "/v1/sessions":
                result = await ingress.list_sessions(
                    _session_list_page(headers, query, execution_identity)
                )
                return 200, _envelope(result.model_dump(mode="json"), request_id)
            if method == "POST" and path == "/v1/runs":
                receipt = await ingress.launch(
                    body or {},
                    execution_identity=execution_identity or _identity(headers),
                )
                return 202, _envelope(
                    {
                        "run_id": receipt.run_id,
                        "session_id": receipt.session_id,
                        "replayed": receipt.replayed,
                    },
                    request_id,
                )
            match = _RUN_CONTROL.fullmatch(path)
            if method == "POST" and match is not None:
                command_id = _idempotency_key(headers)
                control = await ingress.control(
                    match.group(1),
                    body or {},
                    command_id=command_id,
                    execution_identity=execution_identity or _identity(headers),
                )
                return 202, _envelope(control, request_id)
            match = _RUN_EVENTS.fullmatch(path)
            if method == "GET" and match is not None:
                return 200, _envelope(
                    await ingress.evidence(
                        match.group(1),
                        execution_identity=execution_identity or _identity(headers),
                        after_seq=int(query.get("after_seq", ["0"])[0]),
                        limit=int(query.get("limit", ["200"])[0]),
                    ),
                    request_id,
                )
            match = _SESSION_MESSAGES.fullmatch(path)
            if method == "GET" and match is not None:
                result = await ingress.history(
                    _page(headers, query, match.group(1), execution_identity)
                )
                return 200, _envelope(result.model_dump(mode="json"), request_id)
            match = _SESSION_EVENTS.fullmatch(path)
            if method == "GET" and match is not None:
                result = await ingress.replay(
                    _page(headers, query, match.group(1), execution_identity)
                )
                return 200, _envelope(result.model_dump(mode="json"), request_id)
            return 404, _error(
                "route_not_found", "Agent route was not found", request_id
            )
    except IngressError as error:
        return error.status, _error(error.code, error.message, request_id)
    except (ValueError, TypeError) as error:
        LOGGER.info("agent ingress request validation failed: %s", error)
        return 400, _error(
            "invalid_request",
            "Request does not match the Agent v1 contract",
            request_id,
        )
    except Exception:
        LOGGER.exception("agent ingress request failed")
        return 503, _error(
            "agent_unavailable", "Agent dependencies are unavailable", request_id
        )
    finally:
        close = getattr(bus, "aclose", None)
        if close is not None:
            await close()


class AgentRequestHandler(BaseHTTPRequestHandler):
    """Explicit HTTP/JSON adapter; the application remains independently testable."""

    server_version = "kokoro-agent/2"

    def _config(self) -> AgentConfig:
        config = getattr(self.server, "kokoro_config", None)
        if config is None:
            raise RuntimeError("AgentRequestHandler is missing HTTP config")
        return config

    def _request_id_value(self) -> str:
        value = self.__dict__.get("_normalized_request_id")
        if type(value) is not str:
            value = _request_id(self._headers())
            self.__dict__["_normalized_request_id"] = value
        return value

    def _try_admit_request(self) -> bool:
        admit = getattr(self.server, "try_admit_request", None)
        return True if admit is None else bool(admit())

    def _jwks_state(self) -> ExecutionProofJwksState:
        state = getattr(self.server, "execution_proof_jwks", None)
        snapshot: ExecutionProofJwksState | None = None
        failed = False
        try:
            if type(state) is not ExecutionProofJwksState:
                raise ValueError
            snapshot = ExecutionProofJwksState(
                available=state.available,
                body=state.body,
                content_type=state.content_type,
            )
        except Exception:
            failed = True
        if failed or snapshot is None:
            return ExecutionProofJwksState.unavailable()
        return snapshot

    def _body(self) -> dict[str, object] | None:
        length = int(self.headers.get("content-length", "0"))
        if length < 0 or length > _MAX_BODY:
            raise IngressError(
                413, "request_body_too_large", "Request body is too large"
            )
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise IngressError(
                400, "invalid_json", "Request body must be valid JSON"
            ) from error
        if not isinstance(value, dict):
            raise IngressError(
                400, "invalid_json", "Request body must be a JSON object"
            )
        try:
            return _JSON_OBJECT.validate_python(value)
        except ValueError as error:
            raise IngressError(
                400, "invalid_json", "Request object keys must be strings"
            ) from error

    def _headers(self) -> dict[str, str]:
        return {
            name: self.headers.get(name, "") or ""
            for name in (
                "x-request-id",
                "authorization",
                "idempotency-key",
                *_JWKS_IDENTITY_HEADERS,
            )
        }

    def _send_representation(
        self,
        status: int,
        content_type: str,
        encoded: bytes,
        *,
        allow: str | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("cache-control", "no-store")
        self.send_header("content-length", str(len(encoded)))
        if allow is not None:
            self.send_header("allow", allow)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(encoded)

    def _json_response(
        self, status: int, payload: dict[str, object], *, allow: str | None = None
    ) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send_representation(
            status, "application/json; charset=utf-8", encoded, allow=allow
        )

    def _is_jwks_target(self) -> bool:
        return self.path.partition("?")[0] == _JWKS_PATH

    def _jwks_request_is_valid(self) -> bool:
        if "?" in self.path:
            return False
        if self.headers.get_all("transfer-encoding") is not None:
            return False
        if self.headers.get_all("expect") is not None:
            return False
        if any(
            self.headers.get_all(name) is not None for name in _JWKS_IDENTITY_HEADERS
        ):
            return False
        lengths = self.headers.get_all("content-length")
        return lengths is None or (len(lengths) == 1 and lengths[0].strip(" \t") == "0")

    def _serve_jwks(self) -> None:
        request_id = self._request_id_value()
        if self.command not in {"GET", "HEAD"}:
            self._json_response(
                405,
                _error(
                    "execution_proof_jwks_method_not_allowed",
                    "Execution-proof JWKS supports only GET and HEAD",
                    request_id,
                ),
                allow="GET, HEAD",
            )
            return
        if not self._jwks_request_is_valid():
            self._json_response(
                400,
                _error(
                    "execution_proof_jwks_invalid_request",
                    "Execution-proof JWKS request is invalid",
                    request_id,
                ),
            )
            return
        state = self._jwks_state()
        if not state.available or state.body is None:
            self._json_response(
                503,
                _error(
                    "execution_proof_jwks_unavailable",
                    "Execution-proof JWKS is unavailable",
                    request_id,
                ),
            )
            return
        self._send_representation(200, state.content_type, state.body)

    def _serve(self) -> None:
        if self._is_jwks_target():
            self._serve_jwks()
            return
        headers = self._headers()
        request_id = self._request_id_value()
        headers["x-request-id"] = request_id
        try:
            split = urlsplit(self.path)
            is_health_request = self.command == "GET" and split.path == "/healthz"
            config = None if is_health_request else self._config()
            if config is not None:
                auth_failure = _service_auth_failure(config, headers, request_id)
                if auth_failure is not None:
                    self._json_response(*auth_failure)
                    return
                if not self._try_admit_request():
                    self._json_response(
                        503,
                        _error(
                            "agent_draining",
                            "Agent ingress is draining",
                            request_id,
                        ),
                    )
                    return
            if self.command not in {"GET", "POST"}:
                self.send_error(501, "Unsupported method")
                return
            query = parse_qs(split.query, keep_blank_values=True)
            body = self._body() if self.command in {"POST", "PUT", "PATCH"} else None
            jwks = (
                self._jwks_state()
                if self.command == "GET" and split.path == "/readyz"
                else None
            )
            status, payload = asyncio.run(
                dispatch_request(
                    config or self._config(),
                    self.command,
                    split.path,
                    query,
                    headers,
                    body,
                    execution_proof_jwks=jwks,
                )
            )
        except IngressError as error:
            status, payload = (
                error.status,
                _error(error.code, error.message, request_id),
            )
        except Exception:
            LOGGER.exception("agent HTTP host failed")
            status, payload = (
                503,
                _error("agent_unavailable", "Agent is unavailable", request_id),
            )
        self._json_response(status, payload)

    def do_GET(self) -> None:  # noqa: N802
        self._serve()

    def do_HEAD(self) -> None:  # noqa: N802
        self._serve()

    def do_POST(self) -> None:  # noqa: N802
        self._serve()

    def _jwks_method_only(self) -> None:
        self._serve()

    do_PUT = _jwks_method_only
    do_PATCH = _jwks_method_only
    do_DELETE = _jwks_method_only
    do_OPTIONS = _jwks_method_only
    do_TRACE = _jwks_method_only

    def __getattr__(self, name: str) -> object:
        if name.startswith("do_"):
            return self._serve
        raise AttributeError(name)

    def log_request(self, code: int | str = "-", size: int | str = "-") -> None:
        del size
        path = self.path.partition("?")[0]
        if path == _JWKS_PATH:
            route_class = "execution_proof_jwks"
        elif path == "/healthz":
            route_class = "health"
        elif path == "/readyz":
            route_class = "readiness"
        elif _known_business_route(self.command, path):
            route_class = "business"
        else:
            route_class = "unknown"
        LOGGER.info(
            "agent_http_access method=%s route_class=%s status=%s request_id=%s",
            _safe_method(self.command),
            route_class,
            code,
            _request_id_log_reference(self._request_id_value()),
        )

    def log_message(self, format: str, *args: object) -> None:
        del format, args
        LOGGER.info("agent_http_protocol_event")


class AgentHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(
        self,
        address: tuple[str, int],
        config: AgentConfig,
        execution_proof_jwks: ExecutionProofJwksState,
    ) -> None:
        self._handler_condition = threading.Condition()
        self._active_handlers = 0
        self._draining = False
        self.kokoro_config = config
        self.execution_proof_jwks = execution_proof_jwks
        super().__init__(address, AgentRequestHandler)

    def process_request(
        self, request: _Request, client_address: _ClientAddress
    ) -> None:
        with self._handler_condition:
            self._active_handlers += 1
        try:
            super().process_request(request, client_address)
        except BaseException:
            with self._handler_condition:
                self._active_handlers -= 1
                self._handler_condition.notify_all()
            raise

    def process_request_thread(
        self, request: _Request, client_address: _ClientAddress
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self._handler_condition:
                self._active_handlers -= 1
                self._handler_condition.notify_all()

    def start_draining(self) -> None:
        with self._handler_condition:
            self._draining = True

    def try_admit_request(self) -> bool:
        with self._handler_condition:
            return not self._draining

    def wait_for_active_handlers(self, timeout: float) -> bool:
        with self._handler_condition:
            return self._handler_condition.wait_for(
                lambda: self._active_handlers == 0, timeout=max(0.0, timeout)
            )


def create_http_server(
    config: AgentConfig,
    host: str,
    port: int,
    *,
    execution_proof_jwks: ExecutionProofJwksState | None = None,
) -> AgentHttpServer:
    return AgentHttpServer(
        (host, port),
        config,
        execution_proof_jwks or ExecutionProofJwksState.unavailable(),
    )


__all__ = ["AgentRequestHandler", "create_http_server", "dispatch_request"]
