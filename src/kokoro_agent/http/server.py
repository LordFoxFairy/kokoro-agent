"""Small standard-library HTTP host for the Agent business ingress.

The worker and this ingress are separate processes from one package.  The
host intentionally has no framework-specific route magic: every public path
is explicit, bounded, and delegates to :class:`AgentIngress`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Protocol
from urllib.parse import parse_qs, urlsplit

from pydantic import SecretStr, TypeAdapter

from kokoro_agent.chat.query import ChatQuery, ChatQueryRequest, ChatSessionListRequest
from kokoro_agent.contract import ExecutionIdentity, IdentityRef, REQUESTS_STREAM
from kokoro_agent.contract.control import IdentityKind
from kokoro_agent.http.ingress import AgentIngress, IngressError
from kokoro_agent.storage.ledger import LedgerSettings, make_ledger
from kokoro_agent.chat.store import ChatStoreSettings, make_chat_store
from kokoro_agent.streams.factory import StreamSettings, make_stream

LOGGER = logging.getLogger(__name__)
_RUN_CONTROL = re.compile(r"^/v1/runs/([^/]+)/control$")
_RUN_EVENTS = re.compile(r"^/v1/runs/([^/]+)/events$")
_SESSION_MESSAGES = re.compile(r"^/v1/sessions/([^/]+)/messages$")
_SESSION_EVENTS = re.compile(r"^/v1/sessions/([^/]+)/events$")
_MAX_BODY = 1024 * 1024


class AgentConfig(Protocol):
    @property
    def stream(self) -> StreamSettings: ...

    @property
    def ledger(self) -> LedgerSettings: ...

    @property
    def database_url(self) -> str: ...

    @property
    def database_schema(self) -> str: ...

    @property
    def internal_secret_agent(self) -> SecretStr | None: ...


_JSON_OBJECT = TypeAdapter(dict[str, object])


def _request_id(headers: Mapping[str, str]) -> str:
    value = headers.get("x-kokoro-request-id", "").strip()
    return value or "agent-ingress-request"


def _envelope(data: object, request_id: str) -> dict[str, object]:
    return {"data": data, "meta": {"request_id": request_id}}


def _error(code: str, message: str, request_id: str) -> dict[str, object]:
    return {"error": {"code": code, "message": message}, "meta": {"request_id": request_id}}


def _identity(headers: Mapping[str, str]) -> ExecutionIdentity:
    tenant = headers.get("x-kokoro-tenant-ref", "").strip()
    subject = headers.get("x-kokoro-subject-ref", "").strip()
    actor = headers.get("x-kokoro-actor-ref", "").strip()
    assertion = headers.get("x-kokoro-identity-assertion-ref", "").strip()
    if not tenant or not subject or not actor or not assertion:
        raise IngressError(401, "identity_required", "Trusted execution identity headers are required")
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


def _page(headers: Mapping[str, str], query: Mapping[str, list[str]], session_id: str) -> ChatQueryRequest:
    def integer(name: str, default: int) -> int:
        raw = query.get(name, [str(default)])[0]
        try:
            return int(raw)
        except (TypeError, ValueError) as error:
            raise IngressError(400, "invalid_page", f"{name} must be an integer") from error

    return ChatQueryRequest(
        execution_identity=_identity(headers),
        session_id=session_id,
        after_seq=integer("after_seq", 0),
        limit=integer("limit", 200),
    )


def _session_list_page(
    headers: Mapping[str, str], query: Mapping[str, list[str]]
) -> ChatSessionListRequest:
    def integer(name: str, default: int) -> int:
        raw = query.get(name, [str(default)])[0]
        try:
            return int(raw)
        except (TypeError, ValueError) as error:
            raise IngressError(400, "invalid_page", f"{name} must be an integer") from error

    project_ref = query.get("project_ref", [None])[0]
    if project_ref == "":
        project_ref = None
    return ChatSessionListRequest(
        execution_identity=_identity(headers),
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
) -> tuple[int, dict[str, object]]:
    """Execute one request with short-lived owner connections.

    Short-lived connections keep the synchronous stdlib host independent from
    an asyncio event loop owned by another process and make shutdown reliable.
    Redis/PG are still used only through Agent-owned ports.
    """
    request_id = _request_id(headers)
    if method == "GET" and path == "/healthz":
        return 200, {"status": "ok", "service": "kokoro-agent"}
    secret = config.internal_secret_agent
    if secret is None or not secret.get_secret_value().strip():
        return 503, _error(
            "service_auth_not_configured",
            "Agent ingress service authentication is not configured",
            request_id,
        )
    if headers.get("x-kokoro-service") != "kokoro-bff":
        return 403, _error("service_auth_failed", "Agent ingress authentication failed", request_id)
    if headers.get("x-kokoro-internal-secret") != secret.get_secret_value():
        return 403, _error("service_auth_failed", "Agent ingress authentication failed", request_id)
    bus = make_stream(config.stream)
    try:
        async with (
            make_ledger(config.ledger) as ledger,
            make_chat_store(
                ChatStoreSettings(
                    database_url=config.database_url,
                    schema_name=config.database_schema,
                )
            ) as chat_store,
        ):
            if method == "GET" and path == "/readyz":
                await bus.read_all(REQUESTS_STREAM)
                return 200, {"status": "ready", "service": "kokoro-agent"}
            ingress = AgentIngress(bus=bus, ledger=ledger, chat_query=ChatQuery(chat_store))
            if method == "GET" and path == "/v1/sessions":
                result = await ingress.list_sessions(_session_list_page(headers, query))
                return 200, _envelope(result.model_dump(mode="json"), request_id)
            if method == "POST" and path == "/v1/runs":
                receipt = await ingress.launch(body or {})
                return 202, _envelope(
                    {"run_id": receipt.run_id, "session_id": receipt.session_id, "replayed": receipt.replayed},
                    request_id,
                )
            match = _RUN_CONTROL.fullmatch(path)
            if method == "POST" and match is not None:
                return 202, _envelope(
                    await ingress.control(match.group(1), body or {}), request_id
                )
            match = _RUN_EVENTS.fullmatch(path)
            if method == "GET" and match is not None:
                return 200, _envelope(
                    await ingress.evidence(
                        match.group(1),
                        after_seq=int(query.get("after_seq", ["0"])[0]),
                        limit=int(query.get("limit", ["200"])[0]),
                    ),
                    request_id,
                )
            match = _SESSION_MESSAGES.fullmatch(path)
            if method == "GET" and match is not None:
                result = await ingress.history(_page(headers, query, match.group(1)))
                return 200, _envelope(result.model_dump(mode="json"), request_id)
            match = _SESSION_EVENTS.fullmatch(path)
            if method == "GET" and match is not None:
                result = await ingress.replay(_page(headers, query, match.group(1)))
                return 200, _envelope(result.model_dump(mode="json"), request_id)
            return 404, _error("route_not_found", "Agent route was not found", request_id)
    except IngressError as error:
        return error.status, _error(error.code, error.message, request_id)
    except (ValueError, TypeError) as error:
        LOGGER.info("agent ingress request validation failed: %s", error)
        return 400, _error("invalid_request", "Request does not match the Agent v1 contract", request_id)
    except Exception:
        LOGGER.exception("agent ingress request failed")
        return 503, _error("agent_unavailable", "Agent dependencies are unavailable", request_id)
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
            raise RuntimeError("AgentRequestHandler is missing AppConfig")
        return config

    def _body(self) -> dict[str, object] | None:
        length = int(self.headers.get("content-length", "0"))
        if length < 0 or length > _MAX_BODY:
            raise IngressError(413, "request_body_too_large", "Request body is too large")
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise IngressError(400, "invalid_json", "Request body must be valid JSON") from error
        if not isinstance(value, dict):
            raise IngressError(400, "invalid_json", "Request body must be a JSON object")
        try:
            return _JSON_OBJECT.validate_python(value)
        except ValueError as error:
            raise IngressError(400, "invalid_json", "Request object keys must be strings") from error

    def _serve(self) -> None:
        headers = {
            name: self.headers.get(name, "") or ""
            for name in (
                "x-kokoro-request-id",
                "x-kokoro-service",
                "x-kokoro-internal-secret",
                "x-kokoro-tenant-ref",
                "x-kokoro-subject-ref",
                "x-kokoro-actor-ref",
                "x-kokoro-subject-kind",
                "x-kokoro-actor-kind",
                "x-kokoro-identity-assertion-ref",
            )
        }
        request_id = _request_id(headers)
        try:
            split = urlsplit(self.path)
            query = parse_qs(split.query, keep_blank_values=True)
            body = self._body() if self.command in {"POST", "PUT", "PATCH"} else None
            status, payload = asyncio.run(
                dispatch_request(self._config(), self.command, split.path, query, headers, body)
            )
        except IngressError as error:
            status, payload = error.status, _error(error.code, error.message, request_id)
        except Exception:
            LOGGER.exception("agent HTTP host failed")
            status, payload = 503, _error("agent_unavailable", "Agent is unavailable", request_id)
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("cache-control", "no-store")
        self.send_header("content-length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self) -> None:  # noqa: N802
        self._serve()

    def do_POST(self) -> None:  # noqa: N802
        self._serve()

    def log_message(self, format: str, *args: object) -> None:
        LOGGER.info("%s - %s", self.address_string(), format % args)


class AgentHttpServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], config: AgentConfig) -> None:
        self.kokoro_config = config
        super().__init__(address, AgentRequestHandler)


def create_http_server(config: AgentConfig, host: str, port: int) -> ThreadingHTTPServer:
    return AgentHttpServer((host, port), config)


__all__ = ["AgentRequestHandler", "create_http_server", "dispatch_request"]
