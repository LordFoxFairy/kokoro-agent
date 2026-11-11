"""Bounded legacy SSE SDK factory; no RPC dispatch, retries or shared session cache."""

from __future__ import annotations

import codecs
from collections.abc import Generator, AsyncIterator
from contextlib import contextmanager
from contextvars import ContextVar
import logging
from pathlib import Path
import threading
from types import TracebackType
from urllib.parse import urljoin, urlsplit

import httpx
from httpx._client import USE_CLIENT_DEFAULT, UseClientDefault
from httpx._types import AuthTypes
from httpx_sse._decoders import SSEDecoder, SSELineDecoder
import mcp

from kokoro_agent.mcp.egress import AddressResolver, GuardedTransport, default_resolve

_ENDPOINT_BYTES = 8192
_BODY_BYTES = 4_194_304
_SDK_SCOPE: ContextVar[bool] = ContextVar("mcp_sse_sdk_scope", default=False)
_IO_SCOPE: ContextVar[str | None] = ContextVar("mcp_sse_io_scope", default=None)
_MCP_ROOT = Path(mcp.__file__).parent
_HANDLER_LOCK = threading.RLock()
_HANDLER_LEASES: dict[logging.Handler, int] = {}


class _SafeSdkRecords(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not _SDK_SCOPE.get():
            return True
        # MCP shared/session logs on root; client/session uses "client".
        # Provenance, not logger name or secret-bearing message text, selects it.
        source = Path(record.pathname)
        sdk = source.is_relative_to(_MCP_ROOT)
        io_phase = _IO_SCOPE.get()
        io_source = record.name == "httpx" or record.name.startswith("httpcore.")
        if sdk or io_source and io_phase is not None:
            record.msg = "MCP_SSE phase=%s"
            record.args = (io_phase or "session",)
            record.exc_info = None
            record.exc_text = None
            record.stack_info = None
            record.__dict__.pop("message", None)
        return True


_FILTER = _SafeSdkRecords()


@contextmanager
def sse_sdk_scope() -> Generator[None, None, None]:
    """Sanitize at producer handlers (before QueueHandler.enqueue), not a parent logger."""
    # Worker main configures logging before runtime assembly. Otherwise a
    # suppressed stdlib info/debug call could lazily create an unfiltered sink.
    # Never configure or patch the caller's logging to enter an SDK session.
    root = logging.getLogger()
    if not root.handlers:
        raise ValueError("MCP_SSE_LOGGING_UNAVAILABLE")
    handlers: set[logging.Handler] = set(root.handlers)
    for item in list(logging.Logger.manager.loggerDict.values()):
        if isinstance(item, logging.Logger):
            handlers.update(item.handlers)
    if logging.lastResort is not None:
        handlers.add(logging.lastResort)
    with _HANDLER_LOCK:
        for handler in handlers:
            count = _HANDLER_LEASES.get(handler, 0)
            if count == 0:
                handler.addFilter(_FILTER)
            _HANDLER_LEASES[handler] = count + 1
    token = _SDK_SCOPE.set(True)
    try:
        yield
    finally:
        # Caller encloses the SDK context manager, so its children have exited.
        _SDK_SCOPE.reset(token)
        with _HANDLER_LOCK:
            for handler in handlers:
                count = _HANDLER_LEASES[handler] - 1
                if count:
                    _HANDLER_LEASES[handler] = count
                else:
                    del _HANDLER_LEASES[handler]
                    handler.removeFilter(_FILTER)


@contextmanager
def _io_scope(phase: str) -> Generator[None, None, None]:
    token = _IO_SCOPE.set(phase)
    try:
        yield
    finally:
        _IO_SCOPE.reset(token)


def _url(value: str, *, base: str | None = None) -> httpx.URL:
    try:
        if (
            not value
            or len(value.encode("utf-8")) > _ENDPOINT_BYTES
            or any(ord(c) < 32 or ord(c) == 127 for c in value)
            or "\\" in value
            or "#" in value
        ):
            raise ValueError
        resolved = urljoin(base, value) if base is not None else value
        if (
            len(resolved.encode("utf-8")) > _ENDPOINT_BYTES
            or "@" in urlsplit(resolved).netloc
        ):
            raise ValueError
        result = httpx.URL(resolved)
        if (
            not result.is_absolute_url
            or result.scheme not in {"https", "http"}
            or not result.host
            or result.port is not None
            and not 1 <= result.port <= 65535
        ):
            raise ValueError
        return result
    except (ValueError, UnicodeError, httpx.InvalidURL):
        raise ValueError("MCP_SSE_ENDPOINT_INVALID") from None


def _origin(url: httpx.URL) -> tuple[str, str, int]:
    return url.scheme, url.host, url.port or (443 if url.scheme == "https" else 80)


class _Endpoint:
    def __init__(self, canonical: str) -> None:
        self.canonical = _url(canonical)
        self.post: httpx.URL | None = None
        self.started = False
        self.failed = False

    def advertise(self, value: str) -> None:
        target = _url(value, base=str(self.canonical))
        if self.post is not None or _origin(target) != _origin(self.canonical):
            raise ValueError("MCP_SSE_ENDPOINT_INVALID")
        self.post = target

    def request(self, request: httpx.Request) -> None:
        # GuardedTransport mutates host -> pinned IP; compare BEFORE entering it.
        if self.failed:
            raise ValueError("MCP_SSE_SESSION_RETIRED")
        target = _url(str(request.url))
        if _origin(target) != _origin(self.canonical):
            raise ValueError("MCP_SSE_ENDPOINT_INVALID")
        if request.method == "GET" and not self.started and target == self.canonical:
            self.started = True
        elif request.method != "POST" or self.post is None or target != self.post:
            raise ValueError("MCP_SSE_ENDPOINT_INVALID")


class _Observer:
    def __init__(self, endpoint: _Endpoint, encoding: str) -> None:
        # Same locked decoder semantics as httpx_sse: CR/LF/CRLF, comments,
        # multi-data join/event reset and replacement UTF-8 decoding, without
        # re-encoding bytes or imposing the URL bound on JSON-RPC message frames.
        self.text = codecs.getincrementaldecoder(encoding)(errors="replace")
        self.lines = SSELineDecoder()
        self.events = SSEDecoder()
        self.endpoint = endpoint

    def _lines(self, lines: list[str]) -> None:
        for line in lines:
            event = self.events.decode(line.rstrip("\n"))
            if event is not None and event.event == "endpoint":
                self.endpoint.advertise(event.data)

    def feed(self, chunk: bytes) -> None:
        self._lines(self.lines.decode(self.text.decode(chunk)))

    def finish(self) -> None:
        self._lines(self.lines.decode(self.text.decode(b"", final=True)))
        self._lines(self.lines.flush())


class _BoundedStream(httpx.AsyncByteStream):
    def __init__(
        self,
        inner: httpx.AsyncByteStream,
        observer: _Observer | None,
        endpoint: _Endpoint,
    ) -> None:
        self.inner, self.observer, self.endpoint = inner, observer, endpoint
        self.total = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in self._read():
                yield chunk
        except Exception:
            self.endpoint.failed = True
            raise

    async def _read(self) -> AsyncIterator[bytes]:
        iterator = self.inner.__aiter__()
        while True:
            with _io_scope("read"):
                try:
                    chunk = await anext(iterator)
                except StopAsyncIteration:
                    if self.observer is not None:
                        self.observer.finish()
                    return
            self.total += len(chunk)
            if self.total > _BODY_BYTES:
                raise ValueError("MCP_SSE_BODY_TOO_LARGE")
            if self.observer is not None:
                self.observer.feed(chunk)
            yield chunk

    async def aclose(self) -> None:
        with _io_scope("close"):
            await self.inner.aclose()


class _Transport(httpx.AsyncBaseTransport):
    def __init__(self, inner: httpx.AsyncBaseTransport, endpoint: _Endpoint) -> None:
        self.inner, self.endpoint = inner, endpoint

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        try:
            self.endpoint.request(request)
            response = await self.inner.handle_async_request(request)
        except Exception:
            self.endpoint.failed = True
            raise
        try:
            if not 200 <= response.status_code < 300:
                self.endpoint.failed = True
            if response.is_stream_consumed:
                raise ValueError("MCP_SSE_RESPONSE_INVALID")
            encoding = response.headers.get_list("content-encoding")
            if encoding and (
                len(encoding) != 1 or encoding[0].strip().lower() != "identity"
            ):
                raise ValueError("MCP_SSE_RESPONSE_INVALID")
            if 300 <= response.status_code < 400:
                raise ValueError("MCP_SSE_RESPONSE_INVALID")
            length = response.headers.get("content-length")
            if length is not None and (
                not length.isascii()
                or not length.isdecimal()
                or int(length) > _BODY_BYTES
            ):
                raise ValueError("MCP_SSE_BODY_TOO_LARGE")
            if not isinstance(response.stream, httpx.AsyncByteStream):
                raise ValueError("MCP_SSE_RESPONSE_INVALID")
            observer = (
                _Observer(self.endpoint, response.encoding or "utf-8")
                if request.method == "GET"
                else None
            )
            response.stream = _BoundedStream(response.stream, observer, self.endpoint)
            return response
        except Exception:
            self.endpoint.failed = True
            await response.aclose()
            raise

    async def aclose(self) -> None:
        with _io_scope("close"):
            await self.inner.aclose()
        self.endpoint.post = None


class _Client(httpx.AsyncClient):
    async def send(
        self,
        request: httpx.Request,
        *,
        stream: bool = False,
        auth: AuthTypes | UseClientDefault | None = USE_CLIENT_DEFAULT,
        follow_redirects: bool | UseClientDefault = USE_CLIENT_DEFAULT,
    ) -> httpx.Response:
        with _io_scope("get" if request.method == "GET" else "post"):
            try:
                if follow_redirects is True:
                    raise ValueError("MCP_SSE_RESPONSE_INVALID")
                return await super().send(
                    request, stream=stream, auth=auth, follow_redirects=False
                )
            finally:
                request.headers.clear()
                self.cookies.clear()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        try:
            with _io_scope("close"):
                await super().__aexit__(exc_type, exc_value, traceback)
        finally:
            self.headers.clear()
            self.cookies.clear()

    async def aclose(self) -> None:
        try:
            with _io_scope("close"):
                await super().aclose()
        finally:
            self.headers.clear()
            self.cookies.clear()


class SseClientFactory:
    """One native SDK session with a fail-closed latch for swallowed POST errors."""

    def __init__(
        self,
        canonical_endpoint: str,
        *,
        mode: str,
        transport: httpx.AsyncBaseTransport | None,
        resolver: AddressResolver | None,
    ) -> None:
        self.endpoint = _Endpoint(canonical_endpoint)
        if mode != "off" and self.endpoint.canonical.scheme != "https":
            raise ValueError("MCP_SSE_ENDPOINT_INVALID")
        self.mode, self.transport, self.resolver = mode, transport, resolver
        self.used = False

    def raise_if_failed(self) -> None:
        if self.endpoint.failed:
            raise ValueError("MCP_SSE_SESSION_FAILED")

    def __call__(
        self,
        headers: dict[str, str] | None = None,
        timeout: httpx.Timeout | None = None,
        auth: httpx.Auth | None = None,
    ) -> httpx.AsyncClient:
        if self.used:
            raise ValueError("MCP_SSE_SESSION_RETIRED")
        self.used = True
        selected = httpx.Headers(headers)
        if "accept-encoding" not in selected:
            selected["accept-encoding"] = "identity"
        inner = (
            self.transport
            if self.transport is not None
            else httpx.AsyncHTTPTransport(verify=True, trust_env=False, retries=0)
        )
        if self.mode != "off":
            inner = GuardedTransport(inner, self.resolver or default_resolve)
        return _Client(
            transport=_Transport(inner, self.endpoint),
            headers=selected,
            auth=auth,
            timeout=timeout or httpx.Timeout(5, read=300),
            follow_redirects=False,
            trust_env=False,
        )


def build_sse_client_factory(
    canonical_endpoint: str,
    *,
    mode: str,
    transport: httpx.AsyncBaseTransport | None = None,
    resolver: AddressResolver | None = None,
) -> SseClientFactory:
    """Explicit transport/resolver are owned fixture ports, never a runtime URL registry."""
    return SseClientFactory(
        canonical_endpoint, mode=mode, transport=transport, resolver=resolver
    )
