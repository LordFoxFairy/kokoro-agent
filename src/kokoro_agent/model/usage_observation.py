"""Invocation-local observations of SDK-consumed bytes, never financial permission."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from hashlib import sha256
from io import BytesIO
import json
import asyncio
import codecs
import math
from threading import RLock, get_ident
from types import MappingProxyType, TracebackType
from typing import Literal, NoReturn, Self
import zlib

import httpx
import rfc8785
import zstandard

from kokoro_agent.clients.system import ResolvedModel

_LIMIT = 4 * 1024 * 1024
_CHUNK = 64 * 1024
_MAX_COUNT = (1 << 63) - 1
_NAMES = (
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "cached_tokens",
    "reasoning_tokens",
)
_DOMAIN = b"kokoro-agent:usage-observation-projection:v1\n"
_REQUEST_KEY = "kokoro_agent_usage_observation"
Status = Literal["pending", "complete", "unresolved"]
Integrity = Literal["pending", "validated", "unverified_unconsumed_tail", "unresolved"]
Presence = Literal["missing", "null", "reported"]


@dataclass(frozen=True, slots=True)
class ObservedCount:
    presence: Presence
    value: int | None = None


def _empty_counts() -> Mapping[str, ObservedCount]:
    return MappingProxyType({name: ObservedCount("missing") for name in _NAMES})


@dataclass(frozen=True, slots=True)
class PhysicalUsageObservation:
    """Only a bounded semantic projection; no body, credential, or provider identity."""

    status: Status
    reason: str | None
    compression_integrity: Integrity
    usage_presence: Literal["missing", "null", "object"] = "missing"
    counts: Mapping[str, ObservedCount] = field(default_factory=_empty_counts)
    projection_digest: str | None = None
    service_tier: Literal["absent", "null", "default", "unsupported"] = "absent"
    unsupported_usage_category: bool = False


class _ObservationError(Exception):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _fail(reason: str) -> NoReturn:
    raise _ObservationError(reason)


class _JsonObject(dict[str, object]):
    """A JSON object actually built by the duplicate-checking decoder hook."""


def _object(pairs: list[tuple[str, object]]) -> _JsonObject:
    result = _JsonObject()
    for key, value in pairs:
        if key in result:
            _fail("invalid_json")
        result[key] = value
    return result


def _constant(_: str) -> NoReturn:
    _fail("invalid_json")


def _finite_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value):
        _fail("invalid_json")
    return value


def _document(raw: bytes) -> _JsonObject:
    # Check structural depth before json.loads can recurse or allocate nested objects.
    depth = 0
    quoted = escaped = False
    for byte in raw:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (123, 91):
            depth += 1
            if depth > 16:
                _fail("depth_budget_exhausted")
        elif byte in (125, 93):
            depth -= 1
    value: object = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_object,
        parse_constant=_constant,
        parse_float=_finite_float,
    )
    if not isinstance(value, _JsonObject):
        _fail("invalid_json")
    return value


def _count(source: dict[str, object], name: str) -> ObservedCount:
    if name not in source:
        return ObservedCount("missing")
    value = source[name]
    if value is None:
        return ObservedCount("null")
    if type(value) is not int or not 0 <= value <= _MAX_COUNT:
        _fail("invalid_usage")
    return ObservedCount("reported", value)


def _projection(document: dict[str, object]) -> PhysicalUsageObservation:
    raw = document.get("usage")
    presence: Literal["missing", "null", "object"] = (
        "missing" if "usage" not in document else "null" if raw is None else "object"
    )
    if raw is not None and not isinstance(raw, _JsonObject):
        _fail("invalid_usage")
    usage = raw if isinstance(raw, _JsonObject) else _JsonObject()
    counts = {name: _count(usage, name) for name in _NAMES[:3]}
    unknown = bool(
        usage.keys()
        - {*_NAMES[:3], "prompt_tokens_details", "completion_tokens_details"}
    )
    for container, member in (
        ("prompt_tokens_details", "cached_tokens"),
        ("completion_tokens_details", "reasoning_tokens"),
    ):
        details = usage.get(container)
        if details is not None and not isinstance(details, _JsonObject):
            _fail("invalid_usage")
        detail_object = details if isinstance(details, _JsonObject) else _JsonObject()
        unknown |= bool(detail_object.keys() - {member})
        counts[member] = _count(detail_object, member)
    input_count, output_count, total = (counts[name].value for name in _NAMES[:3])
    if input_count is not None and output_count is not None and total is not None:
        if input_count + output_count != total:
            _fail("invalid_usage")
    for subset, inclusive in (
        ("cached_tokens", input_count),
        ("reasoning_tokens", output_count),
    ):
        value = counts[subset].value
        if value is not None and inclusive is not None and value > inclusive:
            _fail("invalid_usage")
    tier: Literal["absent", "null", "default", "unsupported"] = "absent"
    if "service_tier" in document:
        tier = (
            "null"
            if document["service_tier"] is None
            else "default"
            if document["service_tier"] == "default"
            else "unsupported"
        )
    canonical = rfc8785.dumps(
        {
            "version": 1,
            "protocol": "openai_chat_completions",
            "usage_presence": presence,
            "counts": {
                name: {
                    "presence": item.presence,
                    **({"value": str(item.value)} if item.value is not None else {}),
                }
                for name, item in counts.items()
            },
            "service_tier": tier,
            "unsupported_usage_category": unknown,
        }
    )
    if len(canonical) > 4096:
        _fail("projection_budget_exhausted")
    return PhysicalUsageObservation(
        "complete",
        None,
        "pending",
        presence,
        MappingProxyType(counts),
        sha256(_DOMAIN + canonical).hexdigest(),
        tier,
        unknown,
    )


class _Parser:
    def __init__(self, streaming: bool) -> None:
        self.streaming = streaming
        self.utf8 = codecs.getincrementaldecoder("utf-8")()
        self.scratch = bytearray()
        self.data: list[bytes] = []
        self.event_size = 0
        self.events = 0
        self.done = False
        self.projection: PhysicalUsageObservation | None = None

    def feed(self, chunk: bytes) -> None:
        if not self.streaming:
            if len(self.scratch) + len(chunk) > _LIMIT:
                _fail("document_budget_exhausted")
            self.scratch.extend(chunk)
            return
        if self.done:
            return  # The decoder still drains all owned bytes for the output bound.
        self.utf8.decode(chunk, final=False)
        for byte in chunk:
            if self.done:
                break
            self.event_size += 1
            if self.event_size > _CHUNK:
                _fail("event_budget_exhausted")
            if byte != 10:
                self.scratch.append(byte)
                continue
            line = bytes(self.scratch).removesuffix(b"\r")
            self.scratch.clear()
            if not line:
                self._event()
                self.event_size = 0
            elif line.startswith(b"data:"):
                value = line[5:]
                self.data.append(value[1:] if value.startswith(b" ") else value)

    def _event(self) -> None:
        self.events += 1
        if self.events > 4096:
            _fail("event_count_exhausted")
        data = b"\n".join(self.data)
        self.data.clear()
        if not data:
            return
        if data == b"[DONE]":
            self.done = True
            return
        document = _document(data)
        if document.get("choices") != [] or document.get("usage") is None:
            return
        candidate = _projection(document)
        if (
            self.projection is not None
            and self.projection.projection_digest != candidate.projection_digest
        ):
            _fail("conflicting_usage")
        self.projection = candidate

    def finish(self) -> PhysicalUsageObservation:
        if not self.streaming:
            return _projection(_document(bytes(self.scratch)))
        self.utf8.decode(b"", final=True)
        if not self.done:
            _fail("missing_done")
        if self.projection is None:
            _fail("missing_final")
        return self.projection

    def clear(self) -> None:
        self.scratch.clear()
        self.data.clear()
        self.projection = None


# This private token is issued only after reading the entire same immutable buffer.
_PROOF = object()


@dataclass(frozen=True, slots=True)
class _BoundedZstd:
    buffer: bytes
    projection: PhysicalUsageObservation
    token: object


def _zstd_first(raw: bytes, parser: _Parser) -> _BoundedZstd:
    total = 0
    # Locked 0.25.0 C backend probe requires bytes, despite the public KiB wording.
    with BytesIO(raw) as source:
        with zstandard.ZstdDecompressor(max_window_size=_LIMIT).stream_reader(
            source, read_across_frames=True
        ) as reader:
            while chunk := reader.read(min(_CHUNK, _LIMIT - total + 1)):
                total += len(chunk)
                if total > _LIMIT:
                    _fail("output_budget_exhausted")
                parser.feed(chunk)
    projection = parser.finish()
    parser.clear()
    return _BoundedZstd(raw, projection, _PROOF)


def _zstd_integrity(proof: _BoundedZstd) -> bool:
    if proof.token is not _PROOF:
        _fail("invalid_bounded_proof")
    remaining = proof.buffer
    frames = 0
    while remaining:
        frames += 1
        if frames > 4096:
            _fail("frame_budget_exhausted")
        decoder = zstandard.ZstdDecompressor(max_window_size=_LIMIT).decompressobj(
            read_across_frames=False
        )
        # Safe only after the whole identical immutable buffer passed the bounded reader.
        output = decoder.decompress(remaining)
        del output
        if not decoder.eof:
            return False
        tail = decoder.unused_data
        if len(tail) >= len(remaining):
            _fail("invalid_encoding")
        del decoder
        remaining = tail
    return True


def _zlib_decode(raw: bytes, parser: _Parser, encoding: str) -> bool:
    wrapped = (
        len(raw) >= 2
        and (raw[0] & 15) == 8
        and int.from_bytes(raw[:2], "big") % 31 == 0
    )
    window = 31 if encoding == "gzip" else 15 if wrapped else -15
    decoder = zlib.decompressobj(window)
    pending = raw
    total = 0
    while True:
        requested = min(_CHUNK, _LIMIT - total + 1)
        chunk = decoder.decompress(pending, requested)
        total += len(chunk)
        if total > _LIMIT:
            _fail("output_budget_exhausted")
        parser.feed(chunk)
        pending = decoder.unconsumed_tail
        if decoder.eof:
            if decoder.unused_data:
                # HTTPX's gzip/deflate decoder does not expose concatenated members.
                _fail("unsupported_encoding_shape")
            return True
        if not pending and len(chunk) < requested:
            return False


def _observe(
    raw: bytes, encoding: str, streaming: bool, body_ended: bool
) -> PhysicalUsageObservation:
    parser = _Parser(streaming)
    try:
        if encoding == "zstd":
            proof = _zstd_first(raw, parser)
            projection = proof.projection
            validated = _zstd_integrity(proof)
        elif encoding in {"gzip", "deflate"}:
            validated = _zlib_decode(raw, parser, encoding)
            projection = parser.finish()
        elif encoding == "identity":
            for offset in range(0, len(raw), _CHUNK):
                parser.feed(raw[offset : offset + _CHUNK])
            projection = parser.finish()
            validated = True
        else:
            _fail("unsupported_encoding")
        if not streaming and (not body_ended or not validated):
            _fail("incomplete_response")
        if not validated and body_ended:
            _fail("invalid_encoding")
        integrity: Integrity = (
            "validated" if validated else "unverified_unconsumed_tail"
        )
        return PhysicalUsageObservation(
            "complete",
            None,
            integrity,
            projection.usage_presence,
            projection.counts,
            projection.projection_digest,
            projection.service_tier,
            projection.unsupported_usage_category,
        )
    finally:
        parser.clear()


_ACTIVE: ContextVar[_UsageCapture | None] = ContextVar(
    "model_usage_capture", default=None
)


def _current_task() -> asyncio.Task[object] | None:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None  # A synchronous SDK caller need not have an event loop.


class _UsageCapture:
    """One explicitly scoped physical response; inherited shared scopes are ambiguous."""

    def __init__(self, route: ResolvedModel) -> None:
        self._route = route
        self._lock = RLock()
        self._buffer = bytearray()
        self._result = PhysicalUsageObservation("pending", None, "pending")
        self._token: Token[_UsageCapture | None] | None = None
        self._used = False
        self._closed = False
        self._claimed = False
        self._response = False
        self._ended = False
        self._streaming = False
        self._encoding = "identity"
        self._failure: str | None = None
        self._misuse = False
        self._completed = False
        self._owner_thread: int | None = None
        self._owner_task: asyncio.Task[object] | None = None

    def __enter__(self) -> Self:
        if self._used:
            raise ValueError("usage capture cannot be reused")
        self._used = True
        self._owner_thread = get_ident()
        self._owner_task = _current_task()
        self._token = _ACTIVE.set(self)
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._token is not None:
            _ACTIVE.reset(self._token)
            self._token = None
        with self._lock:
            self._closed = True
            try:
                if kind is not None:
                    self._failure = "scope_interrupted"
                if self._failure is not None:
                    _fail(self._failure)
                if not self._completed:
                    _fail("sdk_consumption_incomplete")
                if not self._response:
                    _fail("missing_response")
                raw = bytes(self._buffer)
                self._buffer.clear()
                self._result = _observe(
                    raw, self._encoding, self._streaming, self._ended
                )
            except _ObservationError as failure:
                self._result = PhysicalUsageObservation(
                    "unresolved", failure.reason, "unresolved"
                )
            except (ValueError, UnicodeError, zlib.error, zstandard.ZstdError):
                self._result = PhysicalUsageObservation(
                    "unresolved", "invalid_response", "unresolved"
                )
            finally:
                self._buffer.clear()
                self._owner_task = None
                self._owner_thread = None

    def mark_sdk_consumption_complete(self) -> None:
        """The scope owner attests normal SDK return/exhaustion, not financial truth."""
        with self._lock:
            if (
                self._closed
                or self._token is None
                or _ACTIVE.get() is not self
                or get_ident() != self._owner_thread
                or _current_task() is not self._owner_task
                or not self._response
                or self._misuse
            ):
                if not self._closed and self._token is not None:
                    self._misuse = True
                    self._failure = (
                        "ambiguous_response"
                        if self._failure == "ambiguous_response"
                        else "capture_misuse"
                    )
                    self._buffer.clear()
                raise ValueError("invalid usage capture completion")
            # Observer failures are not caller errors. Keep the original unknown
            # reason; marking SDK consumption never repairs or certifies usage.
            self._completed = True

    def snapshot(self) -> PhysicalUsageObservation:
        with self._lock:
            return self._result

    def claim_request(self, route: ResolvedModel) -> None:
        with self._lock:
            if self._closed:
                return
            if self._claimed or route is not self._route:
                self._misuse = True
                self._failure = "ambiguous_response"
                self.abort("ambiguous_response")
            self._claimed = True

    def abort(self, reason: str) -> None:
        with self._lock:
            if not self._closed:
                self._failure = self._failure or reason
                self._buffer.clear()

    def receive_response(self, response: httpx.Response) -> None:
        with self._lock:
            if self._closed:
                return
            if self._response:
                self._misuse = True
                self._failure = "ambiguous_response"
                self.abort("ambiguous_response")
            self._response = True
            self._encoding = (
                response.headers.get("content-encoding", "identity").strip().lower()
            )
            content_type = (
                response.headers.get("content-type", "")
                .split(";", 1)[0]
                .strip()
                .lower()
            )
            self._streaming = content_type == "text/event-stream"
            if response.status_code != 200 or content_type not in {
                "application/json",
                "text/event-stream",
            }:
                self.abort("unsupported_response")
            if self._encoding not in {"identity", "gzip", "deflate", "zstd"}:
                self.abort("unsupported_encoding")

    def feed(self, chunk: bytes) -> None:
        with self._lock:
            if self._closed or self._failure is not None:
                return
            if len(self._buffer) + len(chunk) > _LIMIT:
                self.abort("input_budget_exhausted")
            else:
                self._buffer.extend(chunk)

    def end(self) -> None:
        with self._lock:
            if not self._closed:
                self._ended = True


def capture_model_usage(route: ResolvedModel) -> _UsageCapture:
    """Consume one SDK invocation, then explicitly mark completion before scope exit."""
    return _UsageCapture(route)


def claim_usage_request(request: httpx.Request, route: ResolvedModel) -> None:
    capture = _ACTIVE.get()
    if capture is not None:
        capture.claim_request(route)
        request.extensions[_REQUEST_KEY] = capture


class _SyncTap(httpx.SyncByteStream):
    def __init__(self, source: httpx.SyncByteStream, capture: _UsageCapture) -> None:
        self.source = source
        self.capture = capture
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        try:
            for chunk in self.source:
                self.capture.feed(chunk)
                yield chunk
            self.capture.end()
        except GeneratorExit:
            raise
        except BaseException:
            self.capture.abort("transport_interrupted")
            raise

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            try:
                self.source.close()
            except BaseException:
                self.capture.abort("transport_interrupted")
                raise


class _AsyncTap(httpx.AsyncByteStream):
    def __init__(self, source: httpx.AsyncByteStream, capture: _UsageCapture) -> None:
        self.source = source
        self.capture = capture
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in self.source:
                self.capture.feed(chunk)
                yield chunk
            self.capture.end()
        except GeneratorExit:
            raise
        except BaseException:
            self.capture.abort("transport_interrupted")
            raise

    async def aclose(self) -> None:
        if not self.closed:
            self.closed = True
            try:
                await self.source.aclose()
            except BaseException:
                self.capture.abort("transport_interrupted")
                raise


def observe_usage_response(response: httpx.Response) -> None:
    capture = response.request.extensions.pop(_REQUEST_KEY, None)
    if not isinstance(capture, _UsageCapture):
        return
    capture.receive_response(response)
    source = response.stream
    if isinstance(source, httpx.SyncByteStream):
        response.stream = _SyncTap(source, capture)
    else:
        response.stream = _AsyncTap(source, capture)


async def observe_usage_response_async(response: httpx.Response) -> None:
    observe_usage_response(response)
