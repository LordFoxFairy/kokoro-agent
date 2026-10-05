"""Pure, bounded admission of materialized chat-completions request bytes."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
import json
import math
from urllib.parse import urlsplit

from pydantic import JsonValue, TypeAdapter

from kokoro_agent.model.call_models import ModelCallInputInvalid

REQUEST_PROFILE = "chat-completions-materialized-json-v1"
MAX_BODY_BYTES = 8_388_608
MAX_DEPTH = 64
MAX_NODES = 131_072
MAX_MEMBERS = 16_384
MAX_STRING_TOKEN = 4_194_304
MAX_KEY_TOKEN = 4_096
MAX_NUMBER_TOKEN = 128
_OBJECT = TypeAdapter(dict[str, JsonValue])
_HEADERS = frozenset(
    "host content-length content-type accept accept-encoding connection user-agent "
    "authorization x-stainless-lang x-stainless-package-version x-stainless-os "
    "x-stainless-arch x-stainless-runtime x-stainless-runtime-version "
    "x-stainless-async x-stainless-retry-count x-stainless-read-timeout "
    "x-stainless-raw-response x-stainless-helper-method".split()
)


def _invalid() -> ModelCallInputInvalid:
    return ModelCallInputInvalid("model request profile is invalid")


def _preflight(raw: bytes) -> None:
    """Count tokens/resources only; stdlib JSON remains the syntax authority."""
    stack: list[tuple[int, int]] = []
    nodes = 0
    i = 0
    while i < len(raw):
        c = raw[i]
        if c in b" \t\r\n,:":
            i += 1
            continue
        if c in b"}]":
            if stack:
                stack.pop()
            i += 1
            continue
        start = i
        key = False
        if c == 34:
            i += 1
            while i < len(raw):
                if raw[i] == 92:
                    i += 2
                elif raw[i] == 34:
                    i += 1
                    break
                else:
                    i += 1
            after = i
            while after < len(raw) and raw[after] in b" \t\r\n":
                after += 1
            key = after < len(raw) and raw[after] == 58
            if i - start > (MAX_KEY_TOKEN if key else MAX_STRING_TOKEN):
                raise _invalid()
        elif c in b"-0123456789":
            while i < len(raw) and raw[i] in b"-+0123456789.eE":
                i += 1
            if i - start > MAX_NUMBER_TOKEN:
                raise _invalid()
        elif c in b"[{":
            i += 1
        else:
            while (
                i < len(raw)
                and raw[i] in b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
            ):
                i += 1
            if i == start:
                raise _invalid()
        nodes += 1
        if nodes > MAX_NODES:
            raise _invalid()
        if stack and (stack[-1][0] == 91 or key):
            kind, count = stack[-1]
            count += 1
            if count > MAX_MEMBERS:
                raise _invalid()
            stack[-1] = kind, count
        if c in b"[{":
            stack.append((c, 0))
            if len(stack) > MAX_DEPTH:
                raise _invalid()


def _pairs(items: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, value in items:
        if key in result:
            raise _invalid()
        result[key] = value
    return result


def _float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise _invalid()
    return result


def _constant(_: str) -> JsonValue:
    raise _invalid()


def _verify_tree(root: dict[str, JsonValue]) -> None:
    # Iterators retain only the bounded container ancestry, not a second node list.
    stack: list[Iterator[JsonValue]] = [iter((root,))]
    nodes = 0
    while stack:
        try:
            item = next(stack[-1])
        except StopIteration:
            stack.pop()
            continue
        nodes += 1
        if isinstance(item, dict):
            nodes += len(item)
            for key in item:
                key.encode("utf-8", errors="strict")
            children = iter(item.values())
        elif isinstance(item, list):
            children = iter(item)
        else:
            if isinstance(item, str):
                item.encode("utf-8", errors="strict")
            if nodes > MAX_NODES:
                raise _invalid()
            continue
        if len(item) > MAX_MEMBERS or len(stack) > MAX_DEPTH or nodes > MAX_NODES:
            raise _invalid()
        stack.append(children)


def validate_request_profile(
    *,
    raw: bytes,
    method: str,
    endpoint: str,
    expected_endpoint: str,
    headers: Sequence[tuple[str, str]],
    model_name: str,
    max_output_tokens: int,
) -> None:
    """Validate only engineering limits; not tokens, authorization or a send permit."""
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_BODY_BYTES:
        raise _invalid()
    target = urlsplit(expected_endpoint)
    if (
        method != "POST"
        or endpoint != expected_endpoint
        or target.scheme not in {"http", "https"}
        or not target.hostname
        or target.username is not None
        or target.password is not None
        or target.query
        or target.fragment
    ):
        raise _invalid()
    values: dict[str, str] = {}
    for name, value in headers:
        name = name.lower()
        if name not in _HEADERS or name in values:
            raise _invalid()
        values[name] = value
    if (
        values.get("content-type") != "application/json"
        or values.get("content-length") != str(len(raw))
        or values.get("host") != target.netloc
        or values.get("accept") != "application/json"
        or values.get("x-stainless-retry-count", "0") != "0"
        or values.get("x-stainless-async", "async:asyncio")
        not in {"async:asyncio", "false"}
        or values.get("x-stainless-raw-response", "true") != "true"
        or values.get("x-stainless-helper-method", "chat.completions.parse")
        not in {"chat.completions.parse", "chat.completions.stream"}
    ):
        raise _invalid()
    try:
        _preflight(raw)
        decoded: object = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_pairs,
            parse_float=_float,
            parse_constant=_constant,
        )
        payload = _OBJECT.validate_python(decoded, strict=True)
        _verify_tree(payload)
        cap = payload.get("max_completion_tokens")
        if (
            type(payload.get("n")) is not int
            or payload["n"] != 1
            or payload.get("model") != model_name
            or type(cap) is not int
            or not 1 <= cap <= max_output_tokens
            or "max_tokens" in payload
            or "max_output_tokens" in payload
        ):
            raise ModelCallInputInvalid("model request execution bound is invalid")
    except ModelCallInputInvalid:
        raise
    except (ValueError, UnicodeError, RecursionError):
        raise _invalid() from None
