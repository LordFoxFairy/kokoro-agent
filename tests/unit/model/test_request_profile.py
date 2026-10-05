"""Pure final-body profile limits; these are engineering, not token bounds."""

from __future__ import annotations

import hashlib
import json

import pytest
import rfc8785

from kokoro_agent.model.call_artifacts import encode_final_request_digest
from kokoro_agent.model.call_models import ModelCallInputInvalid
from kokoro_agent.model.request_profile import validate_request_profile

_ENDPOINT = "http://synthetic.invalid/v1/chat/completions"
_PREFIX = b'{"model":"qa","n":1,"max_completion_tokens":1,"extra":'


def _validate(raw: bytes, extras: tuple[tuple[str, str], ...] = ()) -> None:
    validate_request_profile(
        raw=raw,
        method="POST",
        endpoint=_ENDPOINT,
        expected_endpoint=_ENDPOINT,
        headers=(
            ("Host", "synthetic.invalid"),
            ("Content-Length", str(len(raw))),
            ("Content-Type", "application/json"),
            ("Accept", "application/json"),
            *extras,
        ),
        model_name="qa",
        max_output_tokens=16,
    )


def _body(extra: bytes = b"null") -> bytes:
    return _PREFIX + extra + b"}"


@pytest.mark.parametrize("extra", [b"null", b"true", b"{}", b"[]", '"你好"'.encode()])
def test_valid_profile(extra: bytes) -> None:
    _validate(_body(extra))


@pytest.mark.parametrize(
    "extra",
    [
        b"NaN",
        b"Infinity",
        b"-Infinity",
        b"1e999",
        b'{"x":1,"\\u0078":2}',
        b'"\\ud800"',
        b'"\\udfff"',
        b'"\xff"',
        b"01",
        b"1.",
        b'"unterminated',
        b'{"a":}',
        b"[1,]",
        b"null null",
        b"/*bad*/null",
    ],
)
def test_bad_json_fails_closed(extra: bytes) -> None:
    with pytest.raises(ModelCallInputInvalid):
        _validate(_body(extra))


@pytest.mark.parametrize(
    "raw", [b"\xef\xbb\xbf" + _body(), b"[]", _body() + b"{}", b""]
)
def test_root_and_encoding_rejected(raw: bytes) -> None:
    with pytest.raises(ModelCallInputInvalid):
        _validate(raw)


def _bounded_body(boundary: str, n: int) -> bytes:
    if boundary == "depth":
        return _body(b"[" * n + b"null" + b"]" * n)
    if boundary == "members":
        return _body(b"[" + b",".join([b"0"] * n) + b"]")
    if boundary == "string":
        return _body(b'"' + b"x" * (n - 2) + b'"')
    if boundary == "key":
        return _body(b'{"' + b"x" * (n - 2) + b'":null}')
    if boundary == "number":
        return _body(b"0." + b"0" * (n - 2))
    if boundary == "nodes":
        # Root, four keys, three scalars, outer array, eight inner arrays = 17.
        groups = [b"[" + b",".join([b"0"] * 16_384) + b"]"] * 7
        groups.append(b"[" + b",".join([b"0"] * (n - 17 - 7 * 16_384)) + b"]")
        return _body(b"[" + b",".join(groups) + b"]")
    return _body() + b" " * (n - len(_body()))


@pytest.mark.parametrize(
    ("boundary", "limit"),
    [
        ("depth", 63),
        ("members", 16_384),
        ("string", 4_194_304),
        ("key", 4_096),
        ("number", 128),
        ("nodes", 131_072),
        ("body", 8_388_608),
    ],
)
def test_exact_resource_limit_and_one_over(boundary: str, limit: int) -> None:
    _validate(_bounded_body(boundary, limit))
    with pytest.raises(ModelCallInputInvalid):
        _validate(_bounded_body(boundary, limit + 1))


@pytest.mark.parametrize(
    "header",
    [
        ("Cookie", "x"),
        ("OpenAI-Organization", "x"),
        ("x-new", "x"),
        ("Content-Encoding", "gzip"),
        ("Transfer-Encoding", "chunked"),
        ("Host", "other"),
        ("X-Stainless-Raw-Response", "stream"),
        ("X-Stainless-Helper-Method", "unknown"),
        ("X-Stainless-Retry-Count", "1"),
    ],
)
def test_header_rejection(header: tuple[str, str]) -> None:
    with pytest.raises(ModelCallInputInvalid):
        _validate(_body(), (header,))


@pytest.mark.parametrize(
    "helper", ["chat.completions.parse", "chat.completions.stream"]
)
def test_locked_sdk_header_profile(helper: str) -> None:
    _validate(
        _body(),
        (
            ("X-Stainless-Raw-Response", "true"),
            ("X-Stainless-Helper-Method", helper),
            ("X-Stainless-Async", "async:asyncio"),
        ),
    )


@pytest.mark.parametrize(
    "change",
    [
        {"n": True},
        {"n": 2},
        {"model": "other"},
        {"max_completion_tokens": 17},
        {"max_tokens": 1},
    ],
)
def test_execution_bound_remains_mandatory(change: dict[str, object]) -> None:
    payload: dict[str, object] = {"model": "qa", "n": 1, "max_completion_tokens": 1}
    payload.update(change)
    with pytest.raises(ModelCallInputInvalid):
        _validate(json.dumps(payload).encode())


def test_final_digest_exact_domain_envelope_and_byte_drift() -> None:
    raw = _body()
    planned = "a" * 64
    expected = hashlib.sha256(
        b"kokoro-agent:final-http-request:v1\n"
        + rfc8785.dumps(
            {
                "version": 1,
                "request_profile": "chat-completions-materialized-json-v1",
                "method": "POST",
                "endpoint": _ENDPOINT,
                "planned_digest": planned,
                "body_bytes": len(raw),
                "body_sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
    ).hexdigest()
    assert (
        encode_final_request_digest(endpoint=_ENDPOINT, planned_digest=planned, raw=raw)
        == expected
    )
    assert (
        encode_final_request_digest(
            endpoint=_ENDPOINT, planned_digest=planned, raw=raw + b" "
        )
        != expected
    )
    assert (
        encode_final_request_digest(
            endpoint=_ENDPOINT, planned_digest="b" * 64, raw=raw
        )
        != expected
    )


@pytest.mark.parametrize("encoded", [False, True])
def test_unicode_string_limit_counts_raw_utf8_or_escape_bytes(encoded: bool) -> None:
    unit = b"\\u0061" if encoded else "你".encode()
    available = 4_194_304 - 2
    content = unit * (available // len(unit)) + b"a" * (available % len(unit))
    _validate(_body(b'"' + content + b'"'))
    with pytest.raises(ModelCallInputInvalid):
        _validate(_body(b'"' + content + b'a"'))


def test_object_member_limit_and_surrogate_pair_positive() -> None:
    members = [json.dumps(str(index)).encode() + b":0" for index in range(16_384)]
    _validate(_body(b"{" + b",".join(members) + b"}"))
    with pytest.raises(ModelCallInputInvalid):
        _validate(_body(b"{" + b",".join([*members, b'"extra":0']) + b"}"))
    _validate(_body(b'"\\ud83d\\ude00"'))
    with pytest.raises(ModelCallInputInvalid):
        _validate(_body(b'{"\\ud800":0}'))


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("Host", "other.invalid"),
        ("Content-Length", "1"),
        ("Content-Length", "01"),
        ("Content-Type", "text/plain"),
    ],
)
def test_single_wrong_header_value_is_not_a_duplicate(name: str, value: str) -> None:
    raw = _body()
    headers = {
        "Host": "synthetic.invalid",
        "Content-Length": str(len(raw)),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    headers[name] = value
    with pytest.raises(ModelCallInputInvalid):
        validate_request_profile(
            raw=raw,
            method="POST",
            endpoint=_ENDPOINT,
            expected_endpoint=_ENDPOINT,
            headers=tuple(headers.items()),
            model_name="qa",
            max_output_tokens=16,
        )


def test_total_nodes_count_object_keys_not_only_values() -> None:
    # 9 prefix nodes + 4 object nodes + 2 nodes/member. Add one scalar for even max.
    counts = (16_384, 16_384, 16_384, 16_377)
    objects = [
        b"{" + b",".join(json.dumps(str(i)).encode() + b":0" for i in range(n)) + b"}"
        for n in counts
    ]
    body = _body(b"[" + b",".join([*objects, b"0"]) + b"]")
    _validate(body)  # 9 + 4 + 2*65529 + 1 = 131072
    with pytest.raises(ModelCallInputInvalid):
        _validate(_body(b"[" + b",".join([*objects, b"0", b"0"]) + b"]"))
