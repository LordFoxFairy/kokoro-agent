"""Validate the Agent-owned execution-proof artifacts and provenance details."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import NoReturn, TypeAlias

from jsonschema import Draft202012Validator, ValidationError as JsonSchemaError
from jsonschema import validate as validate_json_schema
from pydantic import TypeAdapter, ValidationError
import rfc8785

from kokoro_agent.execution_proof_negative_specs import (
    NEGATIVE_SPEC_INDEX,
    NEGATIVE_SPECS,
)


OPENAPI_RELATIVE = "contract/openapi/v1/openapi.json"
SCHEMA_RELATIVE = "contract/execution-proof/v1/schema.json"
VECTORS_RELATIVE = "contract/execution-proof/v1/vectors.json"
PROVENANCE_RELATIVE = "contract/provenance.json"
OWNER_SOURCE_FILES = (
    OPENAPI_RELATIVE,
    SCHEMA_RELATIVE,
    VECTORS_RELATIVE,
    "src/kokoro_agent/protocol/control.py",
    "src/kokoro_agent/protocol/events.py",
    "src/kokoro_agent/protocol/streams.py",
)
HEADER_FIELDS = {"typ", "alg", "kid"}
TAMPER_FIELDS = set(
    "name protected_header_base64url claims_base64url signing_input "
    "signature_base64url compact_jws difference".split()
)
CLAIM_FIELDS: set[str] = set(
    "contract_version iss aud tenant_ref actor subject run_id execution_session_id "
    "lease_generation operation request_binding_sha256 iat exp jti".split()
)
MAX_SAFE_INTEGER = 9_007_199_254_740_991
JTI_PATTERN = "^[A-Za-z0-9_-]{21}[AQgw]$"
_OBJECT: TypeAdapter[dict[str, object]] = TypeAdapter(dict[str, object])
_STRINGS: TypeAdapter[list[str]] = TypeAdapter(list[str])
_OBJECTS: TypeAdapter[list[object]] = TypeAdapter(list[object])
_BASE64URL = re.compile(r"^[A-Za-z0-9_-]+$")
JsonScalar: TypeAlias = bool | int | float | str | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class _DuplicateMemberError(ValueError):
    def __init__(self, member: str, pairs: list[tuple[str, object]]) -> None:
        super().__init__(f"duplicate JSON member: {member}")
        self.member = member
        self.pairs = pairs


class _IntegerTokenError(ValueError):
    pass


def _object(value: object, *, source: str) -> dict[str, object]:
    try:
        return _OBJECT.validate_python(value)
    except ValidationError as error:
        raise ValueError(f"{source} must be an object") from error


def _string(value: object, *, source: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{source} must be a string")
    return value


def _integer(value: object, *, source: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{source} must be an integer")
    return value


def _reject_duplicate_members(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateMemberError(key, pairs)
        result[key] = value
    return result


def _reject_float_token(token: str) -> NoReturn:
    raise _IntegerTokenError(token)


def _load_json_object(
    raw: bytes,
    *,
    source: str,
    parse_float: Callable[[str], object] = float,
) -> dict[str, object]:
    try:
        value: object = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_members,
            parse_float=parse_float,
            parse_constant=_reject_float_token,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{source} must contain valid UTF-8 JSON") from error
    return _object(value, source=source)


def _read_json(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise ValueError(f"contract source is missing: {path}")
    return _load_json_object(path.read_bytes(), source=str(path))


def _parse_proof_json(raw: bytes, *, source: str) -> dict[str, object]:
    return _load_json_object(raw, source=source, parse_float=_reject_float_token)


def _json_value(value: object, *, source: str) -> JsonValue:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, list):
        return [
            _json_value(item, source=source) for item in _OBJECTS.validate_python(value)
        ]
    if isinstance(value, dict):
        return {
            key: _json_value(item, source=source)
            for key, item in _OBJECT.validate_python(value).items()
        }
    raise ValueError(f"{source} contains a non-JSON value")


def _json_exact(actual: object, expected: object) -> bool:
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, dict):
        actual_object = _OBJECT.validate_python(actual)
        expected_object = _object(expected, source="expected exact JSON object")
        return actual_object.keys() == expected_object.keys() and all(
            _json_exact(value, expected_object[name])
            for name, value in actual_object.items()
        )
    if isinstance(actual, list):
        actual_array = _OBJECTS.validate_python(actual)
        expected_array = _OBJECTS.validate_python(expected)
        return len(actual_array) == len(expected_array) and all(
            _json_exact(left, right)
            for left, right in zip(actual_array, expected_array, strict=True)
        )
    return actual == expected


def _expect_json(actual: object, expected: object, *, source: str) -> None:
    if not _json_exact(actual, expected):
        raise ValueError(f"{source} is not JSON type-exact")


def _canonicalize(value: object, *, source: str) -> bytes:
    try:
        return rfc8785.dumps(_json_value(value, source=source))
    except rfc8785.CanonicalizationError as error:
        raise ValueError(f"{source} cannot be canonicalized with RFC 8785") from error


def _base64url_bytes(value: str, *, source: str) -> bytes:
    if "=" in value or _BASE64URL.fullmatch(value) is None or len(value) % 4 == 1:
        raise ValueError(f"{source} must be unpadded base64url")
    try:
        return base64.b64decode(
            value + "=" * (-len(value) % 4), altchars=b"-_", validate=True
        )
    except (ValueError, binascii.Error) as error:
        raise ValueError(f"{source} must be unpadded base64url") from error


def _decode_base64url(value: str, *, source: str) -> bytes:
    decoded = _base64url_bytes(value, source=source)
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
    if canonical != value:
        raise ValueError(f"{source} must use canonical unpadded base64url")
    return decoded


def _require_properties(
    schema: dict[str, object], fields: set[str], source: str
) -> dict[str, object]:
    object_keys = {"type", "additionalProperties", "required", "properties"}
    if set(schema) != object_keys:
        raise ValueError(f"{source} schema keys are not exact")
    if (
        schema.get("type") != "object"
        or schema.get("additionalProperties") is not False
    ):
        raise ValueError(f"{source} must be a strict object")
    required = set(_STRINGS.validate_python(schema.get("required")))
    properties = _object(schema.get("properties"), source=f"{source}.properties")
    if required != fields or set(properties) != fields:
        raise ValueError(f"{source} field set is not exact")
    return properties


def _require_non_empty_string(spec: object, *, source: str) -> None:
    actual = _object(spec, source=source)
    _expect_json(actual, {"type": "string", "minLength": 1}, source=source)


def _validate_schema_metadata(schema: dict[str, object]) -> None:
    expected = _load_json_object(
        b'{"$schema":"https://json-schema.org/draft/2020-12/schema",'
        b'"$id":"https://kokoro.dev/contracts/agent/execution-proof/v1/schema.json",'
        b'"x-kokoro-contract-version":"1.0.0","x-kokoro-wire-format":"compact-jws",'
        b'"x-kokoro-canonicalization":"RFC 8785","x-kokoro-string-normalization":"none",'
        b'"x-kokoro-base64url-padding":"forbidden",'
        b'"x-kokoro-compact-jws-max-bytes":16384,'
        b'"x-kokoro-claim-pair-rules":{"exp_must_be_greater_than_iat":true,'
        b'"maximum_ttl_seconds":60}}',
        source="schema metadata specification",
    )
    top_keys = set(expected) | set(
        "title description type additionalProperties required properties".split()
    )
    if set(schema) != top_keys:
        raise ValueError("execution proof root schema keys are not exact")
    for name in ("title", "description"):
        _string(schema.get(name), source=f"execution proof {name}")
    for name, value in expected.items():
        if not _json_exact(schema.get(name), value):
            if name == "x-kokoro-contract-version":
                raise ValueError("execution proof version must be 1.0.0")
            label = name.removeprefix("x-kokoro-").replace("-", " ")
            raise ValueError(f"execution proof {label} metadata is not exact")


def _validate_execution_schema(schema: dict[str, object]) -> None:
    try:
        Draft202012Validator.check_schema(schema)
    except JsonSchemaError as error:
        raise ValueError("execution proof schema is not valid Draft 2020-12") from error
    _validate_schema_metadata(schema)
    root_schema = {
        name: schema[name]
        for name in ("type", "additionalProperties", "required", "properties")
    }
    root = _require_properties(root_schema, {"protected_header", "claims"}, "root")
    header = _object(root["protected_header"], source="protected_header schema")
    header_properties = _require_properties(header, HEADER_FIELDS, "header")
    for name, expected in (
        ("typ", {"const": "kokoro-agent-execution+jwt"}),
        ("alg", {"const": "EdDSA"}),
    ):
        actual = _object(header_properties[name], source=f"{name} schema")
        _expect_json(actual, expected, source=f"execution proof {name} schema")
    _require_non_empty_string(header_properties["kid"], source="kid schema")
    claims = _object(root["claims"], source="claims schema")
    properties = _require_properties(claims, CLAIM_FIELDS, "claims")
    exact_constants = (
        ("contract_version", "1.0.0"),
        ("aud", "https://kokoro.dev/resources/iam-execution-authorization"),
    )
    for name, value in exact_constants:
        actual = _object(properties[name], source=f"{name} schema")
        _expect_json(actual, {"const": value}, source=f"{name} schema")
    for name in ("iss", "tenant_ref", "run_id", "execution_session_id"):
        _require_non_empty_string(properties[name], source=f"{name} schema")
    _validate_claim_schemas(properties)


def _validate_claim_schemas(properties: dict[str, object]) -> None:
    for name in ("actor", "subject"):
        identity = _object(properties[name], source=f"{name} schema")
        fields = _require_properties(identity, {"kind", "opaque_ref"}, name)
        kind = _object(fields["kind"], source=f"{name}.kind schema")
        _expect_json(
            kind,
            {"enum": ["user", "project", "service"]},
            source=f"{name}.kind schema",
        )
        _require_non_empty_string(fields["opaque_ref"], source=f"{name}.opaque_ref")
    for name, minimum in (("lease_generation", 1), ("iat", 0), ("exp", 0)):
        number = _object(properties[name], source=f"{name} schema")
        expected = {
            "type": "integer",
            "minimum": minimum,
            "maximum": MAX_SAFE_INTEGER,
            "x-kokoro-require-integer-token": True,
        }
        _expect_json(number, expected, source=f"{name} numeric schema")
    exact_strings = {
        "operation": {
            "type": "string",
            "pattern": "^[a-z][a-z0-9_.]{0,127}$",
        },
        "request_binding_sha256": {
            "type": "string",
            "pattern": "^[0-9a-f]{64}$",
        },
        "jti": {
            "type": "string",
            "pattern": JTI_PATTERN,
            "x-kokoro-random-bits": 128,
            "x-kokoro-base64url-padding": "forbidden",
        },
    }
    for name, expected in exact_strings.items():
        actual = _object(properties[name], source=f"{name} schema")
        suffix = " padding" if name == "jti" else ""
        _expect_json(actual, expected, source=f"{name}{suffix} schema")


def _validate_schema_instance(
    schema: dict[str, object],
    *,
    header: dict[str, object],
    claims: dict[str, object],
) -> None:
    try:
        validate_json_schema(
            instance={"protected_header": header, "claims": claims},
            schema=schema,
            cls=Draft202012Validator,
        )
    except JsonSchemaError as error:
        raise ValueError(f"proof schema violation: {error.message}") from error


def _validate_claim_pair(claims: dict[str, object]) -> None:
    iat = claims.get("iat")
    exp = claims.get("exp")
    if isinstance(iat, bool) or not isinstance(iat, int):
        raise ValueError("iat must be an integer token")
    if isinstance(exp, bool) or not isinstance(exp, int):
        raise ValueError("exp must be an integer token")
    if exp - iat <= 0 or exp - iat > 60:
        raise ValueError("execution proof exp/iat pair violates TTL policy")


def _validate_canonical_record(
    record: dict[str, object], decoded: dict[str, object], *, source: str
) -> str:
    raw = _string(record.get("raw_json_utf8"), source=f"{source}.raw_json_utf8")
    canonical = _string(
        record.get("canonical_json_utf8"), source=f"{source}.canonical_json_utf8"
    )
    if raw != canonical:
        raise ValueError(f"{source} raw bytes must be the fixed canonical bytes")
    if not _json_exact(
        _parse_proof_json(raw.encode(), source=f"{source} raw bytes"), decoded
    ):
        raise ValueError(f"{source} decoded object does not match raw bytes")
    if _canonicalize(decoded, source=source) != canonical.encode():
        raise ValueError(f"{source} canonical bytes are stale")
    segment = _string(record.get("base64url"), source=f"{source}.base64url")
    if _decode_base64url(segment, source=f"{source} segment") != canonical.encode():
        raise ValueError(f"{source} segment does not encode canonical bytes")
    return segment


def _validate_positive(
    positive: dict[str, object], schema: dict[str, object]
) -> tuple[dict[str, object], dict[str, object]]:
    header_record = _object(positive.get("protected_header"), source="positive header")
    claims_record = _object(positive.get("claims"), source="positive claims")
    header = _object(header_record.get("decoded"), source="positive decoded header")
    claims = _object(claims_record.get("decoded"), source="positive decoded claims")
    _validate_schema_instance(schema, header=header, claims=claims)
    _validate_claim_pair(claims)
    header_segment = _validate_canonical_record(
        header_record, header, source="positive header"
    )
    claims_segment = _validate_canonical_record(
        claims_record, claims, source="positive claims"
    )
    signing_input = _string(
        positive.get("signing_input"), source="positive signing_input"
    )
    if signing_input != f"{header_segment}.{claims_segment}":
        raise ValueError("positive signing input assembly mismatch")
    signature = _string(
        positive.get("signature_base64url"), source="positive signature_base64url"
    )
    if len(_decode_base64url(signature, source="positive signature")) != 64:
        raise ValueError("positive Ed25519 signature must be 64 bytes")
    compact = _string(positive.get("compact_jws"), source="positive compact_jws")
    if compact != f"{signing_input}.{signature}":
        raise ValueError("positive compact JWS does not reassemble all three segments")
    if len(compact.encode("ascii")) > 16 * 1024:
        raise ValueError("positive compact JWS exceeds 16 KiB")
    jti = _string(claims.get("jti"), source="positive jti")
    if len(_decode_base64url(jti, source="positive jti")) != 16:
        raise ValueError("positive jti must encode exactly 16 bytes")
    _validate_jwk(positive, header)
    return header, claims


def _validate_jwk(positive: dict[str, object], header: dict[str, object]) -> None:
    jwk = _object(positive.get("public_jwk"), source="positive public_jwk")
    if set(jwk) != {"kty", "crv", "use", "alg", "kid", "x"}:
        raise ValueError("positive public JWK field set is not exact")
    if (jwk.get("kty"), jwk.get("crv"), jwk.get("use"), jwk.get("alg")) != (
        "OKP",
        "Ed25519",
        "sig",
        "EdDSA",
    ):
        raise ValueError("positive public JWK profile is not exact")
    if jwk.get("kid") != header.get("kid"):
        raise ValueError("positive public JWK kid does not match the protected header")
    x = _string(jwk.get("x"), source="positive public JWK x")
    if len(_decode_base64url(x, source="positive public JWK x")) != 32:
        raise ValueError("positive public JWK x must encode 32 bytes")
    thumbprint = (
        base64.urlsafe_b64encode(
            hashlib.sha256(
                _canonicalize({"crv": "Ed25519", "kty": "OKP", "x": x}, source="JWK")
            ).digest()
        )
        .rstrip(b"=")
        .decode("ascii")
    )
    if positive.get("jwk_thumbprint_sha256") != thumbprint:
        raise ValueError("positive public JWK thumbprint is stale")


def _validate_tamper(tampered: dict[str, object], positive: dict[str, object]) -> None:
    if set(tampered) != TAMPER_FIELDS or not _json_exact(
        tampered.get("name"), "signature_one_bit_tamper"
    ):
        raise ValueError("tampered signature identity and field set are not exact")
    header = _string(tampered.get("protected_header_base64url"), source="tamper header")
    claims = _string(tampered.get("claims_base64url"), source="tamper claims")
    signing_input = _string(tampered.get("signing_input"), source="tamper input")
    positive_header = _object(positive["protected_header"], source="positive header")
    positive_claims = _object(positive["claims"], source="positive claims")
    if header != positive_header["base64url"]:
        raise ValueError("tampered signature fixture changed the protected header")
    if claims != positive_claims["base64url"]:
        raise ValueError("tampered signature fixture changed the claims")
    if (
        signing_input != positive.get("signing_input")
        or signing_input != f"{header}.{claims}"
    ):
        raise ValueError("tampered signature fixture changed the signing input")
    expected = _decode_base64url(
        _string(positive.get("signature_base64url"), source="positive signature"),
        source="positive signature",
    )
    signature_text = _string(
        tampered.get("signature_base64url"), source="tampered signature"
    )
    changed = _decode_base64url(signature_text, source="tampered signature")
    difference = _object(tampered.get("difference"), source="tamper difference")
    index = _integer(difference.get("signature_byte_index"), source="tamper index")
    mask = _integer(difference.get("bit_mask"), source="tamper bit mask")
    if not 0 <= index < len(expected) or mask not in {1, 2, 4, 8, 16, 32, 64, 128}:
        raise ValueError("tampered signature difference metadata is invalid")
    declared = bytearray(expected)
    declared[index] ^= mask
    if len(changed) != 64 or changed != bytes(declared):
        raise ValueError("tampered signature does not equal its declared mutation")
    if tampered.get("compact_jws") != f"{signing_input}.{signature_text}":
        raise ValueError("tampered compact JWS does not recompose all three segments")
    if not _json_exact(difference, {"signature_byte_index": 0, "bit_mask": 1}):
        raise ValueError("tampered signature difference metadata is not exact")


def _pointer(error: JsonSchemaError) -> str:
    tokens = [
        str(token).replace("~", "~0").replace("/", "~1")
        for token in error.absolute_path
    ]
    return "" if not tokens else "/" + "/".join(tokens)


def _validate_named_negative_metadata(item: dict[str, object], name: str) -> str:
    spec = NEGATIVE_SPEC_INDEX.get(name)
    if spec is None:
        raise ValueError(f"unknown named negative: {name}")
    expected = spec.metadata()
    expected["name"] = name
    stage = spec.stage
    payload_key = "encoded_segment" if stage == "base64url" else "raw_json_base64url"
    actual = dict(item)
    if not isinstance(actual.pop(payload_key, None), str) or not _json_exact(
        actual, expected
    ):
        raise ValueError(f"named negative metadata is not exact for {name}")
    return stage


def _validate_schema_negative(
    item: dict[str, object],
    raw: bytes,
    schema: dict[str, object],
    positive_header: dict[str, object],
    positive_claims: dict[str, object],
    positive_claims_raw: str,
) -> None:
    name = _string(item.get("name"), source="negative name")
    decoded = _parse_proof_json(raw, source=f"negative vector {name}")
    difference = _object(item.get("difference"), source="negative difference")
    pointer = _string(difference.get("pointer"), source="negative pointer")
    tokens = pointer.strip("/").split("/")
    if len(tokens) != 2 or tokens[0] not in {"protected_header", "claims"}:
        raise ValueError("negative difference pointer is invalid")
    base = positive_header if tokens[0] == "protected_header" else positive_claims
    expected = {**base, tokens[1]: difference.get("value")}
    if not _json_exact(decoded, expected):
        raise ValueError(f"{name}: expected schema rejection difference mismatch")
    keyword = _string(item.get("expected_keyword"), source=f"{name} keyword")
    if keyword == "maximum":
        member = pointer.removeprefix("/claims/")
        original = f'"{member}":{positive_claims[member]}'
        replacement = f'"{member}":{difference["value"]}'
        if (
            positive_claims_raw.count(original) != 1
            or raw != positive_claims_raw.replace(original, replacement).encode()
        ):
            raise ValueError(f"{name}: not a single token replacement")
    elif raw != _canonicalize(decoded, source=f"negative vector {name}"):
        raise ValueError(f"negative vector {name} is not a canonical single-difference")
    component = _string(
        item.get("component"), source=f"negative vector {name} component"
    )
    header = decoded if component == "protected_header" else positive_header
    claims = decoded if component == "claims" else positive_claims
    try:
        validate_json_schema(
            instance={"protected_header": header, "claims": claims},
            schema=schema,
            cls=Draft202012Validator,
        )
    except JsonSchemaError as error:
        expected_pointer = _string(
            item.get("expected_pointer"), source=f"negative vector {name} pointer"
        )
        if _pointer(error) != expected_pointer or error.validator != keyword:
            raise ValueError(f"{name}: unexpected schema location") from error
        return
    raise ValueError(f"negative vector {name} is not rejected by schema")


def _validate_duplicate_negative(
    item: dict[str, object],
    raw: bytes,
    header: dict[str, object],
    claims: dict[str, object],
) -> None:
    name = _string(item.get("name"), source="duplicate vector name")
    expected_member = _string(item.get("expected_member"), source="expected duplicate")
    try:
        _parse_proof_json(raw, source=f"negative vector {name}")
    except _DuplicateMemberError as error:
        bases = {
            "duplicate_root_member": {"protected_header": header, "claims": claims},
            "duplicate_header_member": header,
            "duplicate_actor_member": _object(claims.get("actor"), source="actor"),
            "duplicate_claim_member": claims,
        }
        if name not in bases:
            raise ValueError(f"unknown duplicate vector: {name}") from error
        base = bases[name]
        first: dict[str, object] = {}
        duplicate_values: list[object] = []
        for key, value in error.pairs:
            if key == expected_member:
                duplicate_values.append(value)
            if key not in first:
                first[key] = value
        if (
            error.member != expected_member
            or not _json_exact(first, base)
            or len(error.pairs) != len(base) + 1
            or not _json_exact(
                duplicate_values, [base[expected_member], base[expected_member]]
            )
        ):
            raise ValueError(f"{name}: duplicate semantics are not exact") from error
        return
    except ValueError as error:
        raise ValueError(f"{name}: duplicate member fixture malformed") from error
    raise ValueError(f"negative vector {name} does not contain a duplicate member")


def _validate_integer_token_negative(
    item: dict[str, object], raw: bytes, *, positive_claims_raw: str
) -> None:
    name = _string(item.get("name"), source="integer token vector name")
    member = _string(item.get("expected_member"), source="integer token member")
    difference = _object(item.get("difference"), source="integer token difference")
    from_token = _string(difference.get("from_token"), source="integer from token")
    to_token = _string(difference.get("to_token"), source="integer to token")
    pointer = _string(difference.get("pointer"), source="integer token pointer")
    needle = f'"{member}":{from_token}'
    if pointer != f"/claims/{member}" or positive_claims_raw.count(needle) != 1:
        raise ValueError(f"negative vector {name} token difference metadata is invalid")
    expected_raw = positive_claims_raw.replace(
        needle, f'"{member}":{to_token}'
    ).encode()
    if raw != expected_raw:
        raise ValueError(f"negative vector {name} token difference is not exact")
    try:
        _parse_proof_json(raw, source=f"negative vector {name}")
    except _IntegerTokenError as error:
        if error.args != (to_token,):
            raise ValueError(f"{name}: wrong numeric token rejected") from error
        return
    raise ValueError(f"negative vector {name} is not rejected as a non-integer token")


def _validate_pair_negative(
    item: dict[str, object],
    raw: bytes,
    schema: dict[str, object],
    header: dict[str, object],
    claims: dict[str, object],
) -> None:
    name = _string(item.get("name"), source="pair vector name")
    decoded = _parse_proof_json(raw, source=f"negative vector {name}")
    difference = _object(item.get("difference"), source="pair difference")
    changes = _OBJECTS.validate_python(difference.get("changes"))
    declared: dict[str, object] = {}
    for change_value in changes:
        change = _object(change_value, source="pair change")
        pointer = _string(change.get("pointer"), source="pair pointer")
        member = pointer.removeprefix("/claims/")
        if pointer != f"/claims/{member}" or pointer in declared:
            raise ValueError(f"negative vector {name} pair pointer is invalid")
        declared[pointer] = change.get("value")
    actual = {
        f"/claims/{member}": decoded.get(member)
        for member in claims.keys() | decoded.keys()
        if not _json_exact(claims.get(member), decoded.get(member))
    }
    if not _json_exact(declared, actual):
        raise ValueError(f"negative vector {name} pair difference is not exact")
    if raw != _canonicalize(decoded, source=f"negative vector {name} pair raw bytes"):
        raise ValueError(f"negative vector {name} pair raw bytes are not canonical")
    _validate_schema_instance(schema, header=header, claims=decoded)
    try:
        _validate_claim_pair(decoded)
    except ValueError:
        return
    raise ValueError(f"negative vector {name} is not rejected by pair policy")


def _validate_base64_negative(
    item: dict[str, object],
    *,
    positive: dict[str, object],
    claims: dict[str, object],
) -> None:
    name = _string(item.get("name"), source="base64 vector name")
    encoded = _string(
        item.get("encoded_segment"), source=f"negative vector {name} segment"
    )
    difference = _object(item.get("difference"), source=f"{name} difference")
    if name == "base64url_padding":
        base = _string(
            _object(positive["claims"], source="positive claims").get("base64url"),
            source="positive claims segment",
        )
        suffix = _string(difference.get("suffix"), source="padding suffix")
        if (
            difference.get("base") != "positive_claims_segment"
            or encoded != base + suffix
        ):
            raise ValueError("base64url padding fixture difference is not exact")
        try:
            _decode_base64url(encoded, source="base64url padding fixture")
        except ValueError:
            return
    elif name == "jti_trailing_pad_bits_alias":
        canonical = _string(item.get("canonical_segment"), source="canonical jti")
        if canonical != claims.get("jti"):
            raise ValueError("jti alias fixture metadata is not exact")
        index = _integer(difference.get("character_index"), source="jti alias index")
        before = _string(difference.get("from"), source="jti alias from")
        after = _string(difference.get("to"), source="jti alias to")
        if (
            not 0 <= index < len(canonical)
            or len(before) != 1
            or len(after) != 1
            or canonical[index] != before
            or encoded != canonical[:index] + after + canonical[index + 1 :]
        ):
            raise ValueError("jti alias difference does not match the actual mutation")
        decoded_alias = _base64url_bytes(encoded, source="jti alias")
        decoded_canonical = _decode_base64url(canonical, source="canonical jti")
        if (
            len(decoded_alias) != 16
            or decoded_alias != decoded_canonical
            or encoded == canonical
        ):
            raise ValueError("jti trailing pad-bit alias fixture is not equivalent")
        try:
            _decode_base64url(encoded, source="jti alias")
        except ValueError:
            return
    raise ValueError(f"negative vector {name} is not rejected at base64url stage")


def _validate_negative_vectors(
    vectors: list[object],
    schema: dict[str, object],
    positive: dict[str, object],
    header: dict[str, object],
    claims: dict[str, object],
) -> None:
    items = [_object(value, source="negative vector") for value in vectors]
    names = [_string(item.get("name"), source="negative vector name") for item in items]
    expected_names = [spec.name for spec in NEGATIVE_SPECS]
    if not _json_exact(names, expected_names):
        raise ValueError("negative vector inventory is not exact and ordered")
    claims_raw = _string(
        _object(positive["claims"], source="positive claims").get(
            "canonical_json_utf8"
        ),
        source="positive claims canonical bytes",
    )
    for item, name in zip(items, names):
        stage = _validate_named_negative_metadata(item, name)
        if stage == "base64url":
            _validate_base64_negative(item, positive=positive, claims=claims)
            continue
        raw_text = _string(
            item.get("raw_json_base64url"), source=f"negative vector {name} raw bytes"
        )
        raw = _decode_base64url(raw_text, source=f"negative vector {name} raw bytes")
        if stage == "json_parse":
            _validate_duplicate_negative(item, raw, header, claims)
        elif stage == "integer_token":
            _validate_integer_token_negative(item, raw, positive_claims_raw=claims_raw)
        elif stage == "schema":
            _validate_schema_negative(item, raw, schema, header, claims, claims_raw)
        elif stage == "pair":
            _validate_pair_negative(item, raw, schema, header, claims)
        elif stage == "canonicalization":
            decoded = _parse_proof_json(raw, source=f"negative vector {name}")
            if (
                item.get("error_kind") != "noncanonical_json"
                or item.get("expected_decoded") != "positive_claims"
                or not _json_exact(decoded, claims)
                or _canonicalize(decoded, source=name) == raw
            ):
                raise ValueError(f"{name}: noncanonical semantics are not exact")
        else:
            raise ValueError(f"negative vector {name} stage is unknown")


def _validate_provenance(root: Path, provenance: dict[str, object]) -> None:
    if provenance.get("owner") != "kokoro-agent":
        raise ValueError("contract provenance owner must be kokoro-agent")
    sources = _STRINGS.validate_python(provenance.get("source_files"))
    if not _json_exact(sources, list(OWNER_SOURCE_FILES)):
        raise ValueError("source_files must equal ordered owner inventory")
    proof = _object(provenance.get("execution_proof"), source="proof provenance")
    if proof.get("version") != "1.0.0":
        raise ValueError("execution proof provenance version must be 1.0.0")
    for name, relative in (("schema", SCHEMA_RELATIVE), ("vectors", VECTORS_RELATIVE)):
        record = _object(proof.get(name), source=f"proof {name} provenance")
        if record.get("path") != relative:
            raise ValueError(f"execution proof {name} artifact path is not exact")
        if (
            record.get("sha256")
            != hashlib.sha256((root / relative).read_bytes()).hexdigest()
        ):
            raise ValueError(f"execution proof {name} artifact digest is stale")
    digest = hashlib.sha256()
    for relative in OWNER_SOURCE_FILES:
        path = root / relative
        if not path.is_file():
            raise ValueError(f"provenance source is missing: {relative}")
        digest.update(path.read_bytes())
    if provenance.get("combined_sha256") != digest.hexdigest():
        raise ValueError("contract provenance aggregate digest is stale")


def validate_execution_proof_contract(root: Path) -> None:
    schema = _read_json(root / SCHEMA_RELATIVE)
    vectors = _read_json(root / VECTORS_RELATIVE)
    provenance = _read_json(root / PROVENANCE_RELATIVE)
    _validate_execution_schema(schema)
    if vectors.get("contract_version") != "1.0.0":
        raise ValueError("execution proof vector version must be 1.0.0")
    positive = _object(vectors.get("positive"), source="positive vector")
    header, claims = _validate_positive(positive, schema)
    tampered = _object(vectors.get("tampered_signature"), source="tampered signature")
    _validate_tamper(tampered, positive)
    negatives = _OBJECTS.validate_python(vectors.get("negative"))
    _validate_negative_vectors(negatives, schema, positive, header, claims)
    _validate_provenance(root, provenance)
