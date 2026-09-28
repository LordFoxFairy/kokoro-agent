"""Build-time validation for the pinned Platform execution-operation artifact."""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import re
from collections.abc import Mapping
from typing import TypeAlias

from pydantic import JsonValue, TypeAdapter, ValidationError

from kokoro_agent.execution.execution_proof_profile import MAX_SAFE_INTEGER
from kokoro_agent.execution.platform_request_binding_values import (
    PlatformRequestBindingError,
    canonical_binding_bytes,
)


JsonObject: TypeAlias = dict[str, object]
EXPECTED_OWNER_COMMIT = "ee25c1f4d6df08be183ca10f7f5e852e0b21f641"
EXPECTED_OWNER_REPOSITORY = "apps/kokoro-capability"
EXPECTED_AGGREGATE = "afca9369c4aedd7a68aea02af834265efc6f480a07668ae0f8dbcad9bd7e545e"
EXPECTED_OWNER_PROVENANCE_SHA256 = (
    "eaf58dafe6d8730d6cf371357136e6ed59532922d19beb97067db8a666b1183d"
)
EXPECTED_EXECUTION_SOURCES = (
    (
        "contract/platform/v1/execution-operations/v1/command-identities.json",
        "contract/execution-operations/v1/command-identities.json",
        "4e25844843b46f86826aba3f2f250cdba6b78d81867288a6414dc333a10091dd",
    ),
    (
        "contract/platform/v1/execution-operations/v1/identities.json",
        "contract/execution-operations/v1/identities.json",
        "ba2064c1e720e41a326e0c88c2a53f26f1727565f24a955f55490816351282e0",
    ),
    (
        "contract/platform/v1/execution-operations/v1/manifest.json",
        "contract/execution-operations/v1/manifest.json",
        "187bbeeceb1e082c15a8df3fc1451f89322575fa13276abe161a2c72fedad9c9",
    ),
    (
        "contract/platform/v1/execution-operations/v1/operation-catalog.json",
        "contract/execution-operations/v1/operation-catalog.json",
        "9da9568ddd37622a9e3b4ccfdb8984705a35b525aa7bd8d5c27c8cb625b8a6cc",
    ),
    (
        "contract/platform/v1/execution-operations/v1/provenance.json",
        "contract/execution-operations/v1/provenance.json",
        EXPECTED_OWNER_PROVENANCE_SHA256,
    ),
    (
        "contract/platform/v1/execution-operations/v1/request-bindings.json",
        "contract/execution-operations/v1/request-bindings.json",
        "050291cf6f56052453ddb841f1e7cc624b2d3da28e5d9afd6abb9108349371fd",
    ),
    (
        "contract/platform/v1/execution-operations/v1/schemas/command-identities.schema.json",
        "contract/execution-operations/v1/schemas/command-identities.schema.json",
        "1ee71ebd279724fe19602535d74fda241debf81da68e0431643185500a28814f",
    ),
    (
        "contract/platform/v1/execution-operations/v1/schemas/identities.schema.json",
        "contract/execution-operations/v1/schemas/identities.schema.json",
        "5ed33481630923d2d237f16a08819627a3826dbe557f3884329066189edbe0c9",
    ),
    (
        "contract/platform/v1/execution-operations/v1/schemas/manifest.schema.json",
        "contract/execution-operations/v1/schemas/manifest.schema.json",
        "911e63385a799add2e592dcb90ae2c8af556968e2cb53a168e70101f68a9ee09",
    ),
    (
        "contract/platform/v1/execution-operations/v1/schemas/operation-catalog.schema.json",
        "contract/execution-operations/v1/schemas/operation-catalog.schema.json",
        "bb7d7e0e37bb9d6d8a696b7704b9c13263e95b5705607c6ed159cf5e01d56161",
    ),
    (
        "contract/platform/v1/execution-operations/v1/schemas/request-bindings.schema.json",
        "contract/execution-operations/v1/schemas/request-bindings.schema.json",
        "a4ce34e858ab4850b6429407b52e6b5ead86d0d629ecc7033d15487a38166b13",
    ),
    (
        "contract/platform/v1/execution-operations/v1/schemas/vector-inventory.schema.json",
        "contract/execution-operations/v1/schemas/vector-inventory.schema.json",
        "a6815f865e8d39610623288b939047367f5a0840ee1f6c8112db5582463d507d",
    ),
    (
        "contract/platform/v1/execution-operations/v1/vectors/negative.json",
        "contract/execution-operations/v1/vectors/negative.json",
        "34b44925718d03715f8c493d5c8afe58b0037aab6886618a536911f38983d121",
    ),
    (
        "contract/platform/v1/execution-operations/v1/vectors/positive.json",
        "contract/execution-operations/v1/vectors/positive.json",
        "ffb38f37dc7a103d48f99b5fe19fd9cc3c1145f86c5fb13424cf6dd3d42fffca",
    ),
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_OBJECT = TypeAdapter(dict[str, object])
_OBJECT_LIST = TypeAdapter(list[object])
_JSON_VALUE: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)
_JSON_OBJECT: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(dict[str, JsonValue])
_WRAPPERS: dict[str, tuple[int, int, re.Pattern[str]]] = {
    name: (minimum, maximum, re.compile(pattern))
    for name, minimum, maximum, pattern in (
        ("SkillSeriesId", 1, 191, r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$"),
        ("SkillId", 1, 191, r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$"),
        ("SkillInstallationId", 1, 191, r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$"),
        (
            "SkillSourceRef",
            7,
            197,
            r"^skill:[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$",
        ),
        ("McpConnectorId", 1, 191, r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$"),
        ("McpServerId", 1, 191, r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$"),
        ("McpConnectionId", 1, 191, r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$"),
        (
            "McpAuthorizationId",
            1,
            191,
            r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$",
        ),
        (
            "McpInvocationGrant",
            46,
            46,
            r"^mcp-grant:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
        ),
    )
}


def _object(value: object, *, label: str) -> JsonObject:
    try:
        return _OBJECT.validate_python(value, strict=True)
    except ValidationError:
        raise PlatformRequestBindingError(f"{label} must be an object") from None


def _list(value: object, *, label: str) -> list[object]:
    try:
        return _OBJECT_LIST.validate_python(value, strict=True)
    except ValidationError:
        raise PlatformRequestBindingError(f"{label} must be an array") from None


def _exact_keys(value: JsonObject, expected: set[str], *, label: str) -> None:
    if set(value) != expected:
        raise PlatformRequestBindingError(f"{label} fields drift")


def _record_path_bytes(value: object) -> bytes:
    path = _object(value, label="owner record").get("path")
    if type(path) is not str:
        raise PlatformRequestBindingError("owner record path is invalid")
    return path.encode()


def strict_parse_raw_json(raw: bytes) -> JsonValue:
    """Reject tokens outside the artifact's integer-only strict JSON profile."""

    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise PlatformRequestBindingError("invalid UTF-8") from None

    def parse_int(token: str) -> int:
        if token == "-0":
            raise PlatformRequestBindingError("number token is forbidden")
        value = int(token)
        if not -MAX_SAFE_INTEGER <= value <= MAX_SAFE_INTEGER:
            raise PlatformRequestBindingError("unsafe integer")
        return value

    def parse_float(_token: str) -> float:
        raise PlatformRequestBindingError("number token is forbidden")

    def pairs(items: list[tuple[str, object]]) -> JsonObject:
        result: JsonObject = {}
        for key, value in items:
            if key in result:
                raise PlatformRequestBindingError("duplicate key")
            result[key] = value
        return result

    try:
        decoded: object = json.loads(
            text,
            parse_int=parse_int,
            parse_float=parse_float,
            parse_constant=lambda _token: (_ for _ in ()).throw(
                PlatformRequestBindingError("number token is forbidden")
            ),
            object_pairs_hook=pairs,
        )
    except PlatformRequestBindingError:
        raise
    except (json.JSONDecodeError, ValueError):
        raise PlatformRequestBindingError("raw JSON is invalid") from None
    try:
        value = _JSON_VALUE.validate_python(decoded, strict=True)
    except ValidationError:
        raise PlatformRequestBindingError("raw JSON value is invalid") from None
    _reject_surrogates(value)
    return value


def _reject_surrogates(value: JsonValue) -> None:
    if type(value) is str:
        _validate_unicode_string(value)
    elif type(value) is list:
        for item in value:
            _reject_surrogates(item)
    elif type(value) is dict:
        for key, item in value.items():
            _reject_surrogates(key)
            _reject_surrogates(item)


def _validate_unicode_string(value: str) -> None:
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        raise PlatformRequestBindingError("lone surrogate") from None


def _presence(value: object, *, label: str, inner: str) -> None:
    presence = _object(value, label=label)
    if type(presence.get("present")) is not bool:
        raise PlatformRequestBindingError(f"{label} presence is invalid")
    if presence["present"]:
        _exact_keys(presence, {"present", "value"}, label=label)
        _member(presence["value"], kind=inner, label=f"{label}.value")
    else:
        _exact_keys(presence, {"present"}, label=label)


def _named(value: object, *, name: str, label: str) -> None:
    item = _object(value, label=label)
    if name == "OwnerScope":
        _exact_keys(item, {"kind", "id"}, label=label)
        for field in ("kind", "id"):
            if type(item[field]) is not str or not item[field]:
                raise PlatformRequestBindingError(f"{label}.{field} is invalid")
        return
    if name == "CommandIdentity":
        _exact_keys(item, {"command_id", "request_digest"}, label=label)
        if type(item["command_id"]) is not str or not item["command_id"]:
            raise PlatformRequestBindingError(f"{label}.command_id is invalid")
        if (
            type(item["request_digest"]) is not str
            or _SHA256.fullmatch(item["request_digest"]) is None
        ):
            raise PlatformRequestBindingError(f"{label}.request_digest is invalid")
        return
    if name == "Page":
        _exact_keys(item, {"limit", "cursor"}, label=label)
        _member(item["limit"], kind="safe-integer", label=f"{label}.limit")
        _presence(item["cursor"], label=f"{label}.cursor", inner="string")
        return
    raise PlatformRequestBindingError(f"unknown projection shape {name}")


def _member(value: object, *, kind: str, label: str) -> None:
    if kind == "string":
        if type(value) is not str:
            raise PlatformRequestBindingError(f"{label} field is invalid")
        _validate_unicode_string(value)
    elif kind == "boolean":
        if type(value) is not bool:
            raise PlatformRequestBindingError(f"{label} field is invalid")
    elif kind in {"enum", "safe-integer"}:
        if type(value) is not int or not -MAX_SAFE_INTEGER <= value <= MAX_SAFE_INTEGER:
            raise PlatformRequestBindingError(f"{label} bool-as-int/enum is invalid")
    elif kind == "lowercase-64hex":
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise PlatformRequestBindingError(f"{label} bytes digest is invalid")
    elif kind == "array<string>":
        items = _list(value, label=label)
        if any(type(item) is not str for item in items):
            raise PlatformRequestBindingError(f"{label} array is invalid")
        for item in items:
            _validate_unicode_string(str(item))
    elif kind == "set<string>":
        raw_items = _list(value, label=label)
        if any(type(item) is not str or not item for item in raw_items):
            raise PlatformRequestBindingError(f"{label} blank set is invalid")
        items = [str(item) for item in raw_items]
        if len(set(items)) != len(items):
            raise PlatformRequestBindingError(f"{label} duplicate set is invalid")
        if items != sorted(items, key=lambda item: item.encode("utf-8")):
            raise PlatformRequestBindingError(f"{label} set order is invalid")
    elif kind.startswith("id<"):
        wrapper = kind[3:-1]
        _presence(value, label=label, inner=f"wrapper<{wrapper}>")
    elif kind.startswith("msg<"):
        _presence(value, label=label, inner=f"named<{kind[4:-1]}>")
    elif kind.startswith("opt<"):
        _presence(value, label=label, inner=kind[4:-1])
    elif kind == "command":
        _presence(value, label=label, inner="named<CommandIdentity>")
    elif kind == "page":
        _presence(value, label=label, inner="named<Page>")
    elif kind.startswith("wrapper<"):
        wrapper = kind[8:-1]
        if type(value) is not str:
            raise PlatformRequestBindingError(f"{label} identity is invalid")
        minimum, maximum, pattern = _WRAPPERS[wrapper]
        size = len(value.encode("utf-8"))
        if not minimum <= size <= maximum or pattern.fullmatch(value) is None:
            raise PlatformRequestBindingError(f"{label} identity is invalid")
    elif kind.startswith("named<"):
        _named(value, name=kind[6:-1], label=label)
    else:
        raise PlatformRequestBindingError(f"unknown projection kind {kind}")


def validate_projected_binding(value: object, *, binding: Mapping[str, object]) -> None:
    root = _object(value, label="binding root")
    _exact_keys(
        root,
        {"binding_version", "fq_method", "tenant_ref", "request_id", "request"},
        label="binding root",
    )
    if (
        root["binding_version"] != "1.0.0"
        or root["fq_method"] != binding["fqMethod"]
        or type(root["tenant_ref"]) is not str
        or not root["tenant_ref"]
        or type(root["request_id"]) is not str
        or not root["request_id"]
    ):
        raise PlatformRequestBindingError("binding method/version/identity is invalid")
    request = _object(root["request"], label="binding request")
    members_value = binding["requestMembers"]
    raw_members = _list(members_value, label="binding descriptor")
    if any(type(member) is not str for member in raw_members):
        raise PlatformRequestBindingError("binding descriptor is invalid")
    members = [str(member) for member in raw_members]
    specifications = [member.split(":", 1) for member in members]
    _exact_keys(
        request, {name for name, _kind in specifications}, label="binding request"
    )
    for name, kind in specifications:
        _member(request[name], kind=kind, label=f"binding request {name}")


def validate_platform_binding_artifact(root: Path) -> tuple[int, int, int]:
    """Validate direct/aggregate pin plus 24 positive, 134 binding and 7 raw negatives."""

    pin_path = root / "contract/platform/v1/provenance.json"
    pin = _object(strict_parse_raw_json(pin_path.read_bytes()), label="Platform pin")
    execution = _object(pin.get("execution_operations"), label="execution pin")
    if (
        pin.get("owner") != "kokoro-platform"
        or pin.get("owner_repository") != EXPECTED_OWNER_REPOSITORY
        or pin.get("owner_commit") != EXPECTED_OWNER_COMMIT
        or execution.get("owner_commit") != EXPECTED_OWNER_COMMIT
        or execution.get("aggregate_sha256") != EXPECTED_AGGREGATE
    ):
        raise PlatformRequestBindingError("Platform execution artifact pin is invalid")
    sources_value = execution.get("sources")
    sources = _list(sources_value, label="execution sources")
    if len(sources) != len(EXPECTED_EXECUTION_SOURCES):
        raise PlatformRequestBindingError(
            "Platform execution direct source pin is invalid"
        )
    observed_sources: list[tuple[str, str, str]] = []
    for source_value in sources:
        source = _object(source_value, label="execution source")
        _exact_keys(source, {"path", "owner_path", "sha256"}, label="execution source")
        path = source.get("path")
        owner_path = source.get("owner_path")
        digest = source.get("sha256")
        if (
            type(path) is not str
            or type(owner_path) is not str
            or type(digest) is not str
        ):
            raise PlatformRequestBindingError("execution source identity is invalid")
        observed_sources.append((path, owner_path, digest))
    if tuple(observed_sources) != EXPECTED_EXECUTION_SOURCES:
        raise PlatformRequestBindingError(
            "Platform execution direct source pin is invalid"
        )
    for path, _owner_path, digest in EXPECTED_EXECUTION_SOURCES:
        if hashlib.sha256((root / path).read_bytes()).hexdigest() != digest:
            raise PlatformRequestBindingError(f"execution source digest drift: {path}")

    artifact = root / "contract/platform/v1/execution-operations/v1"
    provenance_raw = (artifact / "provenance.json").read_bytes()
    if hashlib.sha256(provenance_raw).hexdigest() != EXPECTED_OWNER_PROVENANCE_SHA256:
        raise PlatformRequestBindingError("owner provenance digest drift")
    provenance = _object(
        strict_parse_raw_json(provenance_raw),
        label="owner provenance",
    )
    if provenance.get("aggregateSha256") != EXPECTED_AGGREGATE:
        raise PlatformRequestBindingError("owner aggregate pin is invalid")
    records_value = provenance.get("files")
    records = _list(records_value, label="owner records")
    if len(records) != 13:
        raise PlatformRequestBindingError("owner payload inventory is invalid")
    aggregate = hashlib.sha256()
    for record_value in sorted(
        records,
        key=_record_path_bytes,
    ):
        record = _object(record_value, label="owner record")
        relative = record["path"]
        if type(relative) is not str:
            raise PlatformRequestBindingError("owner record path is invalid")
        payload = (artifact / relative).read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if len(payload) != record["bytes"] or digest != record["sha256"]:
            raise PlatformRequestBindingError(f"owner payload drift: {relative}")
        aggregate.update(
            relative.encode()
            + b"\0"
            + str(len(payload)).encode()
            + b"\0"
            + digest.encode()
            + b"\n"
        )
    if aggregate.hexdigest() != EXPECTED_AGGREGATE:
        raise PlatformRequestBindingError("owner aggregate digest is invalid")

    binding_document = _object(
        strict_parse_raw_json((artifact / "request-bindings.json").read_bytes()),
        label="binding artifact",
    )
    bindings_value = binding_document["bindings"]
    raw_bindings = _list(bindings_value, label="binding inventory")
    if len(raw_bindings) != 24:
        raise PlatformRequestBindingError("binding inventory is invalid")
    bindings = [_object(value, label="binding descriptor") for value in raw_bindings]
    by_method = {str(binding["fqMethod"]): binding for binding in bindings}

    positive_document = _object(
        strict_parse_raw_json((artifact / "vectors/positive.json").read_bytes()),
        label="positive vectors",
    )
    positives_value = positive_document["vectors"]
    positives = _list(positives_value, label="positive vector inventory")
    positive_bindings = 0
    for vector_value in positives:
        vector = _object(vector_value, label="positive vector")
        raw = base64.b64decode(str(vector["rawBase64"]), validate=True)
        if vector["target"] == "raw-bytes-sha256":
            if hashlib.sha256(raw).hexdigest() != vector["sha256"]:
                raise PlatformRequestBindingError("raw-byte vector digest drift")
            continue
        parsed = strict_parse_raw_json(raw)
        canonical = canonical_binding_bytes(
            _JSON_OBJECT.validate_python(
                _object(parsed, label="positive value"), strict=True
            )
        )
        expected_canonical = base64.b64decode(
            str(vector["canonicalBase64"]), validate=True
        )
        if (
            canonical != expected_canonical
            or hashlib.sha256(canonical).hexdigest() != vector["sha256"]
        ):
            raise PlatformRequestBindingError("positive canonical vector drift")
        if vector["target"] == "binding":
            validate_projected_binding(
                parsed, binding=by_method[str(vector["fqMethod"])]
            )
            positive_bindings += 1

    negative_document = _object(
        strict_parse_raw_json((artifact / "vectors/negative.json").read_bytes()),
        label="negative vectors",
    )
    negatives_value = negative_document["vectors"]
    negatives = _list(negatives_value, label="negative vector inventory")
    raw_negative = 0
    binding_negative = 0
    for vector_value in negatives:
        vector = _object(vector_value, label="negative vector")
        raw = base64.b64decode(str(vector["rawBase64"]), validate=True)
        target = vector["target"]
        if target == "canonical-mismatch":
            parsed = strict_parse_raw_json(raw)
            actual = hashlib.sha256(
                canonical_binding_bytes(
                    _JSON_OBJECT.validate_python(
                        _object(parsed, label="canonical mismatch"), strict=True
                    )
                )
            ).hexdigest()
            if actual == vector["forbiddenSha256"]:
                raise PlatformRequestBindingError("Unicode normalization substitution")
            continue
        try:
            parsed = strict_parse_raw_json(raw)
            if target == "binding":
                validate_projected_binding(
                    parsed, binding=by_method[str(vector["fqMethod"])]
                )
        except PlatformRequestBindingError:
            if target == "raw-json":
                raw_negative += 1
            elif target == "binding":
                binding_negative += 1
            continue
        raise PlatformRequestBindingError(f"negative vector accepted: {vector['name']}")
    if (positive_bindings, binding_negative, raw_negative) != (24, 134, 7):
        raise PlatformRequestBindingError("Platform vector coverage is not exact")
    return positive_bindings, binding_negative, raw_negative
