"""Build-time validation for the pinned Platform execution-operation artifact."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from pathlib import Path
import re
from collections.abc import Mapping
from typing import TypeAlias

from pydantic import JsonValue, TypeAdapter, ValidationError
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from jsonschema import validate as validate_json_schema

from kokoro_agent.execution.execution_proof_profile import MAX_SAFE_INTEGER
from kokoro_agent.skills.package import SkillPackageError, validate_package
from kokoro_agent.execution.platform_request_binding_values import (
    PlatformRequestBindingError,
    canonical_binding_bytes,
)


JsonObject: TypeAlias = dict[str, object]
EXPECTED_OWNER_COMMIT = "6a09913a96c686b316bfe707b823d039e625607a"
EXPECTED_OWNER_REPOSITORY = "apps/kokoro-capability"
EXPECTED_AGGREGATE = "902f8f2c2fbeb95a441820c1cf16b0a9c793eadac7106f9fcd5e41e3878b7f79"
EXPECTED_OWNER_PROVENANCE_SHA256 = (
    "c84b87e36206de1b6aa8ef8b5bf5f52649f00afe7a0bcf3bacaa72017c4d4809"
)
EXPECTED_EXECUTION_SOURCES = (
    (
        "contract/platform/v1/execution-operations/v4/command-identities.json",
        "contract/execution-operations/v4/command-identities.json",
        "63e3a355f277c1ffefed82f4716fd91bc4a4c64e8a5b183b4eaa8d0a92a6e6bd",
    ),
    (
        "contract/platform/v1/execution-operations/v4/command-schemas.json",
        "contract/execution-operations/v4/command-schemas.json",
        "20a48aaa2e591f6e3941b7695cccd2719b34c3eeaefc984ec13c1bfa43d4b1cc",
    ),
    (
        "contract/platform/v1/execution-operations/v4/identities.json",
        "contract/execution-operations/v4/identities.json",
        "d52564167b932153c8434015df5cb7c3b53e7e1d5c4e059264479fb47323ea01",
    ),
    (
        "contract/platform/v1/execution-operations/v4/manifest.json",
        "contract/execution-operations/v4/manifest.json",
        "7058e2d2e11c885765ceb4e813e1f4f208e170c97a9b27e8d5f0178fcffc8d5d",
    ),
    (
        "contract/platform/v1/execution-operations/v4/operation-catalog.json",
        "contract/execution-operations/v4/operation-catalog.json",
        "ae4708cd4a9c05d90904222859e93a4ea3d7ec384e419416bb3c61a5ae2cd6d0",
    ),
    (
        "contract/platform/v1/execution-operations/v4/projection-registry.json",
        "contract/execution-operations/v4/projection-registry.json",
        "6101ee717c1c313de294cfa25b99e631153480bf27daaccedae98178e8ac1fcb",
    ),
    (
        "contract/platform/v1/execution-operations/v4/provenance.json",
        "contract/execution-operations/v4/provenance.json",
        "c84b87e36206de1b6aa8ef8b5bf5f52649f00afe7a0bcf3bacaa72017c4d4809",
    ),
    (
        "contract/platform/v1/execution-operations/v4/publish-v1.json",
        "contract/execution-operations/v4/publish-v1.json",
        "14c76f1793ea0a8c8d430095481912cad45b17d72345c213a82fe6e58e026ae9",
    ),
    (
        "contract/platform/v1/execution-operations/v4/request-bindings.json",
        "contract/execution-operations/v4/request-bindings.json",
        "9c8d3c3005f688172c664a12fbd25555504f2817cb488b68a5e0d821d47b1da2",
    ),
    (
        "contract/platform/v1/execution-operations/v4/schemas/command-identities.schema.json",
        "contract/execution-operations/v4/schemas/command-identities.schema.json",
        "601eb6c170bda98f4635f05451ab7a3bb534b8c4bef0d6cfd6f31e33a48c8d20",
    ),
    (
        "contract/platform/v1/execution-operations/v4/schemas/identities.schema.json",
        "contract/execution-operations/v4/schemas/identities.schema.json",
        "693b4134154516133a1494115a8e10cb37702d1b1210d6417497954f7d7e468d",
    ),
    (
        "contract/platform/v1/execution-operations/v4/schemas/manifest.schema.json",
        "contract/execution-operations/v4/schemas/manifest.schema.json",
        "cd638c946a2073a35397dd44fa3b5ca9cfbcb9a80a522a54d042bb2d61bd3481",
    ),
    (
        "contract/platform/v1/execution-operations/v4/schemas/operation-catalog.schema.json",
        "contract/execution-operations/v4/schemas/operation-catalog.schema.json",
        "4fecc7f29be0c05812665d787fd6471f86ea0d85f6fcd6664c44685f1fde720e",
    ),
    (
        "contract/platform/v1/execution-operations/v4/schemas/request-bindings.schema.json",
        "contract/execution-operations/v4/schemas/request-bindings.schema.json",
        "7e39cc320dbdb929ad47e448262aff5ea5a43368987ffe1a38e39de90b446aa0",
    ),
    (
        "contract/platform/v1/execution-operations/v4/schemas/vector-inventory.schema.json",
        "contract/execution-operations/v4/schemas/vector-inventory.schema.json",
        "5afa2f259245348f4ad76ceb4356632b7084ab0da3382cc77168fec71e3f71ba",
    ),
    (
        "contract/platform/v1/execution-operations/v4/schemas/zip-profile-v1.schema.json",
        "contract/execution-operations/v4/schemas/zip-profile-v1.schema.json",
        "97928b92dac7dabf6b51662af2b19741474377c564aaff9411a9ad8ab58d6712",
    ),
    (
        "contract/platform/v1/execution-operations/v4/vectors/command-projection.json",
        "contract/execution-operations/v4/vectors/command-projection.json",
        "2dc336ff4da132ebf0551dc6c74fb4b8a286adb52f4831fa98379a9436af612c",
    ),
    (
        "contract/platform/v1/execution-operations/v4/vectors/negative.json",
        "contract/execution-operations/v4/vectors/negative.json",
        "20e0823dedb82449789cf3ff698705fc9d59e1dad52a54f3518a59ea7514432a",
    ),
    (
        "contract/platform/v1/execution-operations/v4/vectors/positive.json",
        "contract/execution-operations/v4/vectors/positive.json",
        "33941b076d6588e80aa13cd339b3eb2b439617ba05c28a50549377c09f2ce909",
    ),
    (
        "contract/platform/v1/execution-operations/v4/vectors/zip-v1.json",
        "contract/execution-operations/v4/vectors/zip-v1.json",
        "d0d37e7b1ef8b88daede645c4d826f95fac38c25f374fdf2d41d090231d6f943",
    ),
    (
        "contract/platform/v1/execution-operations/v4/zip-profile-v1.json",
        "contract/execution-operations/v4/zip-profile-v1.json",
        "18ef03a3d7ea84f0e2a38c6e625146f607b96689957210052116b557008024fc",
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
        root["binding_version"] != "3.0.0"
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


def _validate_command_extensions(schema: Mapping[str, object], value: object) -> None:
    raw_value: object = value
    if type(value) is str:
        limit = schema.get("x-maxUtf16")
        if type(limit) is int and len(value.encode("utf-16-le")) // 2 > limit:
            raise PlatformRequestBindingError("command UTF-16 limit exceeded")
        byte_limit = schema.get("x-maxDecodedBytes")
        if type(byte_limit) is int:
            try:
                decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
            except (ValueError, binascii.Error):
                raise PlatformRequestBindingError("command base64url invalid") from None
            if (
                len(decoded) > byte_limit
                or base64.urlsafe_b64encode(decoded).rstrip(b"=").decode() != value
            ):
                raise PlatformRequestBindingError("command base64url invalid")
    if isinstance(value, dict):
        properties = schema.get("properties")
        if type(properties) is dict:
            schema_properties = _object(
                schema.get("properties"), label="command schema properties"
            )
            for key, child in _object(raw_value, label="command object").items():
                child_schema = schema_properties.get(key)
                if type(child_schema) is dict:
                    _validate_command_extensions(
                        _object(schema_properties[key], label="command child schema"),
                        child,
                    )
    if isinstance(value, list):
        item_schema = schema.get("items")
        if type(item_schema) is dict:
            for item in _list(raw_value, label="command items"):
                _validate_command_extensions(
                    _object(schema.get("items"), label="command item schema"),
                    item,
                )


def _validate_command_admission(projection: Mapping[str, object]) -> None:
    command = _object(projection.get("command"), label="catalog command")
    owner = _object(command.get("owner_scope"), label="catalog owner")
    context = _object(command.get("product_context"), label="product context")
    product_owner = _object(context.get("owner_scope"), label="product owner")
    if (
        owner.get("kind") == "session"
        or owner != product_owner
        or owner.get("kind") == "user"
        and owner.get("id") != context.get("subject_id")
    ):
        raise PlatformRequestBindingError("catalog admission invalid")


def validate_platform_binding_artifact(root: Path) -> tuple[int, int, int]:
    """Validate the exact v3 owner tree, registry closure, and published vectors."""

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

    artifact = root / "contract/platform/v1/execution-operations/v4"
    expected_files = {
        Path(path)
        .relative_to("contract/platform/v1/execution-operations/v4")
        .as_posix()
        for path, _owner_path, _digest in EXPECTED_EXECUTION_SOURCES
    }
    actual_entries = {
        entry.relative_to(artifact).as_posix() for entry in artifact.rglob("*")
    }
    expected_directories = {
        parent.as_posix()
        for relative in expected_files
        for parent in Path(relative).parents
        if parent != Path(".")
    }
    if actual_entries != expected_files | expected_directories or any(
        entry.is_symlink() for entry in artifact.rglob("*")
    ):
        raise PlatformRequestBindingError("Platform execution artifact tree drift")
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
    if len(records) != 20:
        raise PlatformRequestBindingError("owner payload inventory is invalid")
    manifest = _object(
        strict_parse_raw_json((artifact / "manifest.json").read_bytes()),
        label="owner manifest",
    )
    payload_files = _list(manifest.get("payloadFiles"), label="manifest payload files")
    record_files = {
        _object(value, label="owner record").get("path") for value in records
    }
    if (
        set(payload_files) != record_files
        or set(payload_files) != expected_files - {"provenance.json"}
        or manifest.get("artifactVersion") != "4.0.0"
        or manifest.get("bindingVersion") != "3.0.0"
        or manifest.get("status") != "inactive"
        or manifest.get("routable") is not False
    ):
        raise PlatformRequestBindingError("owner manifest inventory drift")
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

    schemas = _object(manifest.get("schemas"), label="manifest schemas")
    for payload_name, schema_name in schemas.items():
        if type(schema_name) is not str or payload_name not in payload_files:
            raise PlatformRequestBindingError("owner schema path drift")
        schema = _object(
            strict_parse_raw_json((artifact / schema_name).read_bytes()),
            label="owner schema",
        )
        Draft202012Validator.check_schema(schema)
        document = strict_parse_raw_json((artifact / payload_name).read_bytes())
        try:
            validate_json_schema(
                instance=document, schema=schema, cls=Draft202012Validator
            )
        except JsonSchemaValidationError:
            raise PlatformRequestBindingError(
                f"owner schema mismatch: {payload_name}"
            ) from None

    catalog = _object(
        strict_parse_raw_json((artifact / "operation-catalog.json").read_bytes()),
        label="operation catalog",
    )
    operations = [
        _object(row, label="operation")
        for row in _list(catalog.get("operations"), label="operations")
    ]
    operation_by_name = {row.get("operation"): row for row in operations}
    if len(operations) != 34 or len(operation_by_name) != 34:
        raise PlatformRequestBindingError("operation catalog count drift")

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
    tenant_operations = {
        row.get("fqMethod"): row.get("operation")
        for row in operations
        if row.get("class") == "tenant-execution"
    }
    if (
        len(by_method) != 24
        or {method: row.get("operation") for method, row in by_method.items()}
        != tenant_operations
    ):
        raise PlatformRequestBindingError("tenant binding/operation registry drift")

    command_document = _object(
        strict_parse_raw_json((artifact / "command-identities.json").read_bytes()),
        label="command identities",
    )
    command_rows = [
        _object(row, label="command")
        for row in _list(command_document.get("commands"), label="commands")
    ]
    command_schemas = _object(
        _object(
            strict_parse_raw_json((artifact / "command-schemas.json").read_bytes()),
            label="command schemas",
        ).get("schemas"),
        label="command schemas",
    )
    registry = _object(
        strict_parse_raw_json((artifact / "projection-registry.json").read_bytes()),
        label="projection registry",
    )
    messages = _object(registry.get("messages"), label="projection messages")
    primitives = _object(registry.get("primitives"), label="projection primitives")
    command_operations = {row.get("operation") for row in command_rows}
    if len(command_rows) != 17 or command_operations != set(command_schemas):
        raise PlatformRequestBindingError("command registry/schema count drift")
    for row in command_rows:
        operation = row.get("operation")
        if type(operation) is not str or operation not in command_schemas:
            raise PlatformRequestBindingError("command operation identity drift")
        if operation_by_name.get(operation, {}).get("fqMethod") != row.get("fqMethod"):
            raise PlatformRequestBindingError("command operation/method drift")
        members = [
            _object(value, label="command member")
            for value in _list(row.get("commandMembers"), label="command members")
        ]
        for member in members:
            projector = member.get("projector")
            if type(projector) is not str or not (
                projector in messages
                or projector in primitives
                or projector.startswith("wrapper:")
                and projector.removeprefix("wrapper:") in primitives
            ):
                raise PlatformRequestBindingError("command projector registry drift")
        Draft202012Validator.check_schema(
            _object(command_schemas[operation], label="command schema")
        )

    zip_document = _object(
        strict_parse_raw_json((artifact / "vectors/zip-v1.json").read_bytes()),
        label="ZIP vectors",
    )
    zip_vectors = [
        _object(value, label="ZIP vector")
        for value in _list(zip_document.get("vectors"), label="ZIP inventory")
    ]
    if len(zip_vectors) != 29 or len({v.get("name") for v in zip_vectors}) != 29:
        raise PlatformRequestBindingError("ZIP vector inventory drift")
    for vector in zip_vectors:
        raw_zip = base64.b64decode(str(vector["zipBase64"]), validate=True)
        if hashlib.sha256(raw_zip).hexdigest() != vector["zipSha256"]:
            raise PlatformRequestBindingError("ZIP vector digest drift")
        identity = vector.get("manifestIdentity")
        if identity is not None and not isinstance(identity, str):
            raise PlatformRequestBindingError("ZIP vector identity drift")
        try:
            validate_package(
                raw_zip,
                skill_id=str(vector["skillId"]),
                revision=int(str(vector["revision"])),
                manifest_identity=identity,
            )
        except SkillPackageError as error:
            if str(error) != vector["expectedReason"]:
                raise PlatformRequestBindingError(
                    "ZIP negative vector reason drift"
                ) from None
        else:
            if vector["expectedReason"] != "none":
                raise PlatformRequestBindingError("ZIP negative vector accepted")

    positive_document = _object(
        strict_parse_raw_json((artifact / "vectors/positive.json").read_bytes()),
        label="positive vectors",
    )
    positives_value = positive_document["vectors"]
    positives = _list(positives_value, label="positive vector inventory")
    if len(positives) != 55:
        raise PlatformRequestBindingError("positive vector count drift")
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
    if len(negatives) != 142:
        raise PlatformRequestBindingError("negative vector count drift")
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

    command_vectors = _object(
        strict_parse_raw_json(
            (artifact / "vectors/command-projection.json").read_bytes()
        ),
        label="command vectors",
    )
    vectors = [
        _object(value, label="command vector")
        for value in _list(command_vectors.get("vectors"), label="command vectors")
    ]
    if len(vectors) != 165:
        raise PlatformRequestBindingError("command vector count drift")
    for vector in vectors:
        operation = vector.get("operation")
        if operation not in command_operations and operation != "raw":
            raise PlatformRequestBindingError("command vector operation drift")
        raw = base64.b64decode(str(vector.get("rawBase64")), validate=True)
        expected_error = vector.get("expectedError")
        if vector.get("stage") == "raw-parser" and expected_error != "none":
            try:
                strict_parse_raw_json(raw)
            except PlatformRequestBindingError:
                continue
            raise PlatformRequestBindingError("raw command negative vector accepted")
        if expected_error == "none":
            projection = _object(vector.get("projection"), label="command projection")
            if type(operation) is not str or operation not in command_schemas:
                raise PlatformRequestBindingError("command positive operation drift")
            schema = _object(command_schemas[operation], label="command schema")
            try:
                validate_json_schema(
                    instance=projection, schema=schema, cls=Draft202012Validator
                )
            except JsonSchemaValidationError:
                raise PlatformRequestBindingError(
                    "command positive schema drift"
                ) from None
            _validate_command_extensions(schema, projection)
            canonical = canonical_binding_bytes(
                _JSON_OBJECT.validate_python(projection, strict=True)
            )
            if canonical != base64.b64decode(
                str(vector.get("canonicalBase64")), validate=True
            ) or hashlib.sha256(canonical).hexdigest() != vector.get("sha256"):
                raise PlatformRequestBindingError(
                    "command positive canonical digest drift"
                )
            continue
        if type(operation) is not str or operation not in command_schemas:
            raise PlatformRequestBindingError("command negative operation drift")
        schema = _object(command_schemas[operation], label="command schema")
        try:
            decoded = _object(strict_parse_raw_json(raw), label="command negative wire")
            request = _object(
                decoded.pop("request", None), label="command negative request"
            )
            decoded["command"] = request
            validate_json_schema(
                instance=decoded, schema=schema, cls=Draft202012Validator
            )
            _validate_command_extensions(schema, decoded)
            if vector.get("stage") == "admission":
                _validate_command_admission(decoded)
        except (PlatformRequestBindingError, JsonSchemaValidationError):
            continue
        raise PlatformRequestBindingError(
            f"command negative vector accepted: {vector.get('name')}"
        )
    return len(positives), len(negatives), len(vectors)
