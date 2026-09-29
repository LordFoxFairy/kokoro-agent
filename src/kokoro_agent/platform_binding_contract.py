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
from kokoro_agent.execution.platform_request_binding_values import (
    PlatformRequestBindingError,
    canonical_binding_bytes,
)


JsonObject: TypeAlias = dict[str, object]
EXPECTED_OWNER_COMMIT = "5b6eb2c1532b23b9747bc4bf6ac99f69ad453de0"
EXPECTED_OWNER_REPOSITORY = "apps/kokoro-capability"
EXPECTED_AGGREGATE = "324e749da1bc66c1ff03de74e7299716f798f5f5bb5fa19556033b79fa09ff8d"
EXPECTED_OWNER_PROVENANCE_SHA256 = (
    "952cb00840fba598479e95eb3bcfa1a8ffe326a47167325f9edfd15d0f87c2e2"
)
EXPECTED_EXECUTION_SOURCES = (
    (
        "contract/platform/v1/execution-operations/v3/command-identities.json",
        "contract/execution-operations/v3/command-identities.json",
        "e9d38c5bfa47b083dc66435b6e21b3cfdfe1762e29ebab3a6bd86c8b86729e28",
    ),
    (
        "contract/platform/v1/execution-operations/v3/command-schemas.json",
        "contract/execution-operations/v3/command-schemas.json",
        "30cda8f01b6fd8246826c1a7a909fffee4795766ab5328d8d653ae2003ee71b3",
    ),
    (
        "contract/platform/v1/execution-operations/v3/identities.json",
        "contract/execution-operations/v3/identities.json",
        "60e6fb68ad040860983f333839196c872e29c81965bdc89de422c8a67b5b5359",
    ),
    (
        "contract/platform/v1/execution-operations/v3/manifest.json",
        "contract/execution-operations/v3/manifest.json",
        "b8b33095b02ac51cfcbfef7ee4cc8a7e5e0dca9025a87a2b2d31885b41dac4a3",
    ),
    (
        "contract/platform/v1/execution-operations/v3/operation-catalog.json",
        "contract/execution-operations/v3/operation-catalog.json",
        "9a8b7eebb11157d08bf299a33c44a0d975376d238b3827e4de5a48e917d7dc49",
    ),
    (
        "contract/platform/v1/execution-operations/v3/projection-registry.json",
        "contract/execution-operations/v3/projection-registry.json",
        "fccdcbc9161ca3f0ab1e5b094675be699463338df3bd1b65290fd998322f50f5",
    ),
    (
        "contract/platform/v1/execution-operations/v3/provenance.json",
        "contract/execution-operations/v3/provenance.json",
        "952cb00840fba598479e95eb3bcfa1a8ffe326a47167325f9edfd15d0f87c2e2",
    ),
    (
        "contract/platform/v1/execution-operations/v3/request-bindings.json",
        "contract/execution-operations/v3/request-bindings.json",
        "2572ec3a0357c02ac0d2291db3d4b9e53588ce8bb8f63742a013cebb6c4c401d",
    ),
    (
        "contract/platform/v1/execution-operations/v3/schemas/command-identities.schema.json",
        "contract/execution-operations/v3/schemas/command-identities.schema.json",
        "523562734e01f7c529c80abcf3a11ae5874dd9ce4fa36f5b2ed0f896c2bd66ab",
    ),
    (
        "contract/platform/v1/execution-operations/v3/schemas/identities.schema.json",
        "contract/execution-operations/v3/schemas/identities.schema.json",
        "faadbbc5984fbabe1bb68703c2e2213b9598342eae073dd557fb2573b427ef87",
    ),
    (
        "contract/platform/v1/execution-operations/v3/schemas/manifest.schema.json",
        "contract/execution-operations/v3/schemas/manifest.schema.json",
        "5b4274edc000ceb064adf549259064486e757c4621805e9a424b55697217f158",
    ),
    (
        "contract/platform/v1/execution-operations/v3/schemas/operation-catalog.schema.json",
        "contract/execution-operations/v3/schemas/operation-catalog.schema.json",
        "5698936d0c36139088fa2f141c67cf6b8ec5319c7adcc38faeaa0681ffd55d99",
    ),
    (
        "contract/platform/v1/execution-operations/v3/schemas/request-bindings.schema.json",
        "contract/execution-operations/v3/schemas/request-bindings.schema.json",
        "6febac6d74bf6777252feebc606ff185e400cded9822f5a29633fa07f12881e4",
    ),
    (
        "contract/platform/v1/execution-operations/v3/schemas/vector-inventory.schema.json",
        "contract/execution-operations/v3/schemas/vector-inventory.schema.json",
        "ce812f46d0332254be61af0a54bfff3761dce7ab127f825fde0ac012bbf8b204",
    ),
    (
        "contract/platform/v1/execution-operations/v3/vectors/command-projection.json",
        "contract/execution-operations/v3/vectors/command-projection.json",
        "05b6d51ea424ba50473a44cfadee0606e42e628affd8ac28f85f31cd9c10083b",
    ),
    (
        "contract/platform/v1/execution-operations/v3/vectors/negative.json",
        "contract/execution-operations/v3/vectors/negative.json",
        "c674f7add6eeea1ccc93ce42862a2902e5f5e7adfb0edce53c6b00616ef11a1a",
    ),
    (
        "contract/platform/v1/execution-operations/v3/vectors/positive.json",
        "contract/execution-operations/v3/vectors/positive.json",
        "8d5434d356415e1990b2f0b7dd9e2f30b53817d718343d579b784e3ca67da313",
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

    artifact = root / "contract/platform/v1/execution-operations/v3"
    expected_files = {
        Path(path)
        .relative_to("contract/platform/v1/execution-operations/v3")
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
    if len(records) != 16:
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
        or manifest.get("artifactVersion") != "3.0.0"
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
    if len(operations) != 31 or len(operation_by_name) != 31:
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
    if len(command_rows) != 15 or command_operations != set(command_schemas):
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

    positive_document = _object(
        strict_parse_raw_json((artifact / "vectors/positive.json").read_bytes()),
        label="positive vectors",
    )
    positives_value = positive_document["vectors"]
    positives = _list(positives_value, label="positive vector inventory")
    if len(positives) != 53:
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
    if len(vectors) != 139:
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
