"""Validate Agent-owned contracts and pinned consumer artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from pydantic import TypeAdapter, ValidationError

from kokoro_agent import model_usage_contract, model_usage_v2_contract
from kokoro_agent.chat_contract_check import (
    failure_model_bytes,
    http82_display_parent_bytes,
    http72_parent_bytes,
    http74_parent_bytes,
    generate_failure_models,
    read_contract_document as read_document,
    validate_chat_response_contract,
)
from kokoro_agent.distribution_assets import contract_audit_root
from kokoro_agent.execution_proof_contract import (
    OWNER_SOURCE_FILES,
    validate_execution_proof_contract,
    validate_execution_proof_jwk_set_component,
    validate_http_provenance,
)
from kokoro_agent.platform_binding_contract import (
    PLATFORM_AUDIT_FILES,
    validate_platform_binding_artifact,
)
from kokoro_agent.bff_admission_contract import (
    BFF_ADMISSION_AUDIT_FILES,
    validate_bff_admission_consumer,
)
from kokoro_agent.system_consumer_contract import validate_system_consumer_receipt
from kokoro_agent.usage_history_contract import validate_usage_history

ROOT = Path(__file__).resolve().parents[2]
OPENAPI_RELATIVE = "contract/openapi/v1/openapi.json"
REQUIRED_PATHS = {
    "/healthz",
    "/readyz",
    "/v1/execution-proof/jwks",
    "/v1/runs",
    "/v1/model-call-attempts/{attempt_id}/usage-evidence/{event_id}",
    "/v1/model-call-attempts/{attempt_id}/prepared",
    "/v1/runs/{run_id}/control",
    "/v1/runs/{run_id}/events",
    "/v1/sessions",
    "/v1/sessions/{session_id}/messages",
    "/v1/sessions/{session_id}/events",
}
REQUIRED_EXTENSIONS = {
    "x-kokoro-owner",
    "x-kokoro-visibility",
    "x-kokoro-stability",
    "x-kokoro-idempotency",
    "x-kokoro-permission",
}
_OBJECT = TypeAdapter(dict[str, object])


def _object(value: object, *, source: str) -> dict[str, object]:
    try:
        return _OBJECT.validate_python(value)
    except ValidationError as error:
        raise ValueError(
            f"{source} must contain a JSON object with string keys"
        ) from error


def http73_parent_bytes(document: dict[str, object]) -> bytes:
    """Remove only the optional Run selection and prove the complete published parent."""
    parent = _object(
        json.loads(http74_parent_bytes(document)), source="HTTP 7.4 parent"
    )
    info = _object(parent.get("info"), source="info")
    if info.get("version") != "7.4.0":
        raise ValueError("MCP selection requires HTTP 7.4.0")
    info["version"] = "7.3.0"
    parent["info"] = info
    components = _object(parent.get("components"), source="components")
    schemas = _object(components.get("schemas"), source="schemas")
    launch = _object(schemas.get("LaunchRequest"), source="LaunchRequest")
    properties = _object(launch.get("properties"), source="LaunchRequest properties")
    selection = properties.pop("selected_mcp_connection_ids", None)
    if selection != {
        "type": "array",
        "maxItems": 16,
        "uniqueItems": True,
        "x-kokoro-json-byte-limit": 4096,
        "description": "Exact Platform MCP Connection IDs selected for this Run, in caller order. Omission or [] selects no additional connections; selection is intent, not authorization. JSON ASCII encoding is limited to 4096 bytes.",
        "items": {
            "type": "string",
            "minLength": 1,
            "maxLength": 191,
            "pattern": r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}(?![\s\S])",
        },
    }:
        raise ValueError("MCP selection extension is not exact")
    launch["properties"] = properties
    schemas["LaunchRequest"] = launch
    components["schemas"] = schemas
    parent["components"] = components
    raw = (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
    if (
        hashlib.sha256(raw).hexdigest()
        != "c096eab9f2aa392215351a106a670692d5f58f945980a5e028b3a83d17509731"
    ):
        raise ValueError("complete HTTP 7.3 parent differs")
    return raw


def validate_openapi_document(document: dict[str, object]) -> None:
    if _object(document.get("info"), source="info").get("version") != "8.6.0":
        raise ValueError("Agent HTTP contract version must be 8.6.0")
    document = _object(
        json.loads(http82_display_parent_bytes(document)),
        source="HTTP8.2 display parent",
    )
    validate_http82_parent_document(document)


def validate_http82_parent_document(document: dict[str, object]) -> None:
    """The preserved HTTP8.2 validator, also used for historical risk fixtures."""
    openapi = document.get("openapi")
    if not isinstance(openapi, str) or not openapi.startswith("3."):
        raise ValueError("OpenAPI 3 document is required")
    info = _object(document.get("info"), source="info")
    if info.get("version") != "8.2.0":
        raise ValueError("Agent HTTP contract version must be 8.2.0")
    if document.get("x-kokoro-owner") != "kokoro-agent":
        raise ValueError("Agent must own its HTTP contract")
    paths = _object(document.get("paths"), source="paths")
    if set(paths) != REQUIRED_PATHS:
        raise ValueError("OpenAPI route set does not match the Agent owner boundary")
    for path_item in paths.values():
        path_object = _object(path_item, source="path item")
        for method, operation in path_object.items():
            if method not in {"get", "head", "post", "put", "patch", "delete"}:
                continue
            operation_object = _object(operation, source="operation")
            missing = REQUIRED_EXTENSIONS - set(operation_object)
            if missing:
                raise ValueError(
                    f"operation governance metadata missing: {sorted(missing)}"
                )
            if operation_object["x-kokoro-owner"] != "kokoro-agent":
                raise ValueError("operation owner must be kokoro-agent")
    _validate_jwks(paths)
    validate_execution_proof_jwk_set_component(document)
    validate_chat_response_contract(document, paths)
    failure_model_bytes(document)
    http72_parent_bytes(
        _object(json.loads(http73_parent_bytes(document)), source="HTTP 7.3 parent")
    )


def _validate_jwks(paths: dict[str, object]) -> None:
    path = _object(paths.get("/v1/execution-proof/jwks"), source="JWKS path")
    if set(path) != {"get", "head"}:
        raise ValueError("JWKS must expose exact GET and HEAD operations")
    expected_errors = {
        "400": "execution_proof_jwks_invalid_request",
        "405": "execution_proof_jwks_method_not_allowed",
        "503": "execution_proof_jwks_unavailable",
    }
    for method in ("get", "head"):
        operation = _object(path.get(method), source=f"JWKS {method}")
        if (
            operation.get("security") != []
            or operation.get("x-kokoro-owner") != "kokoro-agent"
            or operation.get("x-kokoro-visibility") != "internal-owner"
            or operation.get("x-kokoro-stability") != "stable"
            or operation.get("x-kokoro-idempotency") != "read-only"
            or operation.get("x-kokoro-permission") != "none"
        ):
            raise ValueError("JWKS governance or anonymous security is invalid")
        responses = _object(operation.get("responses"), source="JWKS responses")
        if set(responses) != {"200", "400", "405", "503"}:
            raise ValueError("JWKS response set is invalid")
        for status, expected in expected_errors.items():
            response = _object(responses.get(status), source=f"JWKS {status}")
            if response.get("x-kokoro-error-code") != expected:
                raise ValueError("JWKS stable error code is invalid")
            content = _object(response.get("content"), source="JWKS error media")
            if set(content) != {"application/json"}:
                raise ValueError("JWKS error media is invalid")
            error_media = _object(content["application/json"], source="JWKS error")
            if error_media.get("schema") != {
                "$ref": "#/components/schemas/ErrorEnvelope"
            }:
                raise ValueError("JWKS error envelope schema is invalid")
            _validate_representation_headers(response, allow=status == "405")
        success = _object(responses.get("200"), source="JWKS 200")
        success_content = _object(success.get("content"), source="JWKS success media")
        if set(success_content) != {"application/jwk-set+json"}:
            raise ValueError("JWKS success media is invalid")
        media = _object(
            success_content["application/jwk-set+json"], source="JWKS success body"
        )
        if media.get("schema") != {"$ref": "#/components/schemas/ExecutionProofJwkSet"}:
            raise ValueError("JWKS success schema reference is invalid")
        _validate_representation_headers(success, allow=False)


def _validate_representation_headers(
    response: dict[str, object], *, allow: bool
) -> None:
    headers = _object(response.get("headers"), source="JWKS response headers")
    required: set[str] = {"Cache-Control", "Content-Length"}
    if allow:
        required.add("Allow")
    if set(headers) != required:
        raise ValueError("JWKS response headers are invalid")
    cache = _object(headers["Cache-Control"], source="JWKS cache header")
    if cache.get("required") is not True or cache.get("schema") != {
        "type": "string",
        "const": "no-store",
    }:
        raise ValueError("JWKS cache policy is invalid")
    length = _object(headers["Content-Length"], source="JWKS length header")
    if length.get("required") is not True or length.get("schema") != {
        "type": "string",
        "pattern": "^[0-9]+$",
    }:
        raise ValueError("JWKS length semantics are invalid")
    if allow:
        item = _object(headers["Allow"], source="JWKS allow header")
        if item.get("required") is not True or item.get("schema") != {
            "type": "string",
            "const": "GET, HEAD",
        }:
            raise ValueError("JWKS Allow header is invalid")


def validate(root: Path = ROOT) -> None:
    """Validate all Agent-owned machine facts rooted at ``root``."""

    validate_openapi_document(read_document(root / OPENAPI_RELATIVE))
    validate_http_provenance(root, read_document(root / "contract/provenance.json"))
    validate_system_consumer_receipt(root)
    validate_bff_admission_consumer(root)
    generate_failure_models(root, check=True)
    validate_execution_proof_contract(root)
    validate_platform_binding_artifact(root)
    validate_usage_history(root)
    model_usage_v2_contract.validate_model_usage_contract(root)


def main() -> int:
    root = contract_audit_root(
        {
            *OWNER_SOURCE_FILES,
            *model_usage_contract.USAGE_AUDIT_FILES,
            *model_usage_v2_contract.USAGE_AUDIT_FILES,
            "contract/provenance.json",
            *PLATFORM_AUDIT_FILES,
            *BFF_ADMISSION_AUDIT_FILES,
        }
    )
    validate(root)
    print("kokoro-agent contract: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
