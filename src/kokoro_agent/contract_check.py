"""Validate Agent-owned contracts and pinned consumer artifacts."""

from __future__ import annotations

from pathlib import Path

from pydantic import TypeAdapter, ValidationError

from kokoro_agent.chat_contract_check import (
    failure_model_bytes,
    generate_failure_models,
    read_contract_document,
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
    EXPECTED_EXECUTION_SOURCES,
    PLATFORM_PROVENANCE_RELATIVE,
    validate_platform_binding_artifact,
)

ROOT = Path(__file__).resolve().parents[2]
OPENAPI_RELATIVE = "contract/openapi/v1/openapi.json"
REQUIRED_PATHS = {
    "/healthz",
    "/readyz",
    "/v1/execution-proof/jwks",
    "/v1/runs",
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


def validate_openapi_document(document: dict[str, object]) -> None:
    openapi = document.get("openapi")
    if not isinstance(openapi, str) or not openapi.startswith("3."):
        raise ValueError("OpenAPI 3 document is required")
    info = _object(document.get("info"), source="info")
    if info.get("version") != "5.0.0":
        raise ValueError("Agent HTTP contract version must be 5.0.0")
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

    validate_openapi_document(read_contract_document(root / OPENAPI_RELATIVE))
    validate_http_provenance(
        root, read_contract_document(root / "contract/provenance.json")
    )
    generate_failure_models(root, check=True)
    validate_execution_proof_contract(root)
    validate_platform_binding_artifact(root)


def main() -> int:
    root = contract_audit_root(
        {
            *OWNER_SOURCE_FILES,
            "contract/provenance.json",
            PLATFORM_PROVENANCE_RELATIVE,
            *(record[0] for record in EXPECTED_EXECUTION_SOURCES),
        }
    )
    validate(root)
    print("kokoro-agent contract: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
