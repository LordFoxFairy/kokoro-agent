"""Validate Agent-owned OpenAPI and execution-proof machine contracts."""

from __future__ import annotations

from pathlib import Path

from pydantic import TypeAdapter, ValidationError

from kokoro_agent.execution_proof_contract import validate_execution_proof_contract


ROOT = Path(__file__).resolve().parents[2]
OPENAPI_RELATIVE = "contract/openapi/v1/openapi.json"
REQUIRED_PATHS = {
    "/healthz",
    "/readyz",
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


def _read_openapi(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise ValueError(f"contract source is missing: {path}")
    try:
        return _OBJECT.validate_json(path.read_bytes())
    except ValidationError as error:
        raise ValueError(f"{path} must contain a JSON object") from error


def _validate_openapi(document: dict[str, object]) -> None:
    openapi = document.get("openapi")
    if not isinstance(openapi, str) or not openapi.startswith("3."):
        raise ValueError("OpenAPI 3 document is required")
    info = _object(document.get("info"), source="info")
    if info.get("version") != "1.0.0":
        raise ValueError(
            "Agent HTTP contract version must remain 1.0.0 for the v1 source"
        )
    if document.get("x-kokoro-owner") != "kokoro-agent":
        raise ValueError("Agent must own its HTTP contract")
    paths = _object(document.get("paths"), source="paths")
    if set(paths) != REQUIRED_PATHS:
        raise ValueError("OpenAPI route set does not match the Agent owner boundary")
    for path_item in paths.values():
        path_object = _object(path_item, source="path item")
        for method, operation in path_object.items():
            if method not in {"get", "post", "put", "patch", "delete"}:
                continue
            operation_object = _object(operation, source="operation")
            missing = REQUIRED_EXTENSIONS - set(operation_object)
            if missing:
                raise ValueError(
                    f"operation governance metadata missing: {sorted(missing)}"
                )
            if operation_object["x-kokoro-owner"] != "kokoro-agent":
                raise ValueError("operation owner must be kokoro-agent")


def validate(root: Path = ROOT) -> None:
    """Validate all Agent-owned machine facts rooted at ``root``."""

    _validate_openapi(_read_openapi(root / OPENAPI_RELATIVE))
    validate_execution_proof_contract(root)


def main() -> int:
    validate()
    print("kokoro-agent contract: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
