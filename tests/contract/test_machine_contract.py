"""Agent HTTP machine contract is present, owned, and covers every route."""

from __future__ import annotations

import json
from pathlib import Path
from pydantic import TypeAdapter


ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "contract" / "openapi" / "v1" / "openapi.json"
CONTRACT_README = ROOT / "contract" / "README.md"
_OBJECT = TypeAdapter(dict[str, object])


def _object(value: object) -> dict[str, object]:
    return _OBJECT.validate_python(value)


def _document() -> dict[str, object]:
    return _object(json.loads(CONTRACT.read_text(encoding="utf-8")))


def test_agent_http_contract_is_versioned_and_owned() -> None:
    document = _document()
    assert isinstance(document["openapi"], str)
    assert document["openapi"].startswith("3.")
    assert _object(document["info"])["version"] == "1.1.0"
    assert document["x-kokoro-owner"] == "kokoro-agent"
    assert document["x-kokoro-visibility"] == "internal-owner"
    assert CONTRACT_README.is_file()


def test_every_operation_declares_governance_metadata() -> None:
    document = _document()
    paths = _object(document["paths"])
    operations: list[dict[str, object]] = [
        _object(operation)
        for path_item in paths.values()
        for method, operation in _object(path_item).items()
        if method in {"get", "head", "post", "put", "patch", "delete"}
    ]
    assert operations
    for operation in operations:
        assert operation["x-kokoro-owner"] == "kokoro-agent"
        assert operation["x-kokoro-visibility"] == "internal-owner"
        assert operation["x-kokoro-stability"] == "stable"
        assert "x-kokoro-permission" in operation


def test_http_route_set_is_explicit() -> None:
    document = _document()
    paths = _object(document["paths"])
    assert set(paths) == {
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
