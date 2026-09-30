"""Agent HTTP machine contract is present, owned, and covers every route."""

from __future__ import annotations

import json
from pathlib import Path
from jsonschema import Draft202012Validator
from pydantic import TypeAdapter
import pytest


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
    assert _object(document["info"])["version"] == "2.0.0"
    assert document["x-kokoro-owner"] == "kokoro-agent"
    assert document["x-kokoro-visibility"] == "internal-owner"
    assert CONTRACT_README.is_file()


def test_launch_requires_exact_typed_skill_selection_schema() -> None:
    document = _document()
    schema = _object(_object(document["components"])["schemas"])["LaunchRequest"]
    launch = _object(schema)
    required = launch["required"]
    assert isinstance(required, list)
    assert "selected_skill_source_refs" in required
    refs = _object(_object(launch["properties"])["selected_skill_source_refs"])
    assert refs["maxItems"] == 16
    assert refs["uniqueItems"] is True
    assert refs["x-kokoro-json-byte-limit"] == 4096
    assert _object(refs["items"])["pattern"] == (
        r"^skill:(?!skill:)[A-Za-z0-9][A-Za-z0-9._:-]{0,190}(?![\s\S])"
    )


@pytest.mark.parametrize("suffix", ["\n", "\r", "\r\n", "\u2028", "\u2029"])
def test_skill_selection_schema_rejects_trailing_line_terminators(suffix: str) -> None:
    document = _document()
    schema = _object(_object(document["components"])["schemas"])["LaunchRequest"]
    refs = _object(_object(_object(schema)["properties"])["selected_skill_source_refs"])
    validator = Draft202012Validator(refs)
    assert validator.is_valid(["skill:valid"])
    assert not validator.is_valid([f"skill:valid{suffix}"])


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
