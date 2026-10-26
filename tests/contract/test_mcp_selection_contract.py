"""An optional precise business selection is the only change from HTTP 7.3."""

import copy
import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import ValidationError, validate
import kokoro_agent.contract_check as checks

ROOT = Path(__file__).resolve().parents[2]
PARENT_SHA = "c096eab9f2aa392215351a106a670692d5f58f945980a5e028b3a83d17509731"


def _document():
    return json.loads((ROOT / "contract/openapi/v1/openapi.json").read_text())


def test_http74_adds_only_optional_exact_connection_selection():
    document = _document()
    assert document["info"]["version"] == "8.0.0"
    schema = document["components"]["schemas"]["LaunchRequest"]
    assert "selected_mcp_connection_ids" not in schema["required"]
    selection = schema["properties"]["selected_mcp_connection_ids"]
    assert selection["x-kokoro-json-byte-limit"] == 4096
    for value in ([], ["connection-1"], ["b", "a"]):
        validate(value, selection)
    for invalid in (
        None,
        "x",
        [True],
        ["x", "x"],
        ["a\n"],
        ["https://host/mcp"],
        ["a" * 192],
        [f"c{i}" for i in range(17)],
    ):
        with pytest.raises(ValidationError):
            validate(invalid, selection)
    assert (
        hashlib.sha256(checks.http73_parent_bytes(document)).hexdigest() == PARENT_SHA
    )
    checks.validate_openapi_document(document)


@pytest.mark.parametrize(
    "mutation",
    ["required", "nullable", "duplicates", "pattern", "auth", "failure", "route"],
)
def test_http73_parent_proof_rejects_unrelated_or_weakened_changes(mutation: str):
    document = copy.deepcopy(_document())
    launch = document["components"]["schemas"]["LaunchRequest"]
    selection = launch["properties"]["selected_mcp_connection_ids"]
    if mutation == "required":
        launch["required"].append("selected_mcp_connection_ids")
    elif mutation == "nullable":
        selection["type"] = ["array", "null"]
    elif mutation == "duplicates":
        selection["uniqueItems"] = False
    elif mutation == "pattern":
        selection["items"]["pattern"] = ".*"
    elif mutation == "auth":
        document["paths"]["/v1/runs"]["post"]["security"] = []
    elif mutation == "failure":
        document["components"]["schemas"]["Failure"]["properties"]["code"]["enum"].pop()
    else:
        del document["paths"]["/readyz"]
    with pytest.raises(ValueError):
        checks.http73_parent_bytes(document)
