"""HTTP8.10 adds only an optional safe exact CustomAPI profile selection."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from kokoro_agent.chat_contract_check import http89_custom_api_parent_bytes
from kokoro_agent.contract_check import validate_openapi_document

ROOT = Path(__file__).parents[2]
PARENT_SHA = "875545c23cf4cd055adfb4aedf6ea3c994eab6be10a25f1f68db248ffecb32c9"


def document():
    return json.loads((ROOT / "contract/openapi/v1/openapi.json").read_text())


def test_exact_parent_bytes_and_selection_shape() -> None:
    current = document()
    assert current["info"]["version"] == "8.10.0"
    assert (
        hashlib.sha256(http89_custom_api_parent_bytes(current)).hexdigest()
        == PARENT_SHA
    )
    launch = current["components"]["schemas"]["LaunchRequest"]
    assert "selected_custom_api_profiles" not in launch["required"]
    selection = launch["properties"]["selected_custom_api_profiles"]
    assert selection["x-kokoro-unique-by"] == "profile_id"
    validator: Any = Draft202012Validator(selection)
    for value in (
        [],
        [{"profile_id": "p", "expected_state_version": 1}],
        [
            {"profile_id": str(i), "expected_state_version": 2147483647}
            for i in range(32)
        ],
    ):
        validator.validate(value)
    for value in (
        None,
        ["p"],
        [{"profile_id": "p"}],
        [{"profile_id": "p", "expected_state_version": True}],
        [{"profile_id": "p\n", "expected_state_version": 1}],
        [{"profile_id": "p", "expected_state_version": 0}],
        [{"profile_id": "p", "expected_state_version": 2147483648}],
        [{"profile_id": "p", "expected_state_version": 1, "value": "secret"}],
    ):
        assert list(validator.iter_errors(value))
    validate_openapi_document(current)


@pytest.mark.parametrize(
    "mutation",
    [
        "required",
        "nullable",
        "unique",
        "pattern",
        "version",
        "extra",
        "auth",
        "unrelated",
    ],
)
def test_complete_child_and_parent_drift_fail_closed(mutation: str) -> None:
    current = copy.deepcopy(document())
    launch = current["components"]["schemas"]["LaunchRequest"]
    selection = launch["properties"]["selected_custom_api_profiles"]
    if mutation == "required":
        launch["required"].append("selected_custom_api_profiles")
    elif mutation == "nullable":
        selection["type"] = ["array", "null"]
    elif mutation == "unique":
        selection["x-kokoro-unique-by"] = "expected_state_version"
    elif mutation == "pattern":
        selection["items"]["properties"]["profile_id"]["pattern"] = ".*"
    elif mutation == "version":
        selection["items"]["properties"]["expected_state_version"]["maximum"] += 1
    elif mutation == "extra":
        selection["items"]["additionalProperties"] = True
    elif mutation == "auth":
        current["paths"]["/v1/runs"]["post"]["security"] = []
    else:
        current["components"]["schemas"]["RunDisplay"]["properties"]["tool_paths"][
            "maxItems"
        ] = 129
    with pytest.raises(ValueError):
        http89_custom_api_parent_bytes(current)
