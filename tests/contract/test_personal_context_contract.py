"""HTTP8.11 finite private material delta and complete 8.10 inverse."""

from __future__ import annotations
import hashlib
import json
from typing import Any
from pathlib import Path
import pytest
from kokoro_agent.chat_contract_check import (
    http810_personal_context_parent_bytes,
    http811_project_instruction_parent_bytes,
    failure_model_bytes,
)
from kokoro_agent.contract_check import validate_openapi_document

ROOT = Path(__file__).resolve().parents[2]


def document() -> dict[str, Any]:
    return json.loads((ROOT / "contract/openapi/v1/openapi.json").read_text())


def historical_document():
    return json.loads(http811_project_instruction_parent_bytes(document()))


def test_personal_context_exact_parent_preserves_every_old_byte_and_capacity_failure() -> (
    None
):
    current = historical_document()
    assert current["info"]["version"] == "8.11.0"
    assert (
        hashlib.sha256(http810_personal_context_parent_bytes(current)).hexdigest()
        == "af76b63e7aeccec434bae30f41c554416dbd1ebf77871b94e67a5043221dbf6e"
    )
    validate_openapi_document(document())
    assert b'"context_window_exceeded"' in failure_model_bytes(current)
    schemas = current["components"]["schemas"]
    assert "personal_context" not in schemas["LaunchRequest"]["required"]
    assert (
        "context_window_exceeded"
        not in schemas["Failure"]["then"]["properties"]["code"]["enum"]
    )


@pytest.mark.parametrize("target", ["material", "failure", "old_route", "old_field"])
def test_material_inverse_rejects_new_or_historical_drift(target: str) -> None:
    current = historical_document()
    if target == "material":
        current["components"]["schemas"]["LaunchRequest"]["properties"][
            "personal_context"
        ]["oneOf"][1]["additionalProperties"] = True
    elif target == "failure":
        current["components"]["schemas"]["Failure"]["properties"]["code"][
            "enum"
        ].append("made_up")
    elif target == "old_route":
        current["paths"]["/v1/runs"]["post"]["operationId"] = "made_up"
    else:
        current["components"]["schemas"]["LaunchRequest"]["properties"]["content"][
            "maxLength"
        ] = 10
    with pytest.raises(ValueError):
        http810_personal_context_parent_bytes(current)


def test_shared_context_finite_delta_preserves_complete_issued_http817() -> None:
    from kokoro_agent.chat_contract_check import http817_shared_context_parent_bytes

    current = document()
    assert current["info"]["version"] == "8.18.0"
    parent = http817_shared_context_parent_bytes(current)
    assert (
        hashlib.sha256(parent).hexdigest()
        == "c471094369efa80f1182e41c67ef8150835a4544c283fd8d2d4e689707a51aad"
    )
    assert (
        "shared_conversation_context"
        not in current["components"]["schemas"]["LaunchRequest"]["required"]
    )
    validate_openapi_document(current)
    current["components"]["schemas"]["LaunchRequest"]["properties"][
        "shared_conversation_context"
    ]["additionalProperties"] = True
    with pytest.raises(ValueError):
        http817_shared_context_parent_bytes(current)
