"""Only the safe pending failure is added; no funds or usage schema is changed."""

from pathlib import Path
import copy
import hashlib
import json
import pytest
from kokoro_agent.chat_contract_check import http72_parent_bytes
from jsonschema import validate, ValidationError

ROOT = Path(__file__).resolve().parents[2]
PARENT_SHA = "4e75244c676e02736867d2f461d18b4d67d93151ff03ffaf6e632685b77ca9db"


def test_pending_code_is_nonretryable_and_only_http72_change():
    document = json.loads((ROOT / "contract/openapi/v1/openapi.json").read_text())
    assert document["info"]["version"] == "7.3.0"
    failure = document["components"]["schemas"]["Failure"]
    validate({"code": "model_settlement_pending", "retryable": False}, failure)
    with pytest.raises(ValidationError):
        validate({"code": "model_settlement_pending", "retryable": True}, failure)
    with pytest.raises(ValidationError):
        validate({"code": "invented_financial_success", "retryable": False}, failure)
    parent = copy.deepcopy(document)
    assert (
        parent["components"]["schemas"]["Failure"]["properties"]["code"]["enum"].pop()
        == "model_settlement_pending"
    )
    parent["info"]["version"] = "7.2.0"
    assert (
        hashlib.sha256(
            (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
        ).hexdigest()
        == PARENT_SHA
    )


@pytest.mark.parametrize(
    "mutation", ["other_schema", "retryable", "duplicate_code", "path"]
)
def test_pending_extension_proof_rejects_other_owner_changes(mutation: str) -> None:
    document = json.loads((ROOT / "contract/openapi/v1/openapi.json").read_text())
    failure = document["components"]["schemas"]["Failure"]
    if mutation == "other_schema":
        document["components"]["schemas"]["Meta"]["additionalProperties"] = True
    elif mutation == "retryable":
        failure["then"]["properties"]["code"]["enum"].append("model_settlement_pending")
    elif mutation == "duplicate_code":
        failure["properties"]["code"]["enum"].append("model_settlement_pending")
    else:
        del document["paths"]["/v1/runs"]
    with pytest.raises(ValueError):
        http72_parent_bytes(document)
