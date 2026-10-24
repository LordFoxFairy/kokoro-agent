"""Additive owner wire, preserving the entire previous HTTP7 document."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from kokoro_agent.chat_contract_check import http72_parent_bytes
from kokoro_agent.contract_check import http73_parent_bytes

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
PATH = "/v1/model-call-attempts/{attempt_id}/prepared"
OLD_SHA = "ed0f5fc400f3307a4c50213a39859f2d5aacf8b4eecd11c1e50f53032d0fffd3"


def test_prepared_is_the_only_addition_to_complete_http7_parent() -> None:
    document = json.loads((ROOT / "contract/openapi/v1/openapi.json").read_text())
    document = json.loads(
        http72_parent_bytes(json.loads(http73_parent_bytes(document)))
    )
    assert document["info"]["version"] == "7.2.0"
    operation = document["paths"][PATH]["get"]
    assert operation["operationId"] == "getPreparedModelCall"
    assert operation["security"] == [{"BillingModelCallReaderBearer": []}]
    assert operation["x-kokoro-idempotency"] == "read-only"
    assert operation["x-kokoro-permission"] == "model-call-prepared.read"
    assert set(operation["responses"]) == {
        "200",
        "400",
        "401",
        "403",
        "404",
        "409",
        "500",
        "503",
    }
    assert (
        operation["responses"]["409"]["x-kokoro-error-code"]
        == "model_call_prepared_unavailable"
    )
    parent = copy.deepcopy(document)
    del parent["paths"][
        "/v1/model-call-attempts/{attempt_id}/usage-evidence/{event_id}"
    ]["get"]["description"]
    parent["info"]["version"] = "7.0.0"
    del parent["paths"][PATH]
    for name in ["PreparedModelCallSnapshot", "PreparedModelCallSnapshotEnvelope"]:
        del parent["components"]["schemas"][name]
    raw = (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
    assert hashlib.sha256(raw).hexdigest() == OLD_SHA


def test_prepared_envelope_is_closed_and_bounded() -> None:
    doc = json.loads((ROOT / "contract/openapi/v1/openapi.json").read_text())
    schema = doc["components"]["schemas"]["PreparedModelCallSnapshot"]
    Draft202012Validator.check_schema(schema)
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == set(schema["properties"])
    for name in ["attempt_binding", "planned", "authorization"]:
        raw = schema["properties"][f"{name}_bytes_base64url"]
        assert raw["maxLength"] == 87382
        assert raw["minLength"] == 1
        assert raw["pattern"] == "^[A-Za-z0-9_-]+$"
    assert schema["properties"]["status"]["enum"] == [
        "prepared",
        "authorization_pending",
        "authorized",
    ]
