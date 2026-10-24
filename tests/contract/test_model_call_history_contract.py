"""HTTP7.2 expands only the exact sealed read, preserving all prior owner wire."""

from pathlib import Path
from kokoro_agent.chat_contract_check import http72_parent_bytes
import hashlib
import json

ROOT = Path(__file__).resolve().parents[2]
PATH = "/v1/model-call-attempts/{attempt_id}/usage-evidence/{event_id}"
HTTP71_SHA = "6c0f532d983367ce3a22e05ffab00f83b7a4481de04f7121e72dd2a5098365a5"


def test_history_read_is_explicit_and_the_entire_http71_parent_is_preserved() -> None:
    doc = json.loads((ROOT / "contract/openapi/v1/openapi.json").read_text())
    doc = json.loads(http72_parent_bytes(doc))
    assert doc["info"]["version"] == "7.2.0"
    op = doc["paths"][PATH]["get"]
    assert op["security"] == [{"BillingModelCallReaderBearer": []}]
    text = op["description"]
    for term in ["historical", "current head", "immutable", "execution permission"]:
        assert term in text
    doc["info"]["version"] = "7.1.0"
    del op["description"]
    raw = (json.dumps(doc, indent=2, ensure_ascii=False) + "\n").encode()
    assert hashlib.sha256(raw).hexdigest() == HTTP71_SHA
