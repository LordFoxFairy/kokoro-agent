"""A private legacy final proof is the sole additive HTTP8.9 change."""

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator


def test_final_display_preserves_complete_issued_parent_and_rejects_null() -> None:
    from kokoro_agent.chat_contract_check import http88_final_display_parent_bytes

    path = Path(__file__).resolve().parents[2] / "contract/openapi/v1/openapi.json"
    document = json.loads(path.read_text())
    assert document["info"]["version"] == "8.9.0"
    assert hashlib.sha256(http88_final_display_parent_bytes(document)).hexdigest() == (
        "fd8b2857ecbc9577bd98ca73234e4d81c53d4fe189ba86badcf35c774b9a6830"
    )
    field = document["components"]["schemas"]["RunDisplay"]["properties"][
        "final_message_id"
    ]
    validator: Any = Draft202012Validator(field)
    validator.validate("msg_" + "a" * 32)
    for value in [None, "", "native-id", "msg_" + "a" * 32 + "\n"]:
        assert list(validator.iter_errors(value))
    for mutation in ["field", "old"]:
        changed = copy.deepcopy(document)
        if mutation == "field":
            changed["components"]["schemas"]["RunDisplay"]["properties"][
                "final_message_id"
            ]["pattern"] = ".*"
        else:
            changed["components"]["schemas"]["RunDisplay"]["properties"]["tool_paths"][
                "maxItems"
            ] = 129
        with pytest.raises(ValueError):
            http88_final_display_parent_bytes(changed)
