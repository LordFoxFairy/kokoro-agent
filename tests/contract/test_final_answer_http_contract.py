"""The optional decoded success binding is the sole HTTP8.8 addition."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any
import pytest
from kokoro_agent.chat_contract_check import (
    http87_final_answer_parent_bytes,
    validate_decoded_chat_payload,
)


def _document() -> dict[str, Any]:
    return json.loads(
        (
            Path(__file__).resolve().parents[2] / "contract/openapi/v1/openapi.json"
        ).read_text()
    )


def test_complete_http87_parent_is_preserved_without_other_deltas() -> None:
    document = _document()
    assert document["info"]["version"] == "8.8.0"
    assert (
        hashlib.sha256(http87_final_answer_parent_bytes(document)).hexdigest()
        == "74fb43159ba559fe049f027a5fe4877d29524f5a73eae88c8d15913760cfb9fc"
    )
    changed = copy.deepcopy(document)
    changed["components"]["schemas"]["ChatRunCompleted"]["properties"][
        "final_message_id"
    ]["minLength"] = 0
    with pytest.raises(ValueError):
        http87_final_answer_parent_bytes(changed)


@pytest.mark.parametrize(
    "payload,valid",
    [
        ({"status": "completed"}, True),
        ({"status": "cancelled"}, True),
        ({"status": "completed", "final_message_id": "msg-public"}, True),
        ({"status": "cancelled", "final_message_id": "msg-public"}, False),
        ({"status": "completed", "final_message_id": None}, False),
        ({"status": "completed", "final_message_id": ""}, False),
        ({"status": "completed", "final_segment_id": "native"}, False),
    ],
)
def test_decoded_terminal_binding_requires_success_and_uses_public_identity(
    payload: dict[str, Any], valid: bool
) -> None:
    if valid:
        validate_decoded_chat_payload(
            _document(), event_type="run.completed", payload_json=json.dumps(payload)
        )
    else:
        with pytest.raises(ValueError):
            validate_decoded_chat_payload(
                _document(),
                event_type="run.completed",
                payload_json=json.dumps(payload),
            )
