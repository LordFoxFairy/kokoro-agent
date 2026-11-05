"""The additive project launch field must not alter any issued HTTP 8.11 fact."""

import hashlib
import json
from pathlib import Path

from kokoro_agent.contract_check import validate_openapi_document

ROOT = Path(__file__).resolve().parents[2]
PARENT_SHA = "ad34ba2c8080f6a459a52116f929ed2c5f017a742dcb5d17d0e165ac18f1772b"


def test_project_instruction_is_an_optional_launch_only_contract_delta() -> None:
    current = json.loads((ROOT / "contract/openapi/v1/openapi.json").read_text())
    assert current["info"]["version"] == "8.12.0"
    validate_openapi_document(current)
    launch = current["components"]["schemas"]["LaunchRequest"]
    assert launch["properties"].pop("project_instruction")["type"] == "string"
    assert "project_instruction" not in launch["required"]
    assert current["paths"]["/v1/runs"]["post"]["x-kokoro-max-body-bytes"] == 1048576
    current["info"]["version"] = "8.11.0"
    raw = (json.dumps(current, indent=2, ensure_ascii=False) + "\n").encode()
    assert hashlib.sha256(raw).hexdigest() == PARENT_SHA
