from __future__ import annotations
import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest


def test_http83_display_is_the_only_delta_from_exact_published_http82() -> None:
    from kokoro_agent.chat_contract_check import http82_display_parent_bytes

    document = json.loads(
        (
            Path(__file__).resolve().parents[2] / "contract/openapi/v1/openapi.json"
        ).read_text()
    )
    assert document["info"]["version"] == "8.3.0"
    assert (
        hashlib.sha256(http82_display_parent_bytes(document)).hexdigest()
        == "8a0b9973661205b8e29020934c39e5c7ab629b4c42db4e79c845a410d96efa2a"
    )

    def missing_route(doc: dict[str, Any]) -> None:
        doc["paths"].pop("/v1/runs/{run_id}/display")

    def wrong_old_field(doc: dict[str, Any]) -> None:
        doc["components"]["schemas"]["ChatActivity"]["oneOf"][0]["properties"].pop(
            "display_code"
        )

    def wrong_bound(doc: dict[str, Any]) -> None:
        doc["components"]["schemas"]["RunDisplay"]["properties"]["tool_paths"].update(
            maxItems=129
        )

    for mutate in [missing_route, wrong_old_field, wrong_bound]:
        changed = copy.deepcopy(document)
        mutate(changed)
        with pytest.raises(ValueError):
            http82_display_parent_bytes(changed)


@pytest.mark.parametrize("check", [False, True])
def test_current_generator_rejects_a_complete_old_parent_before_any_write(
    tmp_path: Path, check: bool
) -> None:
    import shutil
    from kokoro_agent.chat_contract_check import (
        generate_failure_models,
        http82_display_parent_bytes,
    )
    from kokoro_agent.execution_proof_contract import OWNER_SOURCE_FILES

    root = Path(__file__).resolve().parents[2]
    for relative in {
        *OWNER_SOURCE_FILES,
        "contract/provenance.json",
        "src/kokoro_agent/protocol/run_failure_generated.py",
    }:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / relative, target)
    source = tmp_path / "contract/openapi/v1/openapi.json"
    source.write_bytes(http82_display_parent_bytes(json.loads(source.read_text())))
    before = {
        p.relative_to(tmp_path): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    with pytest.raises(ValueError, match="HTTP 8.3.0"):
        generate_failure_models(tmp_path, check=check)
    assert {
        p.relative_to(tmp_path): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    } == before
