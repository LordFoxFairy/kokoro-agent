"""The fixed BFF publication is the only editable source for budget wire fields."""

from pathlib import Path
from copy import deepcopy

import pytest
from pydantic import ValidationError

from kokoro_agent.bff_admission_contract import generate_bff_admission_models
from kokoro_agent.clients.bff_admission_generated import (
    AgentModelAdmissionRequest,
    AgentModelAdmissionResponse,
)

ROOT = Path(__file__).resolve().parents[2]


def test_bff_consumer_is_reproducible_from_one_fixed_publication() -> None:
    generate_bff_admission_models(ROOT, check=True)


def test_request_is_closed_and_uses_codepoint_lengths() -> None:
    data = dict(
        run_id="😀" * 512,
        request_ref="request",
        attempt_id="11111111-1111-4111-8111-111111111111",
        attempt_binding_digest="a" * 64,
        planned_digest="b" * 64,
        final_request_digest="c" * 64,
    )
    assert AgentModelAdmissionRequest.model_validate(data).run_id == data["run_id"]
    for change in [
        {"run_id": "😀" * 513},
        {"tenant_id": "injected"},
        {"attempt_id": data["attempt_id"] + "\n"},
        {"planned_digest": "b" * 64 + "\n"},
    ]:
        with pytest.raises(ValidationError):
            AgentModelAdmissionRequest.model_validate({**data, **change})


def test_receipt_is_closed_and_preserves_whole_integer_budget() -> None:
    data = dict(
        admission_id="11111111-1111-4111-8111-111111111111",
        hold_id="22222222-2222-4222-8222-222222222222",
        run_id="run",
        attempt_id="33333333-3333-4333-8333-333333333333",
        request_ref="request",
        attempt_binding_digest="a" * 64,
        planned_digest="b" * 64,
        final_request_digest="c" * 64,
        budget_credits="9223372036854",
        pricing_snapshot_digest="d" * 64,
        status="held",
        created_at="2026-10-22T00:00:00.000Z",
        updated_at="2026-10-22T00:00:00.000Z",
    )
    assert (
        AgentModelAdmissionResponse.model_validate({"data": data}).data.budget_credits
        == "9223372036854"
    )
    for change in [
        {"budget_credits": "9223372036855"},
        {"budget_credits": 1},
        {"budget_credits": "1.5"},
        {"budget_credits": "0"},
        {"created_at": "2026-02-30T00:00:00.000Z"},
        {"tenant_id": "injected"},
    ]:
        with pytest.raises(ValidationError):
            AgentModelAdmissionResponse.model_validate(
                {"data": {**deepcopy(data), **change}}
            )


def test_failure_codegen_preserves_bff_and_unknown_artifacts_still_fail(
    tmp_path: Path,
) -> None:
    import json
    import shutil
    from kokoro_agent.execution_proof_contract import OWNER_SOURCE_FILES
    from kokoro_agent.chat_contract_check import generate_failure_models
    from kokoro_agent.bff_admission_contract import validate_bff_admission_consumer

    for relative in (*OWNER_SOURCE_FILES, "contract/provenance.json"):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, target)
    path = tmp_path / "contract/provenance.json"
    original = json.loads(path.read_text())
    generate_failure_models(tmp_path, check=False)
    assert (
        json.loads(path.read_text())["generated_artifacts"][1]
        == original["generated_artifacts"][1]
    )
    generate_failure_models(tmp_path, check=True)
    validate_bff_admission_consumer(tmp_path)
    original["generated_artifacts"].append({"path": "unexpected.py"})
    path.write_text(json.dumps(original))
    with pytest.raises(ValueError):
        generate_failure_models(tmp_path, check=True)
    with pytest.raises(ValueError):
        validate_bff_admission_consumer(tmp_path)
