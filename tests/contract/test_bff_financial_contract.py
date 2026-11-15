"""Fixed financial owner wire, including conditional ACKs and integer limits."""

from pathlib import Path
import importlib
from copy import deepcopy
import pytest
from pydantic import BaseModel, ValidationError
from kokoro_agent.bff_admission_contract import bff_admission_receipt

ROOT = Path(__file__).resolve().parents[2]
ID = "11111111-1111-4111-8111-111111111111"


def test_receipt_fixes_four_formal_operations_without_changing_existing_wire() -> None:
    receipt = bff_admission_receipt()
    assert receipt["version"] == "1.4.0"
    assert receipt["source_commit"] == "bfbaf3b24bfa7c339761facf97278f933b64e710"
    assert receipt["operations"] == [
        "POST /internal/v1/model-admissions",
        "POST /internal/v1/model-admissions/recover",
        "POST /internal/v1/model-admissions/{admission_id}/settle",
        "POST /internal/v1/model-admissions/close-unreserved",
    ]


def generated(name: str) -> type[BaseModel]:
    cls = getattr(
        importlib.import_module("kokoro_agent.clients.bff_admission_generated"),
        name,
        None,
    )
    assert isinstance(cls, type) and issubclass(cls, BaseModel), (
        "formal settlement model is absent"
    )
    return cls


def ack(status: str = "captured") -> dict[str, dict[str, object]]:
    return {
        "data": dict(
            admission_id=ID,
            attempt_id=ID,
            evidence_event_id=ID,
            run_id="😀" * 512,
            evidence_revision="9223372036854775807",
            evidence_digest="a" * 64,
            status=status,
            reason="budget_exceeded" if status == "pending_reconciliation" else None,
            captured_credits="1" if status == "captured" else "0",
            released_credits="1" if status == "released" else "0",
            usage_event_id=ID if status == "captured" else None,
            settlement_id=ID if status == "captured" else None,
            updated_at="2026-10-23T00:00:00.000Z",
        )
    }


@pytest.mark.parametrize("status", ["captured", "released", "pending_reconciliation"])
def test_each_original_conditional_ack_is_accepted(status: str) -> None:
    value = generated("AgentModelSettlementResponse").model_validate(ack(status))
    assert value.model_dump()["data"]["status"] == status


@pytest.mark.parametrize(
    "status,patch",
    [
        ("captured", {"reason": "budget_exceeded"}),
        ("captured", {"usage_event_id": None}),
        ("captured", {"settlement_id": None}),
        ("released", {"captured_credits": "1"}),
        ("released", {"usage_event_id": ID}),
        ("pending_reconciliation", {"reason": None}),
        ("pending_reconciliation", {"captured_credits": "1"}),
        ("pending_reconciliation", {"released_credits": "1"}),
        ("captured", {"captured_credits": "9223372036855"}),
        ("captured", {"evidence_revision": "9223372036854775808"}),
        ("captured", {"evidence_revision": "1\n"}),
        ("captured", {"evidence_revision": 1}),
        ("captured", {"run_id": "😀" * 513}),
        ("captured", {"admission_id": ID + "\n"}),
        ("captured", {"updated_at": "2026-02-30T00:00:00.000Z"}),
        ("captured", {"tenant_id": "injected"}),
    ],
)
def test_every_branch_retains_owner_constraints(
    status: str, patch: dict[str, object]
) -> None:
    raw = deepcopy(ack(status))
    data = raw["data"]
    assert isinstance(data, dict)
    data.update(patch)
    cls = generated("AgentModelSettlementResponse")
    with pytest.raises(ValidationError):
        cls.model_validate(raw)


def test_settlement_request_has_only_original_four_facts() -> None:
    cls = generated("AgentModelSettlementRequest")
    raw = dict(
        run_id="run",
        evidence_event_id=ID,
        evidence_revision="1",
        evidence_digest="a" * 64,
    )
    assert set(cls.model_validate(raw).model_dump()) == set(raw)
    for patch in [
        {"evidence_revision": "9223372036854775808"},
        {"amount_credits": "1"},
        {"tenant_id": "injected"},
    ]:
        with pytest.raises(ValidationError):
            cls.model_validate({**raw, **patch})


def test_generated_intersections_and_unions_match_independent_owner_schema_oracle() -> (
    None
):
    from jsonschema import Draft202012Validator, validate
    from jsonschema.exceptions import ValidationError as SchemaValidationError
    import yaml
    from pydantic import TypeAdapter

    document = yaml.safe_load(
        (ROOT / "contract/bff/internal-v1/openapi.yaml").read_bytes()
    )
    schemas = document["components"]["schemas"]
    schema = schemas["AgentModelSettlementResponse"]
    cls = generated("AgentModelSettlementResponse")
    patches: list[dict[str, object]] = [{}]
    for field in ("captured_credits", "released_credits"):
        patches.extend(
            {field: value}
            for value in [
                "0",
                "1",
                "9223372036854",
                "9223372036855",
                "9999999999999",
                "01",
                "-1",
                "1.1",
                1,
                None,
            ]
        )
    patches.extend(
        {"evidence_revision": value}
        for value in [
            "1",
            "9223372036854775807",
            "9223372036854775808",
            "9999999999999999999",
            "0",
            "1\n",
            1,
        ]
    )
    for field in ("status", "reason", "usage_event_id", "settlement_id"):
        patches.extend(
            {field: value}
            for value in [
                None,
                "captured",
                "released",
                "pending_reconciliation",
                "unknown_usage_or_outcome",
                "budget_exceeded",
                ID,
                "wrong",
            ]
        )
    for status in ["captured", "released", "pending_reconciliation"]:
        for patch in patches:
            raw = ack(status)
            raw["data"].update(patch)
            try:
                validate(raw, schema, cls=Draft202012Validator)
                expected = True
            except SchemaValidationError:
                expected = False
            try:
                cls.model_validate(raw)
                actual = True
            except ValidationError:
                actual = False
            assert actual is expected, (status, patch)
    # Scalar aliases used for path/expectation validation come from owner fields too.
    mod = importlib.import_module("kokoro_agent.clients.bff_admission_generated")
    scalar = TypeAdapter(mod.AgentModelAdmissionId)
    assert scalar.validate_python(ID, strict=True) == ID
    with pytest.raises(ValidationError):
        scalar.validate_python(ID + "/../recover", strict=True)


def test_unreserved_close_contract_is_independent_and_preserves_original_nine_bindings() -> (
    None
):
    request = generated("AgentModelUnreservedCloseRequest")
    response = generated("AgentModelUnreservedCloseResponse")
    raw = dict(
        run_id="run-1",
        attempt_id=ID,
        request_ref="agent-model-admit:" + ID,
        attempt_binding_digest="a" * 64,
        planned_digest="b" * 64,
        final_request_digest="c" * 64,
        evidence_event_id=ID,
        evidence_revision="1",
        evidence_digest="d" * 64,
    )
    assert request.model_validate(raw).model_dump() == raw
    result = raw | dict(
        closure_ref=ID, status="unreserved_closed", closed_at="2026-11-15T00:00:00.000Z"
    )
    assert response.model_validate({"data": result}).model_dump()["data"] == result
    for extra in (
        {"tenant_id": "injected"},
        {"captured_credits": "0"},
        {"proof": "injected"},
    ):
        with pytest.raises(ValidationError):
            request.model_validate(raw | extra)
        with pytest.raises(ValidationError):
            response.model_validate({"data": result | extra})
    with pytest.raises(ValidationError):
        response.model_validate({"data": result | {"status": "released"}})
    for patch in (
        {"closed_at": "2026-02-30T00:00:00.000Z"},
        {"closed_at": "2026-11-15T00:00:00.000Z\n"},
        {"evidence_event_id": "not-a-uuid"},
        {"evidence_revision": "9223372036854775808"},
    ):
        with pytest.raises(ValidationError):
            response.model_validate({"data": result | patch})
