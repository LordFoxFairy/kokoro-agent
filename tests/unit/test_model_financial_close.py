"""No-reservation receipts never become settlement, usage or permission."""

from dataclasses import replace
from collections.abc import Mapping

import pytest

from kokoro_agent.model.financial_close import (
    ModelFinancialCloseReceipt,
    ModelFinancialCloseCommand,
)
from kokoro_agent.model.financial_receipt import ModelFinancialConflict


def receipt() -> ModelFinancialCloseReceipt:
    return ModelFinancialCloseReceipt(
        run_id="run-1",
        attempt_id="11111111-1111-4111-8111-111111111111",
        request_ref="agent-model-admit:11111111-1111-4111-8111-111111111111",
        attempt_binding_digest="a" * 64,
        planned_digest="b" * 64,
        final_request_digest="c" * 64,
        evidence_event_id="22222222-2222-4222-8222-222222222222",
        evidence_revision="1",
        evidence_digest="d" * 64,
        closure_ref="33333333-3333-4333-8333-333333333333",
        status="unreserved_closed",
        closed_at="2026-11-15T00:00:00.000Z",
    )


def test_close_receipt_is_canonical_bounded_and_domain_separated() -> None:
    from kokoro_agent.model.financial_close import (
        encode_close_receipt,
        decode_close_receipt,
    )

    value = receipt()
    raw, digest = encode_close_receipt(value)
    assert decode_close_receipt(raw, digest) == value
    with pytest.raises(ModelFinancialConflict):
        decode_close_receipt(raw, "0" * 64)
    with pytest.raises(ModelFinancialConflict):
        decode_close_receipt(raw + b" ", digest)
    with pytest.raises(ModelFinancialConflict):
        encode_close_receipt(replace(value, evidence_revision="01"))
    with pytest.raises(ModelFinancialConflict):
        encode_close_receipt(replace(value, closed_at="2026-99-15T00:00:00.000Z"))


def test_operator_cli_requires_one_exact_target_and_explicit_expected_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent import cli

    captured: list[object] = []

    async def close(
        environment: Mapping[str, str], command: ModelFinancialCloseCommand
    ) -> ModelFinancialCloseReceipt:
        captured.append((environment, command))
        return receipt()

    monkeypatch.setattr(cli, "close_unreserved", close)
    env = {"KOKORO_AGENT_DATABASE_URL": "postgresql://fixture/unused"}
    args = [
        "model:close-unreserved",
        "--tenant-id",
        "tenant-1",
        "--run-id",
        "run-1",
        "--attempt-id",
        receipt().attempt_id,
        "--evidence-event-id",
        receipt().evidence_event_id,
        "--evidence-revision",
        "1",
        "--evidence-digest",
        receipt().evidence_digest,
    ]
    assert cli.main(args, environment=env) == 0
    assert len(captured) == 1
    with pytest.raises(SystemExit):
        cli.main(["model:close-unreserved", "--tenant-id", "tenant-1"], environment=env)


@pytest.mark.parametrize(
    "field,value",
    [
        ("attempt_id", "not-a-uuid"),
        ("evidence_revision", "01"),
        ("evidence_digest", "0"),
        ("tenant_id", " "),
    ],
)
def test_close_command_rejects_invalid_boundaries_before_database(
    field: str, value: str
) -> None:
    from dataclasses import asdict

    command = ModelFinancialCloseCommand(
        tenant_id="tenant-1",
        run_id="run-1",
        attempt_id=receipt().attempt_id,
        evidence_event_id=receipt().evidence_event_id,
        evidence_revision="1",
        evidence_digest=receipt().evidence_digest,
    )
    values = asdict(command)
    values[field] = value
    with pytest.raises(ModelFinancialConflict):
        ModelFinancialCloseCommand(**values)
