"""Durable ACK validation never becomes a second financial authority."""

from dataclasses import asdict, replace
import hashlib

import pytest
import rfc8785

from kokoro_agent.model.financial_receipt import (
    ModelFinancialConflict,
    decode_financial_receipt,
    encode_financial_receipt,
)
from kokoro_agent.model.settlement import ModelSettlementReceipt


def receipt() -> ModelSettlementReceipt:
    return ModelSettlementReceipt(
        admission_ref="11111111-1111-4111-8111-111111111111",
        run_id="run-1",
        attempt_id="22222222-2222-4222-8222-222222222222",
        event_id="33333333-3333-4333-8333-333333333333",
        revision="1",
        digest="a" * 64,
        status="released",
        reason=None,
        captured_credits="0",
        released_credits="10",
        usage_event_ref=None,
        settlement_ref=None,
        updated_at="2026-10-23T00:00:00.000Z",
    )


def decode(raw: bytes) -> ModelSettlementReceipt:
    # Independent domain literal protects the persisted byte/digest contract.
    digest = hashlib.sha256(b"kokoro-agent:model-financial-ack:1\x00" + raw).hexdigest()
    return decode_financial_receipt(raw, digest)


@pytest.mark.parametrize("status", ["captured", "released", "pending_reconciliation"])
def test_full_conditional_ack_round_trip(status: str) -> None:
    value = receipt()
    if status == "captured":
        value = replace(
            value,
            status="captured",
            captured_credits="3",
            released_credits="7",
            usage_event_ref="44444444-4444-4444-8444-444444444444",
            settlement_ref="55555555-5555-4555-8555-555555555555",
        )
    elif status == "pending_reconciliation":
        value = replace(
            value,
            status="pending_reconciliation",
            reason="budget_exceeded",
            released_credits="0",
        )
    raw, digest = encode_financial_receipt(value)
    assert decode(raw) == value == decode_financial_receipt(raw, digest)


@pytest.mark.parametrize(
    "changes",
    [
        {"run_id": ""},
        {"run_id": "a" * 513},
        {"admission_ref": "not-a-uuid"},
        {"event_id": "33333333333343338333333333333333"},
        {"revision": "01"},
        {"revision": "0"},
        {"revision": "9223372036854775808"},
        {"digest": "A" * 64},
        {"released_credits": "0.1"},
        {"released_credits": "01"},
        {"released_credits": "-1"},
        {"released_credits": "9223372036855"},
        {"updated_at": "2026-02-30T00:00:00.000Z"},
        {"updated_at": "2026-10-23T00:00:00Z"},
        {"captured_credits": "1"},
        {"reason": "unknown_usage_or_outcome"},
        {"status": "captured"},
        {"status": "pending_reconciliation", "released_credits": "0"},
    ],
)
def test_invalid_full_ack_is_a_fixed_safe_conflict(changes: dict[str, str]) -> None:
    raw = rfc8785.dumps(asdict(receipt()) | changes)
    with pytest.raises(
        ModelFinancialConflict, match="^model financial delivery facts conflict$"
    ):
        decode(raw)


def test_non_utf8_internal_ack_is_a_safe_conflict_before_storage() -> None:
    with pytest.raises(
        ModelFinancialConflict, match="^model financial delivery facts conflict$"
    ):
        encode_financial_receipt(replace(receipt(), run_id="\ud800"))


@pytest.mark.parametrize(
    "damage", ["duplicate", "unknown", "whitespace", "digest", "bound"]
)
def test_storage_json_and_digest_are_strict_not_merely_parseable(damage: str) -> None:
    raw, _ = encode_financial_receipt(receipt())
    if damage == "duplicate":
        raw = raw[:-1] + b',"status":"released"}'
    elif damage == "unknown":
        raw = rfc8785.dumps(asdict(receipt()) | {"internal": "not-owned"})
    elif damage == "whitespace":
        raw += b"\n"
    elif damage == "bound":
        raw = b" " * 16385
    with pytest.raises(ModelFinancialConflict):
        if damage == "digest":
            decode_financial_receipt(raw, "0" * 64)
        else:
            decode(raw)


def test_maximum_integer_credit_is_preserved_without_float_or_rounding() -> None:
    value = replace(
        receipt(), released_credits="9223372036854", revision="9223372036854775807"
    )
    raw, _ = encode_financial_receipt(value)
    assert decode(raw) == value
