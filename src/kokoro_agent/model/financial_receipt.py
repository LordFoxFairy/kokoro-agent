"""Canonical local delivery receipt, never a second price or balance authority."""

from __future__ import annotations

from dataclasses import asdict, fields
from datetime import datetime
import hashlib
import re
from uuid import UUID

from pydantic import TypeAdapter
import rfc8785
from kokoro_agent.model.settlement import ModelSettlementReceipt

_DOMAIN = b"kokoro-agent:model-financial-ack:1\x00"
_RECEIPT = TypeAdapter(ModelSettlementReceipt)
_OBJECT = TypeAdapter(dict[str, object])
_KEYS = {field.name for field in fields(ModelSettlementReceipt)}


class ModelFinancialConflict(RuntimeError):
    def __init__(self) -> None:
        super().__init__("model financial delivery facts conflict")


def encode_financial_receipt(value: ModelSettlementReceipt) -> tuple[bytes, str]:
    raw: bytes | None = None
    try:
        raw = rfc8785.dumps(asdict(value))
    except (ValueError, TypeError, OverflowError, RecursionError):
        pass
    if raw is None:
        raise ModelFinancialConflict()
    digest = hashlib.sha256(_DOMAIN + raw).hexdigest()
    decode_financial_receipt(raw, digest)
    return raw, digest


def decode_financial_receipt(raw: bytes, digest: str) -> ModelSettlementReceipt:
    result: ModelSettlementReceipt | None = None
    try:
        if (
            not 1 <= len(raw) <= 16384
            or hashlib.sha256(_DOMAIN + raw).hexdigest() != digest
        ):
            raise ValueError
        document = _OBJECT.validate_json(raw)
        if set(document) != _KEYS:
            raise ValueError
        value = _RECEIPT.validate_json(raw, strict=True)
        if rfc8785.dumps(asdict(value)) != raw:
            raise ValueError
        for ref in (
            value.admission_ref,
            value.attempt_id,
            value.event_id,
            value.usage_event_ref,
            value.settlement_ref,
        ):
            if ref is not None and str(UUID(ref)) != ref:
                raise ValueError
        if not 1 <= len(value.run_id) <= 512 or not re.fullmatch(
            r"[0-9a-f]{64}", value.digest
        ):
            raise ValueError
        if (
            not re.fullmatch(r"[1-9][0-9]{0,18}", value.revision)
            or int(value.revision) > (1 << 63) - 1
        ):
            raise ValueError
        for credits in (value.captured_credits, value.released_credits):
            if (
                not re.fullmatch(r"0|[1-9][0-9]{0,12}", credits)
                or int(credits) > ((1 << 63) - 1) // 1_000_000
            ):
                raise ValueError
        if not re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z",
            value.updated_at,
        ):
            raise ValueError
        datetime.fromisoformat(value.updated_at.replace("Z", "+00:00"))
        if value.status == "captured":
            valid = (
                value.reason is None
                and value.usage_event_ref is not None
                and value.settlement_ref is not None
            )
        elif value.status == "released":
            valid = (
                value.reason is None
                and value.captured_credits == "0"
                and value.usage_event_ref is None
                and value.settlement_ref is None
            )
        else:
            valid = (
                value.reason is not None
                and value.captured_credits == value.released_credits == "0"
                and value.usage_event_ref is None
                and value.settlement_ref is None
            )
        if not valid:
            raise ValueError
        result = value
    except (ValueError, TypeError, OverflowError, RecursionError):
        pass
    if result is None:
        raise ModelFinancialConflict()
    return result
