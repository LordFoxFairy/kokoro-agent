"""Explicit no-reservation closure facts, separate from usage and settlement ACKs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import datetime
import hashlib
import re
from typing import Literal
from uuid import UUID

from pydantic import TypeAdapter
import rfc8785

from kokoro_agent.model.budget import ModelBudgetCommand
from kokoro_agent.model.financial_reconciliation import ModelFinancialWork
from kokoro_agent.model.financial_receipt import ModelFinancialConflict
from kokoro_agent.model.settlement import ModelSettlementCommand


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelFinancialCloseCommand:
    tenant_id: str
    run_id: str
    attempt_id: str
    evidence_event_id: str
    evidence_revision: str
    evidence_digest: str

    def __post_init__(self) -> None:
        try:
            if (
                not re.fullmatch(r"[^\s\x00-\x1f\x7f]{1,191}", self.tenant_id)
                or not 1 <= len(self.run_id) <= 512
                or str(UUID(self.attempt_id)) != self.attempt_id
                or str(UUID(self.evidence_event_id)) != self.evidence_event_id
                or not re.fullmatch(r"[1-9][0-9]{0,18}", self.evidence_revision)
                or int(self.evidence_revision) > (1 << 63) - 1
                or not re.fullmatch(r"[0-9a-f]{64}", self.evidence_digest)
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise ModelFinancialConflict() from None


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelFinancialCloseReceipt:
    run_id: str
    attempt_id: str
    request_ref: str
    attempt_binding_digest: str
    planned_digest: str
    final_request_digest: str
    evidence_event_id: str
    evidence_revision: str
    evidence_digest: str
    closure_ref: str
    status: Literal["unreserved_closed"]
    closed_at: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelFinancialCloseWork:
    command: ModelFinancialCloseCommand
    financial: ModelFinancialWork
    actor_ref: str
    receipt: ModelFinancialCloseReceipt | None = None

    @property
    def budget(self) -> ModelBudgetCommand:
        return self.financial.budget

    @property
    def evidence(self) -> ModelSettlementCommand:
        return self.financial.evidence


_DOMAIN = b"kokoro-agent:model-financial-close:1\x00"
_ADAPTER = TypeAdapter(ModelFinancialCloseReceipt)
_KEYS = {field.name for field in fields(ModelFinancialCloseReceipt)}


def encode_close_receipt(value: ModelFinancialCloseReceipt) -> tuple[bytes, str]:
    try:
        raw = rfc8785.dumps(asdict(value))
        digest = hashlib.sha256(_DOMAIN + raw).hexdigest()
        decode_close_receipt(raw, digest)
        return raw, digest
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ModelFinancialConflict() from None


def decode_close_receipt(raw: bytes, digest: str) -> ModelFinancialCloseReceipt:
    try:
        if (
            not 1 <= len(raw) <= 16384
            or hashlib.sha256(_DOMAIN + raw).hexdigest() != digest
        ):
            raise ValueError
        value = _ADAPTER.validate_json(raw, strict=True)
        document = asdict(value)
        if set(document) != _KEYS or rfc8785.dumps(document) != raw:
            raise ValueError
        for ref in (value.attempt_id, value.evidence_event_id, value.closure_ref):
            if str(UUID(ref)) != ref:
                raise ValueError
        for sha in (
            value.attempt_binding_digest,
            value.planned_digest,
            value.final_request_digest,
            value.evidence_digest,
        ):
            if not re.fullmatch(r"[0-9a-f]{64}", sha):
                raise ValueError
        if (
            not 1 <= len(value.run_id) <= 512
            or not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", value.request_ref)
            or not re.fullmatch(r"[1-9][0-9]{0,18}", value.evidence_revision)
            or int(value.evidence_revision) > (1 << 63) - 1
            or not re.fullmatch(
                r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z",
                value.closed_at,
            )
        ):
            raise ValueError
        datetime.fromisoformat(value.closed_at.replace("Z", "+00:00"))
        return value
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ModelFinancialConflict() from None


def validate_close_binding(
    work: ModelFinancialCloseWork, receipt: ModelFinancialCloseReceipt
) -> None:
    encode_close_receipt(receipt)
    expected = asdict(work.budget) | {
        "evidence_event_id": work.evidence.event_id,
        "evidence_revision": work.evidence.revision,
        "evidence_digest": work.evidence.digest,
    }
    if any(getattr(receipt, key) != value for key, value in expected.items()):
        raise ModelFinancialConflict()
