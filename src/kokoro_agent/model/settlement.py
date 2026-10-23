"""Financial notification facts; no pricing, wallet writes, or execution authority."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelSettlementCommand:
    run_id: str
    attempt_id: str
    event_id: str
    revision: str
    digest: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelSettlementReceipt:
    admission_ref: str
    run_id: str
    attempt_id: str
    event_id: str
    revision: str
    digest: str
    status: Literal["captured", "released", "pending_reconciliation"]
    reason: Literal["unknown_usage_or_outcome", "budget_exceeded"] | None
    captured_credits: str
    released_credits: str
    usage_event_ref: str | None
    settlement_ref: str | None
    updated_at: str
