# GENERATED — DO NOT EDIT. Source: Root contract protobuf descriptor
# Regenerate: uv run python scripts/sync_interaction_v2_contract.py --contract-root <path>
from __future__ import annotations

from datetime import datetime
from enum import IntEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class InteractionDecisionKindV2(IntEnum):
    INTERACTION_DECISION_KIND_V2_UNSPECIFIED = 0
    INTERACTION_DECISION_KIND_V2_APPROVE = 1
    INTERACTION_DECISION_KIND_V2_EDIT = 2
    INTERACTION_DECISION_KIND_V2_REJECT = 3
    INTERACTION_DECISION_KIND_V2_RESPOND = 4
    INTERACTION_DECISION_KIND_V2_SUBMIT = 5

class DecisionDataClassificationV2(IntEnum):
    DECISION_DATA_CLASSIFICATION_V2_UNSPECIFIED = 0
    DECISION_DATA_CLASSIFICATION_V2_INTERNAL = 1
    DECISION_DATA_CLASSIFICATION_V2_CONFIDENTIAL = 2
    DECISION_DATA_CLASSIFICATION_V2_RESTRICTED = 3

class RunResumeReceiptStatusV2(IntEnum):
    RUN_RESUME_RECEIPT_STATUS_V2_UNSPECIFIED = 0
    RUN_RESUME_RECEIPT_STATUS_V2_PERSISTED = 1
    RUN_RESUME_RECEIPT_STATUS_V2_APPLYING = 2
    RUN_RESUME_RECEIPT_STATUS_V2_APPLIED = 3
    RUN_RESUME_RECEIPT_STATUS_V2_SUPERSEDED = 4
    RUN_RESUME_RECEIPT_STATUS_V2_REJECTED = 5
    RUN_RESUME_RECEIPT_STATUS_V2_OUTCOME_UNKNOWN = 6
    RUN_RESUME_RECEIPT_STATUS_V2_CLOSED_BY_TERMINAL = 7

class EncryptedDecisionValueV2(StrictModel):
    encryption_key_handle: str = Field(min_length=1, max_length=256)
    ciphertext: bytes = Field(min_length=1, max_length=65536)
    ciphertext_sha256: str = Field(pattern='^[0-9a-f]{64}$')
    nonce: bytes = Field(min_length=12, max_length=32)
    associated_data_sha256: str = Field(pattern='^[0-9a-f]{64}$')
    classification: DecisionDataClassificationV2

class ApproveDecisionV2(StrictModel):
    pass

class EditDecisionV2(StrictModel):
    value: EncryptedDecisionValueV2

class RejectDecisionV2(StrictModel):
    safe_reason: str | None = None

class RespondDecisionV2(StrictModel):
    value: EncryptedDecisionValueV2

class SubmitDecisionV2(StrictModel):
    value: EncryptedDecisionValueV2

class InteractionDecisionPayloadV2(StrictModel):
    approve: ApproveDecisionV2 | None = None
    edit: EditDecisionV2 | None = None
    reject: RejectDecisionV2 | None = None
    respond: RespondDecisionV2 | None = None
    submit: SubmitDecisionV2 | None = None

    @model_validator(mode="after")
    def _validate_decision(self) -> Self:
        fields = ('approve', 'edit', 'reject', 'respond', 'submit',)
        if sum(getattr(self, name) is not None for name in fields) != 1:
            raise ValueError("exactly one decision arm is required")
        return self

class RunResumeDecisionV2(StrictModel):
    interaction_owner_ref: str = Field(min_length=1, max_length=256)
    owner_revision: int = Field(gt=0)
    projection_event_ref: str = Field(min_length=1, max_length=256)
    application_request_ref: str = Field(min_length=1, max_length=256)
    group_member_ordinal: int = Field(gt=0, le=64)
    decision_receipt_ref: str = Field(min_length=1, max_length=256)
    decision_payload_sha256: str = Field(pattern='^[0-9a-f]{64}$')
    kind: InteractionDecisionKindV2
    decision: InteractionDecisionPayloadV2

class RunResumePayloadV2(StrictModel):
    run_id: str = Field(min_length=1, max_length=128)
    resume_ref: str = Field(min_length=1, max_length=256)
    pending_frame_digest: str = Field(pattern='^[0-9a-f]{64}$')
    decision_group_ref: str = Field(min_length=1, max_length=256)
    decision_group_revision: int = Field(gt=0)
    decisions: list[RunResumeDecisionV2] = Field(default_factory=list[RunResumeDecisionV2], min_length=1, max_length=64)

class RunResumeV2(StrictModel):
    payload: RunResumePayloadV2
    request_digest: str = Field(pattern='^[0-9a-f]{64}$')
    interaction_protocol_release_epoch: str = Field(min_length=1, max_length=256)

class RunResumeReceiptEventV2(StrictModel):
    run_id: str = Field(min_length=1, max_length=128)
    resume_ref: str = Field(min_length=1, max_length=256)
    resume_receipt_ref: str = Field(min_length=1, max_length=256)
    resume_receipt_event_ref: str = Field(min_length=1, max_length=256)
    resume_receipt_revision: int = Field(gt=0)
    predecessor_receipt_event_ref: str | None = None
    predecessor_receipt_event_sha256: str | None = None
    request_digest: str = Field(pattern='^[0-9a-f]{64}$')
    receipt_event_sha256: str = Field(pattern='^[0-9a-f]{64}$')
    status: RunResumeReceiptStatusV2
    disposition_proof_ref: str | None = None
    disposition_proof_sha256: str | None = None
    application_proof_ref: str | None = None
    application_proof_sha256: str | None = None
    applied_checkpoint_ref: str | None = None
    terminal_evidence_ref: str | None = None
    safe_code: str | None = None
    recorded_at: datetime
    producer_instance_ref: str = Field(min_length=1, max_length=256)
    producer_generation: int = Field(gt=0)
    interaction_protocol_release_epoch: str = Field(min_length=1, max_length=256)

class GetRunResumeReceiptEventsRequest(StrictModel):
    run_id: str = Field(min_length=1, max_length=128)
    resume_ref: str = Field(min_length=1, max_length=256)
    after_receipt_revision: int = 0
    page_size: int = Field(gt=0, le=64)

class GetRunResumeReceiptEventsResponse(StrictModel):
    resume_receipt_ref: str = Field(min_length=1, max_length=256)
    current_head_revision: int = Field(gt=0)
    current_head_event_ref: str = Field(min_length=1, max_length=256)
    current_head_event_sha256: str = Field(pattern='^[0-9a-f]{64}$')
    events: list[RunResumeReceiptEventV2] = Field(default_factory=list[RunResumeReceiptEventV2], max_length=64)
    next_after_receipt_revision: int | None = None
    has_more: bool = False
    returned_after_receipt_revision: int = 0
    run_id: str = Field(min_length=1, max_length=128)
    resume_ref: str = Field(min_length=1, max_length=256)

for _model in (EncryptedDecisionValueV2, ApproveDecisionV2, EditDecisionV2, RejectDecisionV2, RespondDecisionV2, SubmitDecisionV2, InteractionDecisionPayloadV2, RunResumeDecisionV2, RunResumePayloadV2, RunResumeV2, RunResumeReceiptEventV2, GetRunResumeReceiptEventsRequest, GetRunResumeReceiptEventsResponse,):
    _model.model_rebuild()

del _model
