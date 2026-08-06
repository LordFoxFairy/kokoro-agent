"""Root R1 Presentation submission envelope built from the existing planner fact."""

from __future__ import annotations

import hashlib
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic.alias_generators import to_camel

from kokoro_agent.presentation.candidate import (
    AgentAguiEventCandidate,
    canonical_json_bytes,
    event_digest,
)
from kokoro_agent.presentation.profile import ClosedAguiEvent


class _SubmissionModel(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        alias_generator=to_camel,
        populate_by_name=True,
        revalidate_instances="always",
    )


class SubmissionRoute(_SubmissionModel):
    internal_run_ref: str = Field(min_length=1, max_length=128)
    internal_thread_ref: str = Field(
        min_length=14,
        max_length=128,
        pattern=r"^agent\.thread:[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    internal_message_ref: str | None = Field(default=None, min_length=1, max_length=128)


class SubmissionSource(_SubmissionModel):
    source_event_ref: str = Field(min_length=1, max_length=128)
    event_ordinal: str = Field(pattern=r"^(0|[1-9][0-9]{0,19})$")
    recorded_at: str = Field(
        pattern=(
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"\.[0-9]{3}Z$"
        )
    )
    route: SubmissionRoute


def submission_identity(
    *,
    contract_revision: str,
    source: SubmissionSource,
    event_digest_value: str,
) -> str:
    route = source.route
    material = "\0".join(
        (
            contract_revision,
            route.internal_run_ref,
            route.internal_thread_ref,
            route.internal_message_ref or "",
            source.source_event_ref,
            source.event_ordinal,
            source.recorded_at,
            event_digest_value,
        )
    ).encode()
    return f"presentation.submission:sha256:{hashlib.sha256(material).hexdigest()}"


class PresentationSubmission(_SubmissionModel):
    contract_revision: Annotated[
        str, Field(pattern=r"^kokoro\.presentation\.submission\.v1$")
    ]
    submission_ref: str = Field(
        pattern=r"^presentation\.submission:sha256:[0-9a-f]{64}$"
    )
    source: SubmissionSource
    event_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    event: ClosedAguiEvent

    @model_validator(mode="after")
    def validate_identity(self) -> PresentationSubmission:
        if event_digest(self.event) != self.event_digest:
            raise ValueError("PRESENTATION_SUBMISSION_EVENT_DIGEST_INVALID")
        expected = submission_identity(
            contract_revision=self.contract_revision,
            source=self.source,
            event_digest_value=self.event_digest,
        )
        if self.submission_ref != expected:
            raise ValueError("PRESENTATION_SUBMISSION_REF_INVALID")
        return self

    @classmethod
    def from_candidate(
        cls, candidate: AgentAguiEventCandidate
    ) -> PresentationSubmission:
        source = SubmissionSource(
            source_event_ref=candidate.source.source_event_ref,
            event_ordinal=candidate.source.source_ordinal,
            recorded_at=candidate.source.recorded_at,
            route=SubmissionRoute(
                internal_run_ref=candidate.source.route.internal_run_ref,
                internal_thread_ref=candidate.source.route.internal_thread_ref,
                **(
                    {}
                    if candidate.source.route.internal_message_ref is None
                    else {
                        "internal_message_ref": (
                            candidate.source.route.internal_message_ref
                        )
                    }
                ),
            ),
        )
        contract_revision = "kokoro.presentation.submission.v1"
        return cls(
            contract_revision=contract_revision,
            submission_ref=submission_identity(
                contract_revision=contract_revision,
                source=source,
                event_digest_value=candidate.event_digest,
            ),
            source=source,
            event_digest=candidate.event_digest,
            event=candidate.event,
        )

    def envelope_bytes(self) -> bytes:
        return canonical_json_bytes(
            self.model_dump(mode="json", by_alias=True, exclude_none=True)
        )


__all__ = [
    "PresentationSubmission",
    "SubmissionRoute",
    "SubmissionSource",
    "submission_identity",
]
