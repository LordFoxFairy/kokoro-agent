"""Pure knowledge-preserving projection of SDK-consumed source observations."""

from __future__ import annotations

from typing import Literal
import hashlib
import rfc8785
from kokoro_agent.clients.system import ResolvedModel
from kokoro_agent.model.call_models import PreparedModelCall
from kokoro_agent.protocol.model_usage_v2_generated import (
    ActualAttribution,
    UnknownAttribution,
    VerifiedAttribution,
    PlannedBinding,
)

from kokoro_agent.model.usage_observation import PhysicalUsageObservation
from kokoro_agent.protocol.model_usage_v2_generated import (
    CountObservation,
    ReportedCount,
    UnreportedCount,
    TokenUsageV1,
    NormalizedUsage,
    UnresolvedUsage,
    UsageObservation,
    SupportedProfile,
    UnsupportedProfile,
    ProfileAssessment,
)


def normalize_observed_usage(observation: PhysicalUsageObservation) -> UsageObservation:
    if (
        observation.status != "complete"
        or observation.compression_integrity != "validated"
    ):
        return UnresolvedUsage(
            state="unresolved",
            reason="incomplete_response",
            raw_usage_digest=observation.projection_digest,
        )
    if observation.usage_presence != "object":
        return UnresolvedUsage(
            state="unresolved",
            reason="missing_usage",
            raw_usage_digest=observation.projection_digest,
        )
    try:
        return _normalized(observation)
    except (ValueError, TypeError, KeyError):
        pass
    return UnresolvedUsage(
        state="unresolved",
        reason="invalid_usage",
        raw_usage_digest=observation.projection_digest,
    )


def _normalized(observation: PhysicalUsageObservation) -> NormalizedUsage:
    def count(name: str) -> CountObservation:
        value = observation.counts[name]
        if value.presence == "reported":
            if type(value.value) is not int or not 0 <= value.value <= (1 << 63) - 1:
                raise ValueError
            return ReportedCount(state="reported", count=str(value.value))
        if value.value is not None:
            raise ValueError
        return UnreportedCount(state="unreported")

    input_total, output_total, total = (
        count(name) for name in ("prompt_tokens", "completion_tokens", "total_tokens")
    )
    cached, reasoning = count("cached_tokens"), count("reasoning_tokens")
    totals_state: Literal["known_zero", "known_nonzero", "unknown"] = "unknown"
    if (
        isinstance(input_total, ReportedCount)
        and isinstance(output_total, ReportedCount)
        and isinstance(total, ReportedCount)
    ):
        if int(input_total.count) + int(output_total.count) != int(total.count):
            raise ValueError
        totals_state = "known_zero" if total.count == "0" else "known_nonzero"
    for subset, inclusive in ((cached, input_total), (reasoning, output_total)):
        if (
            isinstance(subset, ReportedCount)
            and isinstance(inclusive, ReportedCount)
            and int(subset.count) > int(inclusive.count)
        ):
            raise ValueError
    assessment: ProfileAssessment = SupportedProfile(state="supported")
    if observation.unsupported_usage_category:
        assessment = UnsupportedProfile(
            state="unsupported", reason="unsupported_usage_category"
        )
    elif observation.service_tier == "unsupported":
        assessment = UnsupportedProfile(
            state="unsupported", reason="unsupported_service_tier"
        )
    return NormalizedUsage(
        state="normalized",
        raw_usage_digest=observation.projection_digest,
        value=TokenUsageV1(
            profile_id="token_usage_v1",
            unit="token",
            source_protocol="openai_chat_completions",
            aggregation_mode="final_cumulative",
            input_total=input_total,
            output_total=output_total,
            total=total,
            input_cache_read=cached,
            input_cache_write=UnreportedCount(state="unreported"),
            output_reasoning=reasoning,
            totals_state=totals_state,
            classification_state="unknown"
            if assessment.state == "unsupported" or totals_state == "unknown"
            else "partial",
            profile_assessment=assessment,
        ),
    )


def actual_from_response(
    *,
    route: ResolvedModel,
    prepared: PreparedModelCall,
    observation: PhysicalUsageObservation,
) -> ActualAttribution:
    """Map the authenticated route's same-response declaration, never a raw-name fallback."""
    planned = PlannedBinding.model_validate_json(prepared.planned_bytes, strict=True)
    matches = (
        planned.provider,
        planned.model,
        planned.revision_id,
        planned.digest,
        planned.provider_model_name,
        planned.request_model,
        planned.feature_key,
        planned.revision,
        planned.generation,
        planned.tenant_generation,
    ) == (
        route.provider_id,
        route.model_id,
        route.revision_id,
        route.digest,
        route.provider_model_name,
        route.request_model,
        route.feature_key,
        str(route.revision),
        route.generation,
        route.tenant_generation,
    )
    if (
        not matches
        or observation.status != "complete"
        or observation.compression_integrity != "validated"
        or observation.reported_model != route.provider_model_name
    ):
        return UnknownAttribution(state="unknown", reason="unverified_binding")
    document = {
        "version": 1,
        "attempt_id": prepared.attempt_id,
        "attempt_binding_digest": prepared.attempt_binding_digest,
        "final_request_digest": prepared.final_request_digest,
        "planned_digest": prepared.planned_digest,
        "revision_id": route.revision_id,
        "revision_digest": route.digest,
        "provider_ref": route.provider_id,
        "model_ref": route.model_id,
        "reported_model": observation.reported_model,
    }
    return VerifiedAttribution(
        state="verified",
        provider_ref=route.provider_id,
        model_ref=route.model_id,
        evidence_ref="authenticated-route-response-v1:" + prepared.attempt_id,
        evidence_digest=hashlib.sha256(
            b"kokoro-agent:authenticated-route-response:v1\n" + rfc8785.dumps(document)
        ).hexdigest(),
    )
