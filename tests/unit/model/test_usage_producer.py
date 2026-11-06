"""Usage producer preserves source knowledge instead of inventing free counts."""

from __future__ import annotations

import importlib
from types import MappingProxyType

import pytest

from kokoro_agent.model.usage_observation import PhysicalUsageObservation, ObservedCount


@pytest.fixture
def producer():
    return importlib.import_module("kokoro_agent.model.usage_producer")


def observation(**changes: object) -> PhysicalUsageObservation:
    from dataclasses import replace

    value = PhysicalUsageObservation(
        status="complete",
        reason=None,
        compression_integrity="validated",
        usage_presence="object",
        counts=MappingProxyType(
            {
                "prompt_tokens": ObservedCount("reported", 17),
                "completion_tokens": ObservedCount("reported", 3),
                "total_tokens": ObservedCount("reported", 20),
                "cached_tokens": ObservedCount("missing"),
                "reasoning_tokens": ObservedCount("null"),
            }
        ),
        projection_digest="a" * 64,
    )
    return replace(value, **changes)


def test_source_totals_are_normalized_without_inventing_details(
    producer: object,
) -> None:
    value = getattr(producer, "normalize_observed_usage")(observation())
    assert value.state == "normalized" and value.value.input_total.count == "17"
    assert (
        value.value.total.count == "20"
        and value.value.classification_state == "partial"
    )
    assert (
        value.value.input_cache_read.state
        == value.value.input_cache_write.state
        == value.value.output_reasoning.state
        == "unreported"
    )
    assert value.raw_usage_digest == "a" * 64


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "unresolved", "reason": "scope_interrupted"},
        {"usage_presence": "missing"},
        {"compression_integrity": "unverified_unconsumed_tail"},
    ],
)
def test_incomplete_source_stays_unresolved_not_zero(
    producer: object, changes: dict[str, object]
) -> None:
    value = getattr(producer, "normalize_observed_usage")(observation(**changes))
    assert value.state == "unresolved"


def test_only_explicit_reported_zero_is_known_zero(producer: object) -> None:
    counts = MappingProxyType(
        {
            k: ObservedCount("reported", 0)
            if k in {"prompt_tokens", "completion_tokens", "total_tokens"}
            else ObservedCount("missing")
            for k in observation().counts
        }
    )
    value = getattr(producer, "normalize_observed_usage")(observation(counts=counts))
    assert value.state == "normalized" and value.value.totals_state == "known_zero"


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"unsupported_usage_category": True}, "unsupported_usage_category"),
        ({"service_tier": "unsupported"}, "unsupported_service_tier"),
    ],
)
def test_unsupported_semantics_are_explicit_not_billable_known_usage(
    producer: object, changes: dict[str, object], reason: str
) -> None:
    value = getattr(producer, "normalize_observed_usage")(observation(**changes))
    assert value.state == "normalized" and value.value.classification_state == "unknown"
    assert (
        value.value.profile_assessment.state == "unsupported"
        and value.value.profile_assessment.reason == reason
    )
