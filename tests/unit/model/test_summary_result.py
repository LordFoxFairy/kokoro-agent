"""Bounded canonical native-summary capsules, never a generic model cache."""

from __future__ import annotations

import hashlib
import json

import pytest
import rfc8785

from kokoro_agent.model.summary_result import (
    MAX_SUMMARY_RESULT_BYTES,
    SummaryCallBinding,
    SummaryMessage,
    SummaryResult,
    SummaryUsage,
    decode_summary_result,
    encode_summary_result,
    summary_prompt_digest,
)


def binding() -> SummaryCallBinding:
    return SummaryCallBinding(
        tenant_ref="tenant",
        run_id="run",
        call_id="11111111-1111-5111-8111-111111111111",
        attempt_id="22222222-2222-5222-8222-222222222222",
        peer_path=("general",),
        checkpoint_id="checkpoint",
        checkpoint_ns="",
        task_id="task",
        recipe_digest="a" * 64,
        planned_digest="b" * 64,
        prompt_digest="c" * 64,
    )


def capsule(text: str = " original summary \n") -> SummaryResult:
    return SummaryResult(
        binding=binding(),
        attempt_binding_digest="d" * 64,
        final_request_digest="e" * 64,
        event_id="33333333-3333-5333-8333-333333333333",
        event_digest="f" * 64,
        usage=SummaryUsage(input_tokens="19", output_tokens="5", total_tokens="24"),
        message=SummaryMessage(content=text),
    )


def test_capsule_round_trip_preserves_unstripped_text_and_exact_digest() -> None:
    value = capsule()
    raw, digest = encode_summary_result(value)
    assert digest == hashlib.sha256(raw).hexdigest()
    assert decode_summary_result(raw, digest) == value
    assert decode_summary_result(raw, digest).message.content == " original summary \n"
    assert "原请求".encode() not in raw


@pytest.mark.parametrize(
    "mutation", ["digest", "bytes", "duplicate", "role", "extra", "version"]
)
def test_capsule_corruption_never_becomes_cache_miss(mutation: str) -> None:
    raw, digest = encode_summary_result(capsule())
    if mutation == "digest":
        digest = "0" * 64
    elif mutation == "bytes":
        raw += b" "
        digest = hashlib.sha256(raw).hexdigest()
    elif mutation == "duplicate":
        raw = raw.replace(b'"version":1', b'"version":1,"version":1')
        digest = hashlib.sha256(raw).hexdigest()
    else:
        value = json.loads(raw)
        if mutation == "role":
            value["message"]["role"] = "system"
        elif mutation == "extra":
            value["raw_prompt"] = "must never persist"
        else:
            value["version"] = 2
        raw = rfc8785.dumps(value)
        digest = hashlib.sha256(raw).hexdigest()
    with pytest.raises(ValueError, match="summary result"):
        decode_summary_result(raw, digest)


def test_capsule_limit_applies_to_complete_canonical_utf8_envelope() -> None:
    baseline, _ = encode_summary_result(capsule(""))
    value = capsule("x" * (MAX_SUMMARY_RESULT_BYTES - len(baseline)))
    raw, digest = encode_summary_result(value)
    assert len(raw) == MAX_SUMMARY_RESULT_BYTES
    assert decode_summary_result(raw, digest) == value
    with pytest.raises(ValueError, match="summary result"):
        encode_summary_result(capsule(value.message.content + "x"))
    with pytest.raises(ValueError, match="summary result"):
        encode_summary_result(capsule("界" * (MAX_SUMMARY_RESULT_BYTES // 3)))


def test_prompt_digest_binds_prompt_stop_effective_parameters_and_profile() -> None:
    kwargs = {"model": "routed", "max_completion_tokens": 128, "n": 1}
    digest = summary_prompt_digest("private prompt", None, kwargs, "profile")
    assert digest == summary_prompt_digest("private prompt", None, kwargs, "profile")
    assert len(digest) == 64
    assert digest != summary_prompt_digest("changed", None, kwargs, "profile")
    assert digest != summary_prompt_digest(
        "private prompt", ["stop"], kwargs, "profile"
    )
    assert digest != summary_prompt_digest(
        "private prompt", None, {**kwargs, "n": 2}, "profile"
    )
    assert digest != summary_prompt_digest(
        "private prompt", None, kwargs, "other-profile"
    )
