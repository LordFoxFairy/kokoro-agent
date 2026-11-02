"""Source model labels are observations, never standalone billing attribution."""

from __future__ import annotations

import json
import gzip
from typing import Literal

import httpx
import pytest

from kokoro_agent.clients.system import ResolvedModel, ModelConnection
from kokoro_agent.model.usage_observation import (
    capture_model_usage,
    claim_usage_request,
    observe_usage_response,
)
from kokoro_agent.model.usage_producer import normalize_observed_usage


def route(connection_type: Literal["direct", "litellm"] = "direct") -> ResolvedModel:
    return ResolvedModel(
        model_id="model-a",
        provider_id="provider-a",
        revision_id="11111111-1111-4111-8111-111111111111",
        revision=1,
        digest="a" * 64,
        generation="1",
        tenant_generation="1",
        provider_model_name="raw-a",
        request_model="request-alias",
        feature_key="chat",
        label_key="default",
        context_window=128,
        max_output_tokens=16,
        connection=ModelConnection(
            protocol="openai_chat_completions",
            connection_type=connection_type,
            base_url="https://provider.test/v1",
            secret_handle_ref="test-handle",
        ),
    )


def observe(
    models: list[object],
    *,
    streaming: bool,
    response_ids: list[object] | None = None,
    compressed: bool = False,
    complete: bool = True,
    truncate_encoding: bool = False,
    usage: object = Ellipsis,
    omit_usage: bool = False,
    choices: object = Ellipsis,
    event_object: object = "chat.completion.chunk",
    trailing_documents: list[dict[str, object]] | None = None,
    connection_type: Literal["direct", "litellm"] = "direct",
):
    data: list[dict[str, object]] = [
        {
            "model": m,
            "choices": [],
            "usage": {"prompt_tokens": 17, "completion_tokens": 3, "total_tokens": 20},
        }
        for m in models
    ]
    for document in data:
        if choices is not Ellipsis:
            document["object"] = event_object
            document["choices"] = choices
        if omit_usage:
            document.pop("usage")
        elif usage is not Ellipsis:
            document["usage"] = usage
    if response_ids is not None:
        for document, response_id in zip(data, response_ids, strict=True):
            if response_id is not Ellipsis:
                document["id"] = response_id
    data.extend(trailing_documents or [])
    raw = (
        b"".join(b"data: " + json.dumps(d).encode() + b"\n\n" for d in data)
        + b"data: [DONE]\n\n"
        if streaming
        else json.dumps(data[0]).encode()
    )
    if not complete:
        raw = raw.removesuffix(b"data: [DONE]\n\n") if streaming else raw[:-1]
    if compressed:
        raw = gzip.compress(raw)
    if truncate_encoding:
        raw = raw[:-4]
    bound = route(connection_type)

    def claim(request: httpx.Request) -> None:
        claim_usage_request(request, bound)

    with httpx.Client(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={
                    **({"content-encoding": "gzip"} if compressed else {}),
                    "content-type": "text/event-stream"
                    if streaming
                    else "application/json",
                },
                stream=httpx.ByteStream(raw),
            )
        ),
        event_hooks={
            "request": [claim],
            "response": [observe_usage_response],
        },
    ) as client:
        with capture_model_usage(bound) as capture:
            client.post(
                "https://provider.test/v1/chat/completions",
                json={"model": "request-alias"},
            )
            capture.mark_sdk_consumption_complete()
        return capture.snapshot()


def empty_delta_choices() -> list[dict[str, object]]:
    delta: dict[str, object] = {}
    return [{"index": 0, "delta": delta}]


@pytest.mark.parametrize("streaming", [False, True])
def test_litellm_explicit_redundant_text_preserves_reported_facts(
    streaming: bool,
) -> None:
    usage = {
        "prompt_tokens": 20,
        "completion_tokens": 3,
        "total_tokens": 23,
        "completion_tokens_details": {"reasoning_tokens": 0, "text_tokens": 3},
    }
    actual = observe(
        ["raw-a"], streaming=streaming, connection_type="litellm", usage=usage
    )
    reference = observe(
        ["raw-a"],
        streaming=streaming,
        connection_type="litellm",
        usage={**usage, "completion_tokens_details": {"reasoning_tokens": 0}},
    )
    normalized = normalize_observed_usage(actual)
    assert normalized.state == "normalized"
    assert normalized.value.profile_assessment.state == "supported"
    assert normalized.value.classification_state == "partial"
    assert normalized.value.output_total.model_dump() == {
        "state": "reported",
        "count": "3",
    }
    assert normalized.value.output_reasoning.model_dump() == {
        "state": "reported",
        "count": "0",
    }
    assert normalized.value.input_cache_read.state == "unreported"
    assert actual.projection_digest == reference.projection_digest


@pytest.mark.parametrize("text", [None, True, 3.0, "3", -1, 4])
def test_litellm_text_must_be_explicit_exact_integer(text: object) -> None:
    value = observe(
        ["raw-a"],
        streaming=True,
        connection_type="litellm",
        usage={
            "prompt_tokens": 17,
            "completion_tokens": 3,
            "total_tokens": 20,
            "completion_tokens_details": {"reasoning_tokens": 0, "text_tokens": text},
        },
    )
    normalized = normalize_observed_usage(value)
    assert normalized.state == "normalized"
    assert normalized.value.profile_assessment.state == "unsupported"


@pytest.mark.parametrize(
    "details",
    [
        {"text_tokens": 3},
        {"text_tokens": 3, "reasoning_tokens": None},
        {"text_tokens": 2, "reasoning_tokens": 1},
        {"text_tokens": 3, "reasoning_tokens": 0, "audio_tokens": 0},
        {"text_tokens": 3, "reasoning_tokens": 0, "cache_write_tokens": 0},
        {"text_tokens": 3, "reasoning_tokens": 0, "unknown": 0},
    ],
)
def test_litellm_text_does_not_erase_other_unknown_knowledge(details: object) -> None:
    value = observe(
        ["raw-a"],
        streaming=True,
        connection_type="litellm",
        usage={
            "prompt_tokens": 17,
            "completion_tokens": 3,
            "total_tokens": 20,
            "completion_tokens_details": details,
        },
    )
    normalized = normalize_observed_usage(value)
    assert normalized.state == "normalized"
    assert normalized.value.profile_assessment.state == "unsupported"


@pytest.mark.parametrize("completion", [None, Ellipsis])
def test_litellm_text_cannot_supply_missing_output_total(completion: object) -> None:
    usage: dict[str, object] = {
        "prompt_tokens": 17,
        "total_tokens": 20,
        "completion_tokens_details": {"reasoning_tokens": 0, "text_tokens": 3},
    }
    if completion is not Ellipsis:
        usage["completion_tokens"] = completion
    normalized = normalize_observed_usage(
        observe(
            ["raw-a"],
            streaming=True,
            connection_type="litellm",
            usage=usage,
        )
    )
    assert normalized.state == "normalized"
    assert normalized.value.output_total.state == "unreported"
    assert normalized.value.profile_assessment.state == "unsupported"


def test_direct_redundant_text_remains_unsupported() -> None:
    normalized = normalize_observed_usage(
        observe(
            ["raw-a"],
            streaming=True,
            usage={
                "prompt_tokens": 17,
                "completion_tokens": 3,
                "total_tokens": 20,
                "completion_tokens_details": {"reasoning_tokens": 0, "text_tokens": 3},
            },
        )
    )
    assert normalized.state == "normalized"
    assert normalized.value.profile_assessment.state == "unsupported"


@pytest.mark.parametrize("compressed", [False, True])
def test_litellm_empty_delta_tail_preserves_actual_counts_without_cache_facts(
    compressed: bool,
) -> None:
    # Actual local-qwen/LiteLLM SSE usage tail: not OpenAI's choices=[] frame.
    value = observe(
        ["raw-a"],
        streaming=True,
        connection_type="litellm",
        choices=empty_delta_choices(),
        compressed=compressed,
        usage={"prompt_tokens": 153, "completion_tokens": 21, "total_tokens": 174},
    )
    normalized = normalize_observed_usage(value)
    assert normalized.state == "normalized"
    assert normalized.value.input_total.model_dump() == {
        "state": "reported",
        "count": "153",
    }
    assert normalized.value.output_total.model_dump() == {
        "state": "reported",
        "count": "21",
    }
    assert normalized.value.total.model_dump() == {"state": "reported", "count": "174"}
    assert normalized.value.input_cache_read.state == "unreported"
    assert normalized.value.output_reasoning.state == "unreported"
    assert normalized.value.classification_state == "partial"
    assert value.projection_digest is not None


@pytest.mark.parametrize(
    "choices",
    [
        [{"index": 0, "delta": {"content": "token"}}],
        [{"index": 0, "delta": {"tool_calls": []}}],
        [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        [{"index": 0, "delta": {}, "logprobs": None}],
        [{"index": True, "delta": {}}],
        [{"index": 1, "delta": {}}],
        [{"index": 0, "delta": {}}, {"index": 1, "delta": {}}],
    ],
)
def test_litellm_semantic_or_ambiguous_choice_is_not_final_usage(
    choices: object,
) -> None:
    value = observe(
        ["raw-a"], streaming=True, connection_type="litellm", choices=choices
    )
    normalized = normalize_observed_usage(value)
    assert normalized.state == "unresolved" and normalized.reason == "missing_usage"


def test_direct_empty_delta_usage_remains_unresolved() -> None:
    value = observe(["raw-a"], streaming=True, choices=empty_delta_choices())
    assert normalize_observed_usage(value).state == "unresolved"


@pytest.mark.parametrize("event_object", [None, "chat.completion", 1])
def test_litellm_empty_choice_in_wrong_event_is_not_usage(event_object: object) -> None:
    value = observe(
        ["raw-a"],
        streaming=True,
        connection_type="litellm",
        choices=empty_delta_choices(),
        event_object=event_object,
    )
    assert normalize_observed_usage(value).state == "unresolved"


def test_litellm_identical_tail_is_deduplicated_with_unchanged_projection() -> None:
    choices = empty_delta_choices()
    choices[0]["finish_reason"] = None
    repeated = observe(
        ["raw-a", "raw-a"],
        streaming=True,
        connection_type="litellm",
        choices=choices,
    )
    standard = observe(["raw-a"], streaming=True)
    assert repeated.status == "complete"
    assert repeated.counts["total_tokens"].value == 20
    assert repeated.projection_digest == standard.projection_digest


def test_litellm_usage_tail_followed_by_content_is_not_final() -> None:
    value = observe(
        ["raw-a"],
        streaming=True,
        connection_type="litellm",
        choices=empty_delta_choices(),
        trailing_documents=[
            {
                "model": "raw-a",
                "choices": [{"index": 0, "delta": {"content": "late"}}],
                "usage": None,
            }
        ],
    )
    assert value.status == "unresolved"


def test_litellm_usage_tail_requires_done() -> None:
    value = observe(
        ["raw-a"],
        streaming=True,
        connection_type="litellm",
        choices=empty_delta_choices(),
        complete=False,
    )
    assert value.status == "unresolved" and value.reason == "missing_done"


def test_litellm_usage_tail_conflict_is_not_summed() -> None:
    value = observe(
        ["raw-a"],
        streaming=True,
        connection_type="litellm",
        choices=empty_delta_choices(),
        trailing_documents=[
            {
                "model": "raw-a",
                "object": "chat.completion.chunk",
                "choices": [{"index": 0, "delta": {}}],
                "usage": {
                    "prompt_tokens": 17,
                    "completion_tokens": 4,
                    "total_tokens": 21,
                },
            }
        ],
    )
    assert value.status == "unresolved" and value.reason == "conflicting_usage"


@pytest.mark.parametrize("streaming", [False, True])
def test_exact_reported_model_survives_same_consumed_response(streaming: bool) -> None:
    value = observe(["raw-a"], streaming=streaming)
    assert (
        value.status == "complete" and getattr(value, "reported_model", None) == "raw-a"
    )


def test_conflicting_stream_labels_do_not_destroy_known_usage_or_create_verified_label() -> (
    None
):
    value = observe(["raw-a", "raw-other"], streaming=True)
    assert value.status == "complete" and value.counts["total_tokens"].value == 20
    assert getattr(value, "reported_model", None) is None


@pytest.mark.parametrize("name", [None, 12, "", "x" * 513, "bad\nname"])
def test_invalid_label_does_not_gain_an_identity(name: object) -> None:
    value = observe([name], streaming=False)
    assert value.status == "complete" and getattr(value, "reported_model", None) is None


def test_existing_usage_projection_digest_does_not_change_with_model_label() -> None:
    assert (
        observe(["raw-a"], streaming=False).projection_digest
        == observe(["raw-other"], streaming=False).projection_digest
    )


@pytest.mark.parametrize("streaming", [False, True])
def test_provider_response_id_survives_consumed_bytes(streaming: bool) -> None:
    value = observe(["raw-a"], streaming=streaming, response_ids=["gen-original"])
    assert getattr(value, "provider_response_id", None) == "gen-original"
    assert "gen-original" not in repr(value)


def test_repeated_stream_response_id_is_one_lookup_reference() -> None:
    value = observe(
        ["raw-a", "raw-a"],
        streaming=True,
        response_ids=["gen-original", "gen-original"],
    )
    assert getattr(value, "provider_response_id", None) == "gen-original"


@pytest.mark.parametrize(
    "response_id", [None, 12, "", "x" * 257, "bad\nname", "a b", "编号"]
)
def test_invalid_response_id_does_not_destroy_known_usage(response_id: object) -> None:
    value = observe(["raw-a"], streaming=False, response_ids=[response_id])
    assert value.status == "complete" and value.counts["total_tokens"].value == 20
    assert getattr(value, "provider_response_id", None) is None


@pytest.mark.parametrize("second", ["gen-other", None, "bad\nname"])
def test_conflicting_stream_response_ids_are_not_a_lookup_reference(
    second: object,
) -> None:
    value = observe(
        ["raw-a", "raw-a"], streaming=True, response_ids=["gen-original", second]
    )
    assert (
        value.status == "complete"
        and getattr(value, "provider_response_id", None) is None
    )


def test_response_locator_does_not_change_original_usage_digest() -> None:
    assert (
        observe(["raw-a"], streaming=False, response_ids=["gen-a"]).projection_digest
        == observe(["raw-a"], streaming=False, response_ids=["gen-b"]).projection_digest
    )


@pytest.mark.parametrize("streaming", [False, True])
def test_response_id_survives_validated_compression_only(streaming: bool) -> None:
    value = observe(
        ["raw-a"], streaming=streaming, response_ids=["gen-compressed"], compressed=True
    )
    assert value.provider_response_id == "gen-compressed"
    incomplete = observe(
        ["raw-a"], streaming=streaming, response_ids=["gen-incomplete"], complete=False
    )
    assert incomplete.status == "unresolved" and incomplete.provider_response_id is None


def test_stream_lookup_id_may_be_reported_before_final_usage_frame() -> None:
    value = observe(
        ["raw-a", "raw-a"], streaming=True, response_ids=["gen-first-frame", Ellipsis]
    )
    assert value.provider_response_id == "gen-first-frame"


@pytest.mark.parametrize("streaming", [False, True])
def test_truncated_gzip_response_does_not_leave_a_lookup_id(streaming: bool) -> None:
    value = observe(
        ["raw-a"],
        streaming=streaming,
        response_ids=["gen-truncated"],
        compressed=True,
        truncate_encoding=True,
    )
    assert value.status == "unresolved" and value.provider_response_id is None


@pytest.mark.parametrize("streaming", [False, True])
def test_documented_cost_metadata_does_not_invalidate_token_usage(
    streaming: bool,
) -> None:
    base: dict[str, object] = {
        "prompt_tokens": 17,
        "completion_tokens": 3,
        "total_tokens": 20,
        "prompt_tokens_details": {"cached_tokens": 2},
    }
    expected = observe(["raw-a"], streaming=streaming, usage=base)
    value = observe(
        ["raw-a"],
        streaming=streaming,
        usage={**base, "cost": 0.95, "cost_details": {"upstream_inference_cost": 19}},
    )
    normalized = normalize_observed_usage(value)
    assert normalized.state == "normalized"
    assert normalized.value.profile_assessment.state == "supported"
    assert normalized.value.input_cache_read.model_dump() == {
        "state": "reported",
        "count": "2",
    }
    assert value.projection_digest == expected.projection_digest


@pytest.mark.parametrize(
    "extra",
    [
        {"cost": None, "cost_details": None},
        {"cost": 0, "cost_details": {"upstream_inference_cost": None}},
    ],
)
def test_nullable_cost_metadata_does_not_invent_cache_counts(
    extra: dict[str, object],
) -> None:
    value = observe(
        ["raw-a"],
        streaming=False,
        usage={
            "prompt_tokens": 17,
            "completion_tokens": 3,
            "total_tokens": 20,
            **extra,
        },
    )
    normalized = normalize_observed_usage(value)
    assert normalized.state == "normalized"
    assert normalized.value.profile_assessment.state == "supported"
    assert normalized.value.input_cache_read.state == "unreported"


@pytest.mark.parametrize(
    "extra",
    [
        {"cost": True},
        {"cost": -1},
        {"cost": "0.95"},
        {"cost_details": []},
        {"cost_details": {"upstream_inference_cost": -1}},
        {"cost_details": {"upstream_inference_cost": True}},
        {"cost_details": {"unrecognized_fee": 1}},
        {"unrecognized_tokens": 1},
        {"prompt_tokens_details": {"cache_write_tokens": 1}},
        {"prompt_tokens_details": {"audio_tokens": 1}},
    ],
)
def test_cost_metadata_does_not_hide_unsupported_usage(
    extra: dict[str, object],
) -> None:
    value = observe(
        ["raw-a"],
        streaming=True,
        usage={
            "prompt_tokens": 17,
            "completion_tokens": 3,
            "total_tokens": 20,
            "cost": 0.95,
            **extra,
        },
    )
    normalized = normalize_observed_usage(value)
    assert normalized.state == "normalized"
    assert normalized.value.profile_assessment.state == "unsupported"


@pytest.mark.parametrize("compressed", [False, True])
@pytest.mark.parametrize("omit_usage", [False, True])
def test_complete_stream_without_usage_preserves_lookup_but_not_usage(
    compressed: bool,
    omit_usage: bool,
) -> None:
    value = observe(
        ["raw-a"],
        streaming=True,
        response_ids=["gen-missing-usage"],
        compressed=compressed,
        usage=None,
        omit_usage=omit_usage,
    )
    assert value.status == "complete" and value.compression_integrity == "validated"
    assert (
        value.provider_response_id == "gen-missing-usage"
        and value.reported_model == "raw-a"
    )
    assert value.projection_digest is None
    normalized = normalize_observed_usage(value)
    assert normalized.state == "unresolved" and normalized.reason == "missing_usage"
    assert all(count.value is None for count in value.counts.values())


@pytest.mark.parametrize(
    "response_ids,models,expected_id,expected_model",
    [
        (["gen-a", "gen-b"], ["raw-a", "raw-a"], None, "raw-a"),
        (["gen-a", None], ["raw-a", "raw-a"], None, "raw-a"),
        (["gen-a", "gen-a"], ["raw-a", "raw-b"], "gen-a", None),
    ],
)
def test_missing_usage_does_not_allow_conflicting_response_identity(
    response_ids: list[object],
    models: list[object],
    expected_id: str | None,
    expected_model: str | None,
) -> None:
    value = observe(models, streaming=True, response_ids=response_ids, omit_usage=True)
    assert value.status == "complete"
    assert (
        value.provider_response_id == expected_id
        and value.reported_model == expected_model
    )
    assert normalize_observed_usage(value).state == "unresolved"


@pytest.mark.parametrize("complete,truncate", [(False, False), (True, True)])
def test_incomplete_stream_without_usage_never_preserves_lookup(
    complete: bool, truncate: bool
) -> None:
    value = observe(
        ["raw-a"],
        streaming=True,
        response_ids=["gen-incomplete"],
        omit_usage=True,
        complete=complete,
        compressed=True,
        truncate_encoding=truncate,
    )
    assert value.status == "unresolved"
    assert value.provider_response_id is None and value.reported_model is None


async def test_production_hook_rejects_changed_credential_before_any_prepare_or_send() -> (
    None
):
    from pydantic import SecretStr
    from kokoro_agent.model.credentials import ModelCredential
    from kokoro_agent.model.factory import ModelHttpClients, ChatModelSettings
    from kokoro_agent.model.call_models import ModelCallInputInvalid

    settings = ChatModelSettings(
        disable_streaming=False,
        openai_api_key=None,
        openai_base_url=None,
        openai_reasoning=False,
        anthropic_api_key=None,
        anthropic_base_url=None,
    )
    r = route()
    clients = ModelHttpClients(
        settings,
        r,
        ModelCredential(
            provider_id=r.provider_id,
            secret_handle_ref=r.connection.secret_handle_ref,
            api_key=SecretStr("original-test-key"),
        ),
    )
    try:
        with pytest.raises(ModelCallInputInvalid, match="credential binding"):
            await clients.async_client.post(
                str(clients.endpoint),
                headers={
                    "authorization": "Bearer changed-test-key",
                    "accept": "application/json",
                },
                json={
                    "model": r.request_model,
                    "n": 1,
                    "max_completion_tokens": 1,
                    "messages": [],
                },
            )
    finally:
        await clients.aclose()
