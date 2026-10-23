"""Source model labels are observations, never standalone billing attribution."""

from __future__ import annotations

import json
import gzip

import httpx
import pytest

from kokoro_agent.clients.system import ResolvedModel, ModelConnection
from kokoro_agent.model.usage_observation import (
    capture_model_usage,
    claim_usage_request,
    observe_usage_response,
)


def route() -> ResolvedModel:
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
            connection_type="direct",
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
):
    data: list[dict[str, object]] = [
        {
            "model": m,
            "choices": [],
            "usage": {"prompt_tokens": 17, "completion_tokens": 3, "total_tokens": 20},
        }
        for m in models
    ]
    if response_ids is not None:
        for document, response_id in zip(data, response_ids, strict=True):
            if response_id is not Ellipsis:
                document["id"] = response_id
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
    bound = route()

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
