"""Source model labels are observations, never standalone billing attribution."""

from __future__ import annotations

import json

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


def observe(models: list[object], *, streaming: bool):
    data: list[dict[str, object]] = [
        {
            "model": m,
            "choices": [],
            "usage": {"prompt_tokens": 17, "completion_tokens": 3, "total_tokens": 20},
        }
        for m in models
    ]
    raw = (
        b"".join(b"data: " + json.dumps(d).encode() + b"\n\n" for d in data)
        + b"data: [DONE]\n\n"
        if streaming
        else json.dumps(data[0]).encode()
    )
    bound = route()

    def claim(request: httpx.Request) -> None:
        claim_usage_request(request, bound)

    with httpx.Client(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={
                    "content-type": "text/event-stream"
                    if streaming
                    else "application/json"
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
