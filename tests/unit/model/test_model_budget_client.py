"""The BFF budget wire is single-send, exact, bounded, and contains no user token."""

from __future__ import annotations
import asyncio
import json
import gzip
from dataclasses import asdict, replace
from typing import Any
from collections.abc import Coroutine, Callable

import httpx
import pytest
from pydantic import SecretStr

from kokoro_agent.clients.model_budget import BffModelBudgetClient, ModelBudgetError
from kokoro_agent.model.budget import ModelBudgetCommand

COMMAND = ModelBudgetCommand(
    run_id="run-1",
    attempt_id="11111111-1111-4111-8111-111111111111",
    request_ref="request-1",
    attempt_binding_digest="a" * 64,
    planned_digest="b" * 64,
    final_request_digest="c" * 64,
)


def receipt() -> dict[str, Any]:
    return {
        "data": {
            **asdict(COMMAND),
            "admission_id": "22222222-2222-4222-8222-222222222222",
            "hold_id": "33333333-3333-4333-8333-333333333333",
            "budget_credits": "10",
            "pricing_snapshot_digest": "d" * 64,
            "status": "held",
            "created_at": "2026-10-22T00:00:00.000Z",
            "updated_at": "2026-10-22T00:00:00.000Z",
        }
    }


def response(payload: object, status: int = 201) -> httpx.Response:
    return httpx.Response(
        status,
        json=payload,
        headers={"cache-control": "no-store", "x-request-id": "test-budget"},
    )


def client(
    handler: Callable[[httpx.Request], httpx.Response]
    | Callable[[httpx.Request], Coroutine[None, None, httpx.Response]],
    timeout: float = 1,
) -> BffModelBudgetClient:
    return BffModelBudgetClient(
        "http://bff.test",
        lambda tenant: SecretStr("s" * 32),
        timeout_s=timeout,
        transport=httpx.MockTransport(handler),
    )


async def reserve(c: BffModelBudgetClient, command: ModelBudgetCommand = COMMAND):
    return await c.reserve(
        tenant_id="tenant-1",
        command=command,
        proof=SecretStr("p" * 16384),
        key="original-key",
        request_id="test-budget",
    )


async def test_exact_closed_command_and_original_admission_ref() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.path == "/internal/v1/model-admissions"
        assert json.loads(request.content) == asdict(COMMAND)
        assert request.headers["authorization"] == "Bearer " + "s" * 32
        assert request.headers["x-billing-execution-proof"] == "p" * 16384
        assert request.headers["idempotency-key"] == "original-key"
        assert "cookie" not in request.headers
        assert "x-billing-consumption-token" not in request.headers
        assert "x-kokoro-tenant-id" not in request.headers
        return response(receipt())

    async with client(handler) as c:
        grant = await reserve(c)
    assert grant.admission_ref == receipt()["data"]["admission_id"]
    assert len(calls) == 1


@pytest.mark.parametrize("field", list(asdict(COMMAND)))
async def test_every_binding_mismatch_is_rejected(field: str) -> None:
    body = receipt()
    body["data"][field] = (
        "44444444-4444-4444-8444-444444444444" if field == "attempt_id" else "e" * 64
    )
    async with client(lambda _: response(body)) as c:
        with pytest.raises(ModelBudgetError, match="MODEL_BUDGET_RESPONSE_INVALID"):
            await reserve(c)


@pytest.mark.parametrize(
    "patch",
    [
        {"status": "captured"},
        {"updated_at": "2026-10-22T00:00:00.001Z"},
        {"tenant_id": "injected"},
        {"budget_credits": "0"},
    ],
)
async def test_invalid_or_not_original_held_receipt_is_rejected(
    patch: dict[str, object],
) -> None:
    body = receipt()
    body["data"].update(patch)
    async with client(lambda _: response(body)) as c:
        with pytest.raises(ModelBudgetError, match="MODEL_BUDGET_RESPONSE_INVALID"):
            await reserve(c)


@pytest.mark.parametrize(
    "kind",
    [
        "duplicate",
        "bom",
        "depth",
        "mime",
        "cache",
        "request-id",
        "encoding",
        "redirect",
        "length",
    ],
)
async def test_raw_metadata_and_closed_json_fail_without_retry(kind: str) -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        headers = {
            "content-type": "application/json",
            "cache-control": "no-store",
            "x-request-id": "test-budget",
        }
        raw = json.dumps(receipt()).encode()
        status = 201
        if kind == "duplicate":
            raw = (
                b'{"data":'
                + json.dumps(receipt()["data"]).encode()
                + b',"data":'
                + json.dumps(receipt()["data"]).encode()
                + b"}"
            )
        if kind == "bom":
            raw = b"\xef\xbb\xbf" + raw
        if kind == "depth":
            raw = b"[" * 13 + b"0" + b"]" * 13
        if kind == "mime":
            headers["content-type"] = "text/html"
        if kind == "cache":
            headers["cache-control"] = "private"
        if kind == "request-id":
            headers["x-request-id"] = "other"
        if kind == "encoding":
            headers["content-encoding"] = "gzip"
            raw = gzip.compress(raw)
        if kind == "redirect":
            status = 307
            headers["location"] = "https://foreign.test"
        if kind == "length":
            headers["content-length"] = "32769"
        return httpx.Response(status, content=raw, headers=headers)

    async with client(handler) as c:
        with pytest.raises(ModelBudgetError, match="MODEL_BUDGET_RESPONSE_INVALID"):
            await reserve(c)
    assert calls == 1


async def test_unknown_network_result_is_not_retried_or_reported_as_denial() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadError("TOKEN_SENTINEL", request=request)

    async with client(handler) as c:
        with pytest.raises(ModelBudgetError, match="MODEL_BUDGET_UNAVAILABLE") as error:
            await reserve(c)
    assert calls == 1
    assert error.value.__context__ is None


async def test_timeout_and_cancellation_await_no_second_send() -> None:
    entered = asyncio.Event()
    calls = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        entered.set()
        await asyncio.Event().wait()
        return response(receipt())

    async with client(handler, timeout=0.02) as c:
        with pytest.raises(ModelBudgetError, match="MODEL_BUDGET_UNAVAILABLE"):
            await reserve(c)
    entered.clear()
    async with client(handler) as c:
        task = asyncio.create_task(reserve(c))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert calls == 2


async def test_legal_escaped_unicode_pair_and_oversized_body() -> None:
    changed = replace(COMMAND, run_id="😀" * 512, request_ref="\x01" * 512)

    def handler(request: httpx.Request) -> httpx.Response:
        body = receipt()
        body["data"].update(asdict(changed))
        assert json.loads(request.content) == asdict(changed)
        return response(body)

    async with client(handler) as c:
        assert (await reserve(c, changed)).admission_ref


@pytest.mark.parametrize(
    "origin", ["http://bff.test:TOKEN_SENTINEL", "http://[broken/TOKEN_SENTINEL"]
)
def test_malformed_origin_configuration_never_leaks_private_parts(origin: str) -> None:
    with pytest.raises(ValueError, match="invalid BFF budget configuration") as error:
        BffModelBudgetClient(origin, lambda _: SecretStr("s" * 32))
    assert "TOKEN_SENTINEL" not in str(error.value)


class StalledBody(httpx.AsyncByteStream):
    def __init__(self):
        self.closed = False

    async def __aiter__(self):
        yield json.dumps(receipt()).encode()
        await asyncio.Event().wait()

    async def aclose(self):
        self.closed = True


async def test_complete_json_without_eof_is_not_a_valid_ack() -> None:
    body = StalledBody()
    async with client(
        lambda _: httpx.Response(
            201,
            stream=body,
            headers={
                "content-type": "application/json",
                "cache-control": "no-store",
                "x-request-id": "test-budget",
            },
        ),
        timeout=0.02,
    ) as c:
        with pytest.raises(ModelBudgetError, match="MODEL_BUDGET_UNAVAILABLE"):
            await reserve(c)
    assert body.closed


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (400, "invalid_request"),
        (403, "forbidden"),
        (404, "not_found"),
        (409, "conflict"),
        (422, "insufficient_credit"),
        (422, "price_unavailable"),
        (422, "ineligible"),
        (502, "upstream_invalid"),
        (503, "unavailable"),
    ],
)
async def test_exact_safe_owner_errors_do_not_expose_message(
    status: int, code: str
) -> None:
    async with client(
        lambda _: response(
            {
                "error": {
                    "code": "model_admission_" + code,
                    "message": "TOKEN_SENTINEL",
                    "retryable": status == 503,
                }
            },
            status,
        )
    ) as c:
        with pytest.raises(
            ModelBudgetError, match="MODEL_BUDGET_" + code.upper()
        ) as error:
            await reserve(c)
    assert "TOKEN_SENTINEL" not in str(error.value)


class StalledClose(StalledBody):
    async def aclose(self):
        self.closed = True
        await asyncio.Event().wait()


@pytest.mark.parametrize("cancel", [False, True])
async def test_body_and_close_stalls_have_bounded_deadline_and_cancellation(
    cancel: bool,
) -> None:
    body = StalledClose()
    entered = asyncio.Event()
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        entered.set()
        return httpx.Response(
            201,
            stream=body,
            headers={
                "content-type": "application/json",
                "cache-control": "no-store",
                "x-request-id": "test-budget",
            },
        )

    async with client(handler, timeout=1 if cancel else 0.02) as c:
        task = asyncio.create_task(reserve(c))
        await entered.wait()
        if cancel:
            task.cancel()
        done, _ = await asyncio.wait({task}, timeout=0.5)
        if not done:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert done, "budget body/close exceeded bounded cleanup"
        with pytest.raises(asyncio.CancelledError if cancel else ModelBudgetError):
            await task
    assert calls == 1 and body.closed
