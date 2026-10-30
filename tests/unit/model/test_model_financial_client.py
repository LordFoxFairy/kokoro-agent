"""Recovery and settlement are explicit single sends, never new execution authority."""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict
from typing import Any

import httpx
import pytest
from pydantic import SecretStr
from kokoro_agent.clients.model_budget import BffModelBudgetClient, ModelBudgetError
from kokoro_agent.model.budget import ModelBudgetCommand, ModelBudgetGrant
from kokoro_agent.model.settlement import ModelSettlementCommand, ModelSettlementReceipt

ID = "11111111-1111-4111-8111-111111111111"
ADMISSION = "22222222-2222-4222-8222-222222222222"
COMMAND = ModelBudgetCommand(
    run_id="run-1",
    attempt_id=ID,
    request_ref="original-request",
    attempt_binding_digest="a" * 64,
    planned_digest="b" * 64,
    final_request_digest="c" * 64,
)


def headers() -> dict[str, str]:
    return {
        "content-type": "application/json",
        "cache-control": "no-store",
        "x-request-id": "finance-test",
    }


def grant() -> dict[str, Any]:
    return {
        "data": {
            **asdict(COMMAND),
            "admission_id": ADMISSION,
            "hold_id": ID,
            "budget_credits": "10",
            "pricing_snapshot_digest": "d" * 64,
            "status": "held",
            "created_at": "2026-10-23T00:00:00.000Z",
            "updated_at": "2026-10-23T00:00:00.000Z",
        }
    }


def ack(status: str = "captured") -> dict[str, Any]:
    return {
        "data": dict(
            admission_id=ADMISSION,
            run_id=COMMAND.run_id,
            attempt_id=ID,
            evidence_event_id=ID,
            evidence_revision="1",
            evidence_digest="e" * 64,
            status=status,
            reason="budget_exceeded" if status == "pending_reconciliation" else None,
            captured_credits="1" if status == "captured" else "0",
            released_credits="1" if status == "released" else "0",
            usage_event_id=ID if status == "captured" else None,
            settlement_id=ID if status == "captured" else None,
            updated_at="2026-10-23T00:00:00.000Z",
        )
    }


def error(
    code: str = "model_admission_not_found", retryable: bool = False
) -> dict[str, object]:
    return {"error": dict(code=code, message="PRIVATE_SENTINEL", retryable=retryable)}


async def recover(c: BffModelBudgetClient) -> ModelBudgetGrant | None:
    return await c.recover(
        tenant_id="tenant-1",
        command=COMMAND,
        key="original-key",
        request_id="finance-test",
    )


async def settle(
    c: BffModelBudgetClient, admission: str = ADMISSION
) -> ModelSettlementReceipt:
    return await c.settle(
        tenant_id="tenant-1",
        admission_ref=admission,
        command=ModelSettlementCommand(
            run_id=COMMAND.run_id,
            attempt_id=ID,
            event_id=ID,
            revision="1",
            digest="e" * 64,
        ),
        key="event-original-key",
        request_id="finance-test",
    )


@pytest.mark.parametrize("operation", ["recover", "settle"])
async def test_financial_requests_are_exact_without_proof_or_user_token(
    operation: str,
) -> None:
    calls: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        body = json.loads(req.content)
        assert req.headers["authorization"] == "Bearer " + "s" * 32
        assert not any(
            h in req.headers
            for h in [
                "cookie",
                "x-billing-execution-proof",
                "x-billing-consumption-token",
            ]
        )
        if operation == "recover":
            assert req.url.path == "/internal/v1/model-admissions/recover"
            assert body == asdict(COMMAND)
            assert req.headers["idempotency-key"] == "original-key"
            data = grant()
        else:
            assert req.url.path == f"/internal/v1/model-admissions/{ADMISSION}/settle"
            assert body == dict(
                run_id=COMMAND.run_id,
                evidence_event_id=ID,
                evidence_revision="1",
                evidence_digest="e" * 64,
            )
            assert req.headers["idempotency-key"] == "event-original-key"
            data = ack()
        return httpx.Response(200, json=data, headers=headers())

    async with BffModelBudgetClient(
        "http://bff.test",
        lambda _: SecretStr("s" * 32),
        transport=httpx.MockTransport(handler),
    ) as c:
        result = await (recover(c) if operation == "recover" else settle(c))
        assert result is not None and result.admission_ref == ADMISSION
    assert len(calls) == 1


@pytest.mark.parametrize(
    "code,retryable,returns_miss",
    [
        ("model_admission_not_found", False, True),
        ("model_admission_not_found", True, False),
        ("model_admission_forbidden", False, False),
        ("model_settlement_not_found", False, False),
    ],
)
async def test_only_exact_recover_404_is_a_miss_without_fallback(
    code: str, retryable: bool, returns_miss: bool
) -> None:
    calls: list[httpx.Request] = []

    def handler(req: httpx.Request):
        calls.append(req)
        return httpx.Response(404, json=error(code, retryable), headers=headers())

    async with BffModelBudgetClient(
        "http://bff.test",
        lambda _: SecretStr("s" * 32),
        transport=httpx.MockTransport(handler),
    ) as c:
        if returns_miss:
            assert await recover(c) is None
        else:
            with pytest.raises(ModelBudgetError):
                await recover(c)
    assert len(calls) == 1 and calls[0].url.path.endswith("/recover")


@pytest.mark.parametrize("status", ["captured", "released", "pending_reconciliation"])
async def test_owner_three_states_are_preserved_not_converted_to_capture(
    status: str,
) -> None:
    async with BffModelBudgetClient(
        "http://bff.test",
        lambda _: SecretStr("s" * 32),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json=ack(status), headers=headers())
        ),
    ) as c:
        result = await settle(c)
        assert result.status == status and result.captured_credits == (
            "1" if status == "captured" else "0"
        )


@pytest.mark.parametrize(
    "field",
    [
        "admission_id",
        "run_id",
        "attempt_id",
        "evidence_event_id",
        "evidence_revision",
        "evidence_digest",
    ],
)
async def test_every_settlement_binding_mismatch_is_rejected(field: str) -> None:
    body = ack()
    body["data"][field] = (
        "33333333-3333-4333-8333-333333333333"
        if field.endswith("id")
        else "2"
        if field.endswith("revision")
        else "f" * 64
    )
    async with BffModelBudgetClient(
        "http://bff.test",
        lambda _: SecretStr("s" * 32),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json=body, headers=headers())
        ),
    ) as c:
        with pytest.raises(ModelBudgetError):
            await settle(c)


@pytest.mark.parametrize("operation", ["recover", "settle"])
@pytest.mark.parametrize(
    "fault",
    [
        "duplicate",
        "mime",
        "cache",
        "request-id",
        "cookie",
        "encoding",
        "redirect",
        "limit",
        "status",
        "unknown",
    ],
)
async def test_shared_transport_rejects_invalid_or_unknown_without_second_send(
    operation: str, fault: str
) -> None:
    calls: list[httpx.Request] = []

    def handler(req: httpx.Request):
        calls.append(req)
        if fault == "unknown":
            raise httpx.ReadError("PRIVATE_SENTINEL", request=req)
        h = headers()
        body = grant() if operation == "recover" else ack()
        raw = json.dumps(body).encode()
        status = 200
        if fault == "duplicate":
            raw = b'{"data":{},"data":{}}'
        if fault == "mime":
            h["content-type"] = "text/html"
        if fault == "cache":
            h["cache-control"] = "private"
        if fault == "request-id":
            h["x-request-id"] = "other"
        if fault == "cookie":
            h["set-cookie"] = "private=value"
        if fault == "encoding":
            h["content-encoding"] = "gzip"
        if fault == "redirect":
            status = 307
            h["location"] = "https://other.test/"
        if fault == "limit":
            h["content-length"] = "32769"
        if fault == "status":
            status = 201
        return httpx.Response(status, content=raw, headers=h)

    async with BffModelBudgetClient(
        "http://bff.test",
        lambda _: SecretStr("s" * 32),
        transport=httpx.MockTransport(handler),
    ) as c:
        with pytest.raises(ModelBudgetError) as caught:
            await (recover(c) if operation == "recover" else settle(c))
        assert caught.value.__context__ is None and "PRIVATE_SENTINEL" not in str(
            caught.value
        )
    assert len(calls) == 1


@pytest.mark.parametrize("operation", ["recover", "settle"])
async def test_caller_cancel_propagates_without_retry(operation: str) -> None:
    entered = asyncio.Event()
    calls: list[httpx.Request] = []

    async def handler(req: httpx.Request):
        calls.append(req)
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async with BffModelBudgetClient(
        "http://bff.test",
        lambda _: SecretStr("s" * 32),
        transport=httpx.MockTransport(handler),
    ) as c:
        task = asyncio.create_task(recover(c) if operation == "recover" else settle(c))
        # A missing method must fail instead of leaving this test waiting forever.
        waiter = asyncio.create_task(entered.wait())
        try:
            done, _ = await asyncio.wait(
                [task, waiter], timeout=1, return_when=asyncio.FIRST_COMPLETED
            )
            if task in done:
                await task
            assert entered.is_set()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
    assert len(calls) == 1


@pytest.mark.parametrize("operation", ["recover", "settle"])
async def test_even_a_recover_miss_requires_bounded_successful_response_cleanup(
    operation: str,
) -> None:
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield json.dumps(error() if operation == "recover" else ack()).encode()

        async def aclose(self) -> None:
            await asyncio.Event().wait()

    calls: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        return httpx.Response(
            404 if operation == "recover" else 200, stream=Stream(), headers=headers()
        )

    async with BffModelBudgetClient(
        "http://bff.test",
        lambda _: SecretStr("s" * 32),
        transport=httpx.MockTransport(handler),
        timeout_s=0.02,
    ) as c:
        with pytest.raises(ModelBudgetError, match="MODEL_BUDGET_UNAVAILABLE"):
            await asyncio.wait_for(
                recover(c) if operation == "recover" else settle(c), timeout=1
            )
    assert len(calls) == 1
