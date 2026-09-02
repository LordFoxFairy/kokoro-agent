"""Control command identity and receipt semantics."""

from __future__ import annotations

from typing import Any, cast

import pytest

from kokoro_agent.chat.query import ChatQuery
from kokoro_agent.http.ingress import AgentIngress, IngressError
from kokoro_agent.storage.ledger import ControlAdmission, ControlCommandConflict, ControlReceipt


class ReceiptLedger:
    def __init__(self) -> None:
        self.requests = {"run-1": type("Request", (), {"session_id": "session-1"})()}
        self.receipts: dict[str, ControlReceipt] = {}

    async def get_request(self, run_id: str):
        return self.requests.get(run_id)

    async def admit_control(self, run_id: str, command_id: str, request_digest: str, body: str):
        existing = self.receipts.get(command_id)
        if existing is not None:
            if existing.request_digest != request_digest:
                raise ControlCommandConflict("command digest mismatch")
            return ControlAdmission(
                receipt=existing,
                replayed=True,
                publish_required=existing.status == "pending",
            )
        receipt = ControlReceipt(
            run_id=run_id,
            command_id=command_id,
            request_digest=request_digest,
            status="pending",
        )
        self.receipts[command_id] = receipt
        return ControlAdmission(receipt=receipt, replayed=False, publish_required=True)

    async def mark_control_succeeded(self, command_id: str) -> None:
        receipt = self.receipts[command_id]
        self.receipts[command_id] = receipt.model_copy(update={"status": "succeeded"})

    async def mark_control_failed(self, command_id: str, error_code: str | None = None) -> None:
        receipt = self.receipts[command_id]
        self.receipts[command_id] = receipt.model_copy(
            update={"status": "failed", "error_code": error_code}
        )


class Bus:
    def __init__(self, *, fail: bool = False) -> None:
        self.published: list[dict[str, object]] = []
        self.fail = fail

    async def publish(self, stream: str, event: dict[str, object], *, maxlen: int):
        if self.fail:
            raise RuntimeError("publish failed")
        self.published.append({"stream": stream, "event": event, "maxlen": maxlen})


class ChatStore:
    pass


def _ingress(bus: Bus, ledger: ReceiptLedger) -> AgentIngress:
    return AgentIngress(
        bus=cast(Any, bus),
        ledger=cast(Any, ledger),
        chat_query=ChatQuery(cast(Any, ChatStore())),
    )


@pytest.mark.asyncio
async def test_control_reuses_pending_command_receipt_for_safe_republish() -> None:
    ledger = ReceiptLedger()
    bus = Bus()
    ingress = _ingress(bus, ledger)
    body = {"kind": "run.cancel", "session_id": "session-1"}

    first = await ingress.control("run-1", body, command_id="cmd-1")
    second = await ingress.control("run-1", body, command_id="cmd-1")

    assert first["status"] == "pending"
    assert first["replayed"] is False
    assert second["status"] == "pending"
    assert second["replayed"] is True
    assert first["request_digest"] == second["request_digest"]
    assert len(bus.published) == 2


@pytest.mark.asyncio
async def test_control_rejects_old_decision_id_alias() -> None:
    ledger = ReceiptLedger()
    ingress = _ingress(Bus(), ledger)

    with pytest.raises(IngressError, match="v1 contract") as error:
        await ingress.control(
            "run-1",
            {"kind": "run.cancel", "session_id": "session-1", "decision_id": "old"},
            command_id="cmd-1",
        )

    assert error.value.status == 400
    assert error.value.code == "invalid_run_control"


@pytest.mark.asyncio
async def test_control_rejects_same_command_id_with_different_request_digest() -> None:
    ledger = ReceiptLedger()
    bus = Bus()
    ingress = _ingress(bus, ledger)

    await ingress.control(
        "run-1", {"kind": "run.cancel", "session_id": "session-1"}, command_id="cmd-1"
    )
    with pytest.raises(IngressError) as error:
        await ingress.control(
            "run-1",
            {"kind": "run.steer", "session_id": "session-1", "message_id": "m", "content": "x"},
            command_id="cmd-1",
        )

    assert error.value.status == 409
    assert error.value.code == "command_digest_mismatch"
    assert len(bus.published) == 1


@pytest.mark.asyncio
async def test_control_receipt_replay_returns_terminal_status_without_republishing() -> None:
    ledger = ReceiptLedger()
    bus = Bus()
    ingress = _ingress(bus, ledger)
    body = {"kind": "run.cancel", "session_id": "session-1"}

    await ingress.control("run-1", body, command_id="cmd-1")
    await ledger.mark_control_succeeded("cmd-1")
    replay = await ingress.control("run-1", body, command_id="cmd-1")

    assert replay["status"] == "succeeded"
    assert replay["replayed"] is True
    assert len(bus.published) == 1


@pytest.mark.asyncio
async def test_control_publish_failure_persists_failed_receipt() -> None:
    ledger = ReceiptLedger()
    ingress = _ingress(Bus(fail=True), ledger)

    failed = await ingress.control(
        "run-1", {"kind": "run.cancel", "session_id": "session-1"}, command_id="cmd-1"
    )

    assert failed["run_id"] == "run-1"
    assert failed["command_id"] == "cmd-1"
    assert failed["status"] == "failed"
    assert failed["error_code"] == "control_enqueue_failed"
    assert failed["replayed"] is False
