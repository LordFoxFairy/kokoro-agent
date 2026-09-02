"""Control command identity and receipt semantics."""

from __future__ import annotations

from typing import Any, cast

import pytest

from kokoro_agent.chat.query import ChatQuery
from kokoro_agent.http.ingress import AgentIngress, IngressError
from kokoro_agent.repositories.run_repository import (
    ControlAdmission,
    ControlAdmissionReceipt,
    ControlAdmissionStatus,
    ControlCommandConflict,
)


class ReceiptRepository:
    def __init__(self) -> None:
        self.requests = {
            "run-1": type("Request", (), {"session_id": "session-1"})(),
            "run-2": type("Request", (), {"session_id": "session-1"})(),
        }
        self.commands: dict[tuple[str, str], dict[str, object]] = {}

    async def get_request(self, run_id: str):
        return self.requests.get(run_id)

    async def admit_control(
        self, run_id: str, command_id: str, request_digest: str, body: str
    ):
        key = (run_id, command_id)
        existing = self.commands.get(key)
        if existing is not None:
            if existing["request_digest"] != request_digest:
                raise ControlCommandConflict("command digest mismatch")
            status = str(existing["status"])
            public_status = cast(
                ControlAdmissionStatus,
                {
                    "admitted": "pending",
                    "succeeded": "succeeded",
                    "failed": "failed",
                }[status],
            )
            return ControlAdmission(
                receipt=ControlAdmissionReceipt(
                    run_id=run_id,
                    command_id=command_id,
                    request_digest=request_digest,
                    status=public_status,
                    error_code=cast(str | None, existing["error_code"]),
                ),
                replayed=True,
                publish_required=status == "admitted",
            )
        self.commands[key] = {
            "request_digest": request_digest,
            "status": "admitted",
            "body": body,
            "error_code": None,
        }
        return ControlAdmission(
            receipt=ControlAdmissionReceipt(
                run_id=run_id,
                command_id=command_id,
                request_digest=request_digest,
                status="pending",
            ),
            replayed=False,
            publish_required=True,
        )

    async def mark_control_succeeded(self, run_id: str, command_id: str) -> None:
        self.commands[(run_id, command_id)]["status"] = "succeeded"

    async def mark_control_failed(
        self, run_id: str, command_id: str, error_code: str | None = None
    ) -> None:
        command = self.commands[(run_id, command_id)]
        command["status"] = "failed"
        command["error_code"] = error_code


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


def _ingress(bus: Bus, run_repository: ReceiptRepository) -> AgentIngress:
    return AgentIngress(
        bus=cast(Any, bus),
        run_repository=cast(Any, run_repository),
        chat_query=ChatQuery(cast(Any, ChatStore())),
    )


@pytest.mark.asyncio
async def test_control_reuses_pending_command_receipt_for_safe_republish() -> None:
    run_repository = ReceiptRepository()
    bus = Bus()
    ingress = _ingress(bus, run_repository)
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
    run_repository = ReceiptRepository()
    ingress = _ingress(Bus(), run_repository)

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
    run_repository = ReceiptRepository()
    bus = Bus()
    ingress = _ingress(bus, run_repository)

    await ingress.control(
        "run-1", {"kind": "run.cancel", "session_id": "session-1"}, command_id="cmd-1"
    )
    with pytest.raises(IngressError) as error:
        await ingress.control(
            "run-1",
            {
                "kind": "run.steer",
                "session_id": "session-1",
                "message_id": "m",
                "content": "x",
            },
            command_id="cmd-1",
        )

    assert error.value.status == 409
    assert error.value.code == "command_digest_mismatch"
    assert len(bus.published) == 1


@pytest.mark.asyncio
async def test_control_receipt_replay_returns_terminal_status_without_republishing() -> (
    None
):
    run_repository = ReceiptRepository()
    bus = Bus()
    ingress = _ingress(bus, run_repository)
    body = {"kind": "run.cancel", "session_id": "session-1"}

    await ingress.control("run-1", body, command_id="cmd-1")
    await run_repository.mark_control_succeeded("run-1", "cmd-1")
    replay = await ingress.control("run-1", body, command_id="cmd-1")

    assert replay["status"] == "succeeded"
    assert replay["replayed"] is True
    assert len(bus.published) == 1


@pytest.mark.asyncio
async def test_same_idempotency_key_is_scoped_to_the_run() -> None:
    run_repository = ReceiptRepository()
    bus = Bus()
    ingress = _ingress(bus, run_repository)
    body = {"kind": "run.cancel", "session_id": "session-1"}

    first = await ingress.control("run-1", body, command_id="shared-key")
    second = await ingress.control("run-2", body, command_id="shared-key")

    assert first["run_id"] == "run-1"
    assert second["run_id"] == "run-2"
    assert first["command_id"] == second["command_id"] == "shared-key"
    assert len(bus.published) == 2


@pytest.mark.asyncio
async def test_control_publish_failure_persists_failed_receipt() -> None:
    run_repository = ReceiptRepository()
    ingress = _ingress(Bus(fail=True), run_repository)

    failed = await ingress.control(
        "run-1", {"kind": "run.cancel", "session_id": "session-1"}, command_id="cmd-1"
    )

    assert failed["run_id"] == "run-1"
    assert failed["command_id"] == "cmd-1"
    assert failed["status"] == "failed"
    assert failed["error_code"] == "control_enqueue_failed"
    assert failed["replayed"] is False
