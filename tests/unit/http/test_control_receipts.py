"""Control command identity and receipt semantics."""

from __future__ import annotations

from typing import Any, cast

import pytest

from kokoro_agent.application.chat.service import ChatService
from kokoro_agent.domain.run.scope import runtime_namespace
from kokoro_agent.interfaces.http.ingress import AgentIngress, IngressError
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef
from kokoro_agent.domain.run.repository import (
    ControlAdmission,
    ControlAdmissionReceipt,
    ControlAdmissionStatus,
    ControlCommandConflict,
)


def _identity() -> ExecutionIdentity:
    return ExecutionIdentity(
        tenant_ref="tenant",
        actor=IdentityRef(kind="user", opaque_ref="actor"),
        subject=IdentityRef(kind="user", opaque_ref="subject"),
        identity_assertion_ref="assertion",
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

    async def get_request_scoped(self, run_id: str, tenant_ref: str, namespace: str):
        if tenant_ref != _identity().tenant_ref or namespace != runtime_namespace(
            _identity()
        ):
            return None
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


class ChatRepository:
    pass


def _ingress(bus: Bus, run_repository: ReceiptRepository) -> AgentIngress:
    return AgentIngress(
        bus=cast(Any, bus),
        run_repository=cast(Any, run_repository),
        chat_service=ChatService(cast(Any, ChatRepository())),
    )


@pytest.mark.asyncio
async def test_control_reuses_pending_command_receipt_for_safe_republish() -> None:
    run_repository = ReceiptRepository()
    bus = Bus()
    ingress = _ingress(bus, run_repository)
    body = {"kind": "run.cancel", "session_id": "session-1"}

    first = await ingress.control(
        "run-1", body, command_id="cmd-1", execution_identity=_identity()
    )
    second = await ingress.control(
        "run-1", body, command_id="cmd-1", execution_identity=_identity()
    )

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
            execution_identity=_identity(),
        )

    assert error.value.status == 400
    assert error.value.code == "invalid_run_control"


@pytest.mark.asyncio
async def test_control_rejects_same_command_id_with_different_request_digest() -> None:
    run_repository = ReceiptRepository()
    bus = Bus()
    ingress = _ingress(bus, run_repository)

    await ingress.control(
        "run-1",
        {"kind": "run.cancel", "session_id": "session-1"},
        command_id="cmd-1",
        execution_identity=_identity(),
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
            execution_identity=_identity(),
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

    await ingress.control(
        "run-1", body, command_id="cmd-1", execution_identity=_identity()
    )
    await run_repository.mark_control_succeeded("run-1", "cmd-1")
    replay = await ingress.control(
        "run-1", body, command_id="cmd-1", execution_identity=_identity()
    )

    assert replay["status"] == "succeeded"
    assert replay["replayed"] is True
    assert len(bus.published) == 1


@pytest.mark.asyncio
async def test_same_idempotency_key_is_scoped_to_the_run() -> None:
    run_repository = ReceiptRepository()
    bus = Bus()
    ingress = _ingress(bus, run_repository)
    body = {"kind": "run.cancel", "session_id": "session-1"}

    first = await ingress.control(
        "run-1", body, command_id="shared-key", execution_identity=_identity()
    )
    second = await ingress.control(
        "run-2", body, command_id="shared-key", execution_identity=_identity()
    )

    assert first["run_id"] == "run-1"
    assert second["run_id"] == "run-2"
    assert first["command_id"] == second["command_id"] == "shared-key"
    assert len(bus.published) == 2


@pytest.mark.asyncio
async def test_control_publish_failure_persists_failed_receipt() -> None:
    run_repository = ReceiptRepository()
    ingress = _ingress(Bus(fail=True), run_repository)

    failed = await ingress.control(
        "run-1",
        {"kind": "run.cancel", "session_id": "session-1"},
        command_id="cmd-1",
        execution_identity=_identity(),
    )

    assert failed["run_id"] == "run-1"
    assert failed["command_id"] == "cmd-1"
    assert failed["status"] == "failed"
    assert failed["error_code"] == "control_enqueue_failed"
    assert failed["replayed"] is False


@pytest.mark.asyncio
async def test_resume_normalization_binds_ingress_stored_body_and_decoder() -> None:
    from kokoro_agent.infrastructure.postgres_run_interactions import (
        decode_resume_command,
    )
    from kokoro_agent.protocol.control import RunResume, control_request_digest

    repository = ReceiptRepository()
    ingress = _ingress(Bus(), repository)
    body: dict[str, object] = {
        "kind": "run.resume",
        "session_id": "session-1",
        "expected_pause_revision": 1,
        "pause_ref": "pause-1",
        "decisions": [{"type": "reject", "item_id": "item-1"}],
    }
    first = await ingress.control(
        "run-1", body, command_id="cmd", execution_identity=_identity()
    )
    # Only this nullable optional reason may normalize omission to explicit null.
    second = await ingress.control(
        "run-1",
        {
            **body,
            "decisions": [{"type": "reject", "item_id": "item-1", "reason": None}],
        },
        command_id="cmd",
        execution_identity=_identity(),
    )
    assert first["request_digest"] == second["request_digest"]
    assert second["replayed"] is True
    stored = repository.commands[("run-1", "cmd")]
    text = str(stored["body"])
    parsed = RunResume.model_validate_json(text)
    digest = str(stored["request_digest"])
    assert control_request_digest(parsed) == digest
    assert parsed.request_digest == digest
    assert parsed.model_dump_json() == text
    submission = decode_resume_command(
        text, digest, run_id="run-1", session_id="session-1", command_id="cmd"
    )
    assert submission.decisions[0].payload == b"{}"
    with pytest.raises(ValueError):
        decode_resume_command(
            text + " ", digest, run_id="run-1", session_id="session-1", command_id="cmd"
        )
    assert len(repository.commands) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["revision", "ref", "item", "unknown", "nullable_required", "duplicate"]
)
async def test_resume_required_identity_and_unknown_fields_fail_before_admission(
    fault: str,
) -> None:
    repository = ReceiptRepository()
    bus = Bus()
    body: dict[str, Any] = {
        "kind": "run.resume",
        "session_id": "session-1",
        "expected_pause_revision": 1,
        "pause_ref": "pause-1",
        "decisions": [{"type": "approve", "item_id": "item-1"}],
    }
    if fault == "revision":
        body.pop("expected_pause_revision")
    elif fault == "ref":
        body.pop("pause_ref")
    elif fault == "item":
        body["decisions"][0].pop("item_id")
    elif fault == "unknown":
        body["decisions"][0]["unknown"] = None
    elif fault == "duplicate":
        body["decisions"].append(dict(body["decisions"][0]))
    else:
        body["pause_ref"] = None
    with pytest.raises(IngressError) as caught:
        await _ingress(bus, repository).control(
            "run-1", body, command_id="cmd", execution_identity=_identity()
        )
    assert caught.value.status == 400
    assert repository.commands == {} and bus.published == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["run.cancel", "run.steer"])
@pytest.mark.parametrize("field", ["pause_ref", "expected_pause_revision"])
async def test_resume_only_fields_do_not_change_cancel_or_steer_contract(
    kind: str, field: str
) -> None:
    repository = ReceiptRepository()
    body: dict[str, object] = {"kind": kind, "session_id": "session-1", field: None}
    if kind == "run.steer":
        body.update(message_id="m", content="hi")
    with pytest.raises(IngressError) as caught:
        await _ingress(Bus(), repository).control(
            "run-1", body, command_id="cmd", execution_identity=_identity()
        )
    assert caught.value.code == "invalid_run_control"
    assert repository.commands == {}


@pytest.mark.parametrize("kind", ["run.cancel", "run.steer"])
@pytest.mark.parametrize(
    "extra",
    [
        {"decisions": []},
        {"decisions": None},
        {"pause_ref": None},
        {"expected_pause_revision": None},
    ],
)
async def test_non_resume_rejects_resume_fields_before_admission(
    kind: str, extra: dict[str, object]
) -> None:
    repository = ReceiptRepository()
    bus = Bus()
    body: dict[str, object] = {"kind": kind, "session_id": "session-1", **extra}
    if kind == "run.steer":
        body.update(message_id="m", content="continue")
    with pytest.raises(IngressError) as caught:
        await _ingress(bus, repository).control(
            "run-1", body, command_id="bad-kind", execution_identity=_identity()
        )
    assert caught.value.status == 400 and caught.value.code == "invalid_run_control"
    assert repository.commands == {} and bus.published == []


@pytest.mark.parametrize(
    "decision",
    [
        {"type": "approve", "tool_id": "old"},
        {"type": "submit", "request_id": "old", "value": {}},
        {"type": "edit", "item_id": "item"},
        {"type": "respond", "item_id": "item", "response": ""},
    ],
)
async def test_resume_rejects_old_or_invalid_decisions_with_complete_pause_identity(
    decision: dict[str, object],
) -> None:
    repository, bus = ReceiptRepository(), Bus()
    with pytest.raises(IngressError) as caught:
        await _ingress(bus, repository).control(
            "run-1",
            {
                "kind": "run.resume",
                "session_id": "session-1",
                "expected_pause_revision": 1,
                "pause_ref": "pause-1",
                "decisions": [decision],
            },
            command_id="invalid",
            execution_identity=_identity(),
        )
    assert caught.value.status == 400 and caught.value.code == "invalid_run_control"
    assert repository.commands == {} and bus.published == []


@pytest.mark.parametrize(
    "extra", [{"message_id": None}, {"content": "not-a-cancel-field"}]
)
async def test_cancel_rejects_steer_fields_before_admission(
    extra: dict[str, object],
) -> None:
    repository, bus = ReceiptRepository(), Bus()
    with pytest.raises(IngressError) as failure:
        await _ingress(bus, repository).control(
            "run-1",
            {"kind": "run.cancel", "session_id": "session-1", **extra},
            command_id="command",
            execution_identity=_identity(),
        )
    assert (failure.value.status, failure.value.code) == (400, "invalid_run_control")
    assert repository.commands == {} and bus.published == []
