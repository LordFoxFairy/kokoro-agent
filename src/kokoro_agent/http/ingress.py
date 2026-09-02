"""Versioned business ingress for ``kokoro-agent``.

The worker remains the only component that executes a run.  This module owns
the transport seam that durably admits a launch, publishes the existing worker
envelope, and exposes only identity-scoped, safe chat projections to BFF.
It deliberately has no business database access beyond the Agent-owned
RunRepository and ChatRepository ports.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError

from kokoro_agent.services.chat_service import (
    ChatHistoryPage,
    ChatService,
    ChatQueryRequest,
    ChatReplayPage,
    ChatSessionListPage,
    ChatSessionListRequest,
)
from kokoro_agent.contract import (
    REQUESTS_MAXLEN,
    REQUESTS_STREAM,
    RUN_CONTROL_MAXLEN,
    InboundMessage,
    RunCancel,
    ExecutionIdentity,
    RunInput,
    RunRequest,
    RunResume,
    RunSteer,
    ResumeDecision,
    agent_event_adapter,
    run_control_stream,
    run_events_stream,
)
from kokoro_agent.execution.scope import runtime_namespace
from kokoro_agent.repositories.run_repository import (
    ControlCommandConflict,
    DispatchConflict,
    RunRepository,
)
from kokoro_agent.streams.protocol import StreamProtocol


class IngressError(RuntimeError):
    """An expected business ingress error with stable HTTP semantics."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


class LaunchBody(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    request_id: str
    run_id: str
    session_id: str
    feature_key: str
    execution_identity: ExecutionIdentity
    message_id: str
    content: str
    requested_model_label: str | None = None
    trace: dict[str, Any] | None = None


class ControlBody(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    kind: str
    session_id: str
    decisions: list[dict[str, Any]] | None = None
    message_id: str | None = None
    content: str | None = None


_RESUME_DECISIONS: TypeAdapter[list[ResumeDecision]] = TypeAdapter(list[ResumeDecision])


@dataclass(frozen=True, slots=True)
class LaunchReceipt:
    run_id: str
    session_id: str
    replayed: bool


def _canonical_fence(request: RunRequest) -> str:
    payload = request.model_dump(mode="json", exclude_none=True)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _parse_launch(body: Mapping[str, object]) -> RunRequest:
    try:
        launch = LaunchBody.model_validate(dict(body))
        return RunRequest(
            kind="run.request",
            request_id=launch.request_id,
            run_id=launch.run_id,
            session_id=launch.session_id,
            feature_key=launch.feature_key,
            execution_identity=launch.execution_identity,
            input=RunInput(message_id=launch.message_id, content=launch.content),
            requested_model_label=launch.requested_model_label,
            trace=launch.trace,
        )
    except (ValidationError, TypeError, ValueError) as error:
        raise IngressError(400, "invalid_launch_request", "Launch request does not match the v1 contract") from error


def _canonical_control_digest(run_id: str, control: ControlBody) -> str:
    payload = {"run_id": run_id, **control.model_dump(mode="json", exclude_none=True)}
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _parse_control(
    run_id: str, body: Mapping[str, object], *, command_id: str
) -> tuple[InboundMessage, str]:
    try:
        control = ControlBody.model_validate(dict(body))
        request_digest = _canonical_control_digest(run_id, control)
        if control.kind == "run.cancel":
            return RunCancel(
                kind="run.cancel",
                run_id=run_id,
                session_id=control.session_id,
                command_id=command_id,
                request_digest=request_digest,
            ), request_digest
        if control.kind == "run.steer":
            if control.message_id is None or control.content is None:
                raise ValueError("steer requires message_id and content")
            return RunSteer(
                kind="run.steer",
                run_id=run_id,
                session_id=control.session_id,
                command_id=command_id,
                request_digest=request_digest,
                message_id=control.message_id,
                content=control.content,
            ), request_digest
        if control.kind == "run.resume" and control.decisions:
            return RunResume(
                kind="run.resume",
                run_id=run_id,
                session_id=control.session_id,
                command_id=command_id,
                request_digest=request_digest,
                decisions=_RESUME_DECISIONS.validate_python(control.decisions),
            ), request_digest
        raise ValueError("control kind or required fields are invalid")
    except (ValidationError, TypeError, ValueError) as error:
        raise IngressError(400, "invalid_run_control", "Control request does not match the v1 contract") from error


def _event_json(event: object) -> dict[str, Any]:
    try:
        parsed = agent_event_adapter.validate_python(event)
    except ValidationError as error:
        raise IngressError(502, "agent_event_invalid", "Agent produced an invalid event") from error
    return parsed.model_dump(mode="json", exclude_none=True)


class AgentIngress:
    """Business transport facade over the Agent-owned worker ports."""

    def __init__(self, *, bus: StreamProtocol, run_repository: RunRepository, chat_service: ChatService) -> None:
        self._bus = bus
        self._run_repository = run_repository
        self._chat_service = chat_service

    async def launch(self, body: Mapping[str, object]) -> LaunchReceipt:
        request = _parse_launch(body)
        namespace = runtime_namespace(request.execution_identity)
        try:
            admission = await self._run_repository.enqueue_dispatch(
                request, namespace, _canonical_fence(request)
            )
        except DispatchConflict as error:
            raise IngressError(409, "run_identity_conflict", str(error)) from error
        trace = request.trace or {}
        project_ref_value = trace.get("project_ref")
        project_ref = project_ref_value if isinstance(project_ref_value, str) else None
        await self._chat_service.ensure_session(
            request.execution_identity,
            request.session_id,
            project_ref=project_ref,
            title=request.input.content.strip() or "Kokoro chat",
            updated_at=time.time_ns() // 1_000_000,
        )
        if admission.publish_required:
            await self._bus.publish(
                REQUESTS_STREAM,
                request.model_dump(mode="json", exclude_none=True),
                maxlen=REQUESTS_MAXLEN,
            )
        return LaunchReceipt(
            run_id=request.run_id,
            session_id=request.session_id,
            replayed=admission.replayed,
        )

    async def control(
        self, run_id: str, body: Mapping[str, object], *, command_id: str
    ) -> dict[str, object]:
        command_id = command_id.strip()
        if not command_id.strip():
            raise IngressError(400, "idempotency_key_required", "Control requests require Idempotency-Key")
        msg, request_digest = _parse_control(run_id, body, command_id=command_id)
        request = await self._run_repository.get_request(run_id)
        if request is None:
            raise IngressError(404, "run_not_found", "Run was not found")
        if request.session_id != msg.session_id:
            raise IngressError(403, "run_scope_forbidden", "Run does not belong to this session")
        try:
            admission = await self._run_repository.admit_control(
                run_id, command_id, request_digest, msg.model_dump_json()
            )
        except ControlCommandConflict as error:
            raise IngressError(409, "command_digest_mismatch", str(error)) from error
        if admission.publish_required:
            try:
                await self._bus.publish(
                    run_control_stream(run_id),
                    msg.model_dump(mode="json", exclude_none=True),
                    maxlen=RUN_CONTROL_MAXLEN,
                )
            except Exception:
                await self._run_repository.mark_control_failed(run_id, command_id, "control_enqueue_failed")
                receipt = admission.receipt.model_copy(
                    update={"status": "failed", "error_code": "control_enqueue_failed"}
                )
                return {
                    **receipt.model_dump(mode="json", exclude_none=True),
                    "replayed": admission.replayed,
                }
        return {
            **admission.receipt.model_dump(mode="json", exclude_none=True),
            "replayed": admission.replayed,
        }

    async def evidence(self, run_id: str, *, after_seq: int = 0, limit: int = 200) -> dict[str, object]:
        if after_seq < 0 or limit < 1 or limit > 1000:
            raise IngressError(400, "invalid_page", "after_seq must be >= 0 and limit must be 1..1000")
        if await self._run_repository.get_request(run_id) is None:
            raise IngressError(404, "run_not_found", "Run was not found")
        items = await self._bus.read_all(run_events_stream(run_id))
        events = [_event_json(item.event) for item in items]
        events = [event for event in events if int(event["index"]) > after_seq][:limit]
        next_seq = int(events[-1]["index"]) if events else after_seq
        terminal = any(event["kind"] in {"run.completed", "run.failed"} for event in events)
        return {"run_id": run_id, "events": events, "next_seq": next_seq, "terminal": terminal}

    async def history(
        self, identity: ChatQueryRequest, *, session_id: str | None = None
    ) -> ChatHistoryPage:
        if session_id is not None:
            identity = identity.model_copy(update={"session_id": session_id})
        return await self._chat_service.history(identity)

    async def replay(
        self, identity: ChatQueryRequest, *, session_id: str | None = None
    ) -> ChatReplayPage:
        if session_id is not None:
            identity = identity.model_copy(update={"session_id": session_id})
        return await self._chat_service.replay(identity)

    async def list_sessions(self, request: ChatSessionListRequest) -> ChatSessionListPage:
        return await self._chat_service.list_sessions(request)


__all__ = ["AgentIngress", "IngressError", "LaunchReceipt"]
