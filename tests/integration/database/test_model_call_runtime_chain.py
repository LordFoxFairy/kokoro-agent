"""System gate for the real native model path before Billing is published."""

from __future__ import annotations

import asyncio
from copy import deepcopy
import hashlib
import json
import re
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal, Never, ParamSpec, TypeVar
from uuid import uuid4

import pytest
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore
from pydantic import SecretStr, TypeAdapter

from kokoro_agent.agent_factory import AgentFactory, AgentHandle
from kokoro_agent.agents.subagent_catalog import build_subagent_catalog
from kokoro_agent.clients.system import SystemModelClient
from kokoro_agent.config import AppConfig
from kokoro_agent.model.credentials import ModelCredential
from kokoro_agent.domain.run.models import (
    ExecutionTerminalAuthority,
    RunTerminalOutcome,
    RunUsageSegment,
)
from kokoro_agent.domain.run.repository import RunRepository
from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution import publish_agent_events
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.execution.runtime_profile_plan import RuntimeAssemblyPolicy
from kokoro_agent.execution.runtime_profile_sources import production_manifest
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_model_call_repository import (
    PostgresModelCallRepository,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one
from kokoro_agent.model.call_models import PrepareModelCall, PreparedModelCall
from kokoro_agent.protocol import (
    ExecutionIdentity,
    IdentityRef,
    RunInput,
    RunCompletedPayload,
    RunFailedPayload,
    RunRequest,
    RunStartedPayload,
    SkillPhase,
    SkillProgressCommitted,
    SkillProgressSink,
)
from kokoro_agent.infrastructure.checkpoint_interactions import (
    PauseReadTarget,
    ResumeReadTarget,
)
from kokoro_agent.tools.toolbox import build_toolbox
from kokoro_agent.worker.dependencies import WorkerDependencies
from kokoro_agent.worker.supervisor import RunSupervisor
from kokoro_agent.streams.protocol import StreamItem
from support.fakes import FakeBus

_JSON_OBJECT = TypeAdapter(dict[str, object])
_OBJECT_LIST = TypeAdapter(list[object])
_OBJECT_TUPLE = TypeAdapter(tuple[object, ...])
_AsyncOperation = Callable[..., Awaitable[object]]
_ASYNC_OPERATION: TypeAdapter[_AsyncOperation] = TypeAdapter(_AsyncOperation)
_PREPARE_TOTAL_TIMEOUT_SECONDS = 5.0
_P = ParamSpec("_P")
_T = TypeVar("_T")


@dataclass(slots=True)
class _PrepareTaskTrace:
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    finished: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[object] | None = None
    outcome: str = "not-entered"


class _TracingPostgresModelCallRepository(PostgresModelCallRepository):
    """Test-only observation around the unchanged real prepare transaction."""

    def __init__(
        self,
        database_url: str,
        schema: str,
        trace: _PrepareTaskTrace,
        *,
        total_timeout_seconds: float,
    ) -> None:
        super().__init__(
            database_url,
            schema,
            total_timeout_seconds=total_timeout_seconds,
        )
        self._trace = trace

    async def prepare(self, command: PrepareModelCall) -> PreparedModelCall:
        task = asyncio.current_task()
        assert task is not None
        self._trace.task = task
        self._trace.outcome = "entered"
        self._trace.entered.set()
        try:
            receipt = await super().prepare(command)
        except asyncio.CancelledError:
            self._trace.outcome = "cancelled"
            raise
        except BaseException as error:
            self._trace.outcome = type(error).__name__
            raise
        else:
            self._trace.outcome = "committed"
            return receipt
        finally:
            self._trace.finished.set()


def _task_snapshot(
    task: asyncio.Task[object] | asyncio.Task[bool],
) -> dict[str, object]:
    return {
        "id": id(task),
        "name": task.get_name(),
        "cancelling": task.cancelling(),
        "done": task.done(),
        "cancelled": task.cancelled(),
        "stack": tuple(
            f"{frame.f_code.co_filename}:{frame.f_code.co_name}:{frame.f_lineno}"
            for frame in task.get_stack()
        ),
    }


def _emit_prepare_trace(
    label: str,
    trace: _PrepareTaskTrace,
    invoke_task: asyncio.Task[bool],
) -> None:
    prepare_task = trace.task
    assert prepare_task is not None
    print(
        "R201_PREPARE_TASK_TRACE "
        + json.dumps(
            {
                "label": label,
                "outcome": trace.outcome,
                "prepare": _task_snapshot(prepare_task),
                "invoke": _task_snapshot(invoke_task),
            },
            sort_keys=True,
        )
    )


@dataclass(slots=True)
class _OwnerAndProviderPeer:
    owner_requests: int = 0
    provider_requests: int = 0
    unknown_requests: list[str] = field(default_factory=list[str])
    _server: asyncio.Server | None = None
    _writers: set[asyncio.StreamWriter] = field(
        default_factory=lambda: set[asyncio.StreamWriter]()
    )

    @property
    def origin(self) -> str:
        sockets = () if self._server is None else self._server.sockets
        assert sockets
        return f"http://127.0.0.1:{int(sockets[0].getsockname()[1])}"

    async def __aenter__(self) -> _OwnerAndProviderPeer:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        for writer in tuple(self._writers):
            writer.close()
        for writer in tuple(self._writers):
            try:
                await writer.wait_closed()
            except (ConnectionError, RuntimeError):
                pass
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._writers.add(writer)
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            request_line = headers.split(b"\r\n", 1)[0].decode("ascii")
            content_length = _content_length(headers)
            if content_length:
                await reader.readexactly(content_length)
            if request_line == "POST /v1/system/model-catalog/resolve HTTP/1.1":
                self.owner_requests += 1
                body = _system_route_body(self.origin + "/v1")
                content_type = "application/json"
                status = "200 OK"
            elif request_line == "POST /v1/chat/completions HTTP/1.1":
                self.provider_requests += 1
                body = _provider_stream_body()
                content_type = "text/event-stream"
                status = "200 OK"
            else:
                self.unknown_requests.append(request_line)
                body = b'{"error":"unknown route"}'
                content_type = "application/json"
                status = "404 Not Found"
            writer.write(
                (
                    f"HTTP/1.1 {status}\r\n"
                    f"Content-Type: {content_type}\r\n"
                    f"Content-Length: {len(body)}\r\n"
                    "Connection: close\r\n\r\n"
                ).encode("ascii")
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, RuntimeError):
                pass
            self._writers.discard(writer)


def _content_length(headers: bytes) -> int:
    for line in headers.split(b"\r\n"):
        name, separator, value = line.partition(b":")
        if separator and name.lower() == b"content-length":
            return int(value.strip())
    return 0


def _system_route_body(base_url: str) -> bytes:
    return json.dumps(
        {
            "data": {
                "model_id": "10000000-0000-4000-8000-000000000001",
                "provider_id": "10000000-0000-4000-8000-000000000002",
                "revision_id": "10000000-0000-4000-8000-000000000003",
                "revision": 1,
                "digest": "a" * 64,
                "generation": "1",
                "tenant_generation": "1",
                "provider_model_name": "provider-model-r197",
                "request_model": "gateway-model-r197",
                "feature_key": "chat",
                "label_key": "default",
                "connection": {
                    "protocol": "openai_chat_completions",
                    "connection_type": "direct",
                    "base_url": base_url,
                    "secret_handle_ref": "runtime-chain-model-key",
                },
                "context_window": 8192,
                "max_output_tokens": 256,
            }
        },
        separators=(",", ":"),
    ).encode()


def _provider_stream_body() -> bytes:
    return (
        'data: {"id":"chatcmpl-r197","object":"chat.completion.chunk",'
        '"created":1,"model":"gateway-model-r197","choices":[{"index":0,'
        '"delta":{"role":"assistant","content":"ok"},"finish_reason":null}]}\n\n'
        'data: {"id":"chatcmpl-r197","object":"chat.completion.chunk",'
        '"created":1,"model":"gateway-model-r197","choices":[{"index":0,'
        '"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,'
        '"completion_tokens":1,"total_tokens":2}}\n\ndata: [DONE]\n\n'
    ).encode()


def _request() -> RunRequest:
    suffix = uuid4().hex
    return RunRequest(
        kind="run.request",
        run_id=f"run-r197-{suffix}",
        session_id=f"session-r197-{suffix}",
        feature_key="chat",
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant-r197",
            actor=IdentityRef(kind="user", opaque_ref="actor-r197"),
            subject=IdentityRef(kind="user", opaque_ref="subject-r197"),
            identity_assertion_ref="assertion-r197",
        ),
        input=RunInput(message_id=f"message-r197-{suffix}", content="hello"),
    )


async def _progress(_phase: SkillPhase) -> SkillProgressCommitted:
    return SkillProgressCommitted(source_index=0)


async def _lifecycle_facts(
    database_url: str, schema: str, run_id: str
) -> dict[str, object]:
    async with connect_pg(database_url) as connection, connection.cursor() as cursor:
        await execute_sql(
            cursor,
            """SELECT
                terminal, interaction_phase, model_attempt_count,
                model_dispatched_attempt_count, model_sealed_attempt_count,
                model_unknown_attempt_count, model_usage_completeness
              FROM {} WHERE run_id=%s""".format(qualified(schema, "kokoro_agent_run")),
            (run_id,),
        )
        run = await fetch_one(cursor)
        assert run is not None
        await execute_sql(
            cursor,
            """SELECT status,authorization_state,dispatch_started_at,latest_revision
               FROM {} WHERE run_id=%s ORDER BY attempt_ordinal""".format(
                qualified(schema, "kokoro_agent_model_call_attempt")
            ),
            (run_id,),
        )
        attempt = await fetch_one(cursor)
        await execute_sql(
            cursor,
            """SELECT execution_outcome,usage_state,totals_state,
                      classification_state,sealed,evidence_bytes
               FROM {} WHERE run_id=%s ORDER BY revision""".format(
                qualified(schema, "kokoro_agent_model_usage_evidence")
            ),
            (run_id,),
        )
        evidence = await fetch_one(cursor)
        await execute_sql(
            cursor,
            """SELECT kind,payload_json FROM {} WHERE run_id=%s
               AND kind IN ('run.completed','run.failed') ORDER BY durable_seq""".format(
                qualified(schema, "kokoro_agent_run_outbox")
            ),
            (run_id,),
        )
        terminal = await fetch_one(cursor)
        evidence_document: dict[str, object] | None = None
        if evidence is not None:
            decoded = _JSON_OBJECT.validate_json(bytes(evidence["evidence_bytes"]))
            actual = _JSON_OBJECT.validate_python(decoded.get("actual"), strict=True)
            usage = _JSON_OBJECT.validate_python(decoded.get("usage"), strict=True)
            evidence_document = {
                "actual": {
                    "state": actual.get("state"),
                    "reason": actual.get("reason"),
                },
                "usage": {
                    "state": usage.get("state"),
                    "reason": usage.get("reason"),
                },
            }
        return {
            "run": {
                "terminal": bool(run["terminal"]),
                "interaction_phase": str(run["interaction_phase"]),
                "attempts": int(run["model_attempt_count"]),
                "dispatched": int(run["model_dispatched_attempt_count"]),
                "sealed": int(run["model_sealed_attempt_count"]),
                "unknown": int(run["model_unknown_attempt_count"]),
                "completeness": str(run["model_usage_completeness"]),
            },
            "attempt": (
                None
                if attempt is None
                else {
                    "status": str(attempt["status"]),
                    "authorization_state": str(attempt["authorization_state"]),
                    "dispatched": attempt["dispatch_started_at"] is not None,
                    "latest_revision": int(attempt["latest_revision"]),
                }
            ),
            "evidence": (
                None
                if evidence is None
                else {
                    "outcome": str(evidence["execution_outcome"]),
                    "usage_state": str(evidence["usage_state"]),
                    "totals_state": str(evidence["totals_state"]),
                    "classification_state": str(evidence["classification_state"]),
                    "sealed": bool(evidence["sealed"]),
                    "document": evidence_document,
                }
            ),
            "terminal": (
                None
                if terminal is None
                else {
                    "kind": str(terminal["kind"]),
                    "payload": json.loads(str(terminal["payload_json"])),
                }
            ),
        }


async def _database_snapshot(
    database_url: str, schema: str, run_id: str
) -> tuple[tuple[dict[str, object], ...], ...]:
    tables = (
        "kokoro_agent_run",
        "kokoro_agent_model_call_attempt",
        "kokoro_agent_model_usage_evidence",
        "kokoro_agent_run_outbox",
    )
    snapshots: list[tuple[dict[str, object], ...]] = []
    async with connect_pg(database_url) as connection, connection.cursor() as cursor:
        for table in tables:
            await execute_sql(
                cursor,
                f"SELECT * FROM {qualified(schema, table)} WHERE run_id=%s ORDER BY 1",
                (run_id,),
            )
            rows = await cursor.fetchall()
            snapshots.append(tuple(dict(row) for row in rows))
    return tuple(snapshots)


async def _checkpoint_snapshot(
    checkpointer: BaseCheckpointSaver[str], thread_id: str
) -> tuple[object, ...]:
    config: RunnableConfig = {"configurable": {"thread_id": thread_id}}
    return tuple([deepcopy(item) async for item in checkpointer.alist(config)])


def _safe_checkpoint_leaf(value: object) -> dict[str, object]:
    rendered = repr(value).encode("utf-8", errors="backslashreplace")
    return {
        "type": f"{type(value).__module__}.{type(value).__qualname__}",
        "digest": hashlib.sha256(rendered).hexdigest(),
        "size": len(rendered),
    }


def _safe_checkpoint_key(value: object) -> str:
    if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:<>|/-]{1,96}", value):
        return value
    digest = _safe_checkpoint_leaf(value)["digest"]
    assert isinstance(digest, str)
    return "key#" + digest[:16]


def _safe_checkpoint_shape(value: object) -> object:
    if isinstance(value, BaseException):
        return {
            "exception": _safe_checkpoint_leaf(value),
            "args": [_safe_checkpoint_shape(argument) for argument in value.args],
        }
    if isinstance(value, dict):
        mapping = _JSON_OBJECT.validate_python(value, strict=True)
        return {
            _safe_checkpoint_key(key): _safe_checkpoint_shape(item)
            for key, item in sorted(mapping.items())
        }
    if isinstance(value, list):
        items = _OBJECT_LIST.validate_python(value, strict=True)
        return [_safe_checkpoint_shape(item) for item in items]
    if isinstance(value, tuple):
        items = _OBJECT_TUPLE.validate_python(value, strict=True)
        return [_safe_checkpoint_shape(item) for item in items]
    return _safe_checkpoint_leaf(value)


def _safe_checkpoint_flatten(value: object, *, path: str = "$") -> dict[str, str]:
    if isinstance(value, dict):
        mapping = _JSON_OBJECT.validate_python(value, strict=True)
        flattened: dict[str, str] = {}
        for key, item in mapping.items():
            flattened.update(_safe_checkpoint_flatten(item, path=f"{path}.{key}"))
        return flattened
    if isinstance(value, list):
        items = _OBJECT_LIST.validate_python(value, strict=True)
        flattened = {}
        for index, item in enumerate(items):
            flattened.update(_safe_checkpoint_flatten(item, path=f"{path}[{index}]"))
        return flattened
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return {path: hashlib.sha256(encoded).hexdigest()}


def _safe_checkpoint_leaf_types(value: object, *, path: str = "$") -> dict[str, str]:
    if isinstance(value, dict):
        mapping = _JSON_OBJECT.validate_python(value, strict=True)
        leaf_type = mapping.get("type")
        if isinstance(leaf_type, str) and "digest" in mapping and "size" in mapping:
            return {path: leaf_type}
        flattened: dict[str, str] = {}
        for key, item in mapping.items():
            flattened.update(_safe_checkpoint_leaf_types(item, path=f"{path}.{key}"))
        return flattened
    if isinstance(value, list):
        items = _OBJECT_LIST.validate_python(value, strict=True)
        flattened = {}
        for index, item in enumerate(items):
            flattened.update(_safe_checkpoint_leaf_types(item, path=f"{path}[{index}]"))
        return flattened
    return {}


def _pending_channel_error_flags(snapshot: tuple[object, ...]) -> list[bool]:
    flags: list[bool] = []
    for checkpoint in snapshot:
        values = _OBJECT_TUPLE.validate_python(checkpoint, strict=True)
        if len(values) < 5:
            continue
        pending = _OBJECT_LIST.validate_python(values[4], strict=True)
        for write in pending:
            fields = _OBJECT_TUPLE.validate_python(write, strict=True)
            flags.append(len(fields) >= 2 and fields[1] == "__error__")
    return flags


def _pending_cancellation_value_candidates(
    snapshot: tuple[object, ...],
) -> list[dict[str, object]]:
    candidates: list[dict[str, object]] = []
    for checkpoint in snapshot:
        values = _OBJECT_TUPLE.validate_python(checkpoint, strict=True)
        if len(values) < 5:
            continue
        pending = _OBJECT_LIST.validate_python(values[4], strict=True)
        for write in pending:
            fields = _OBJECT_TUPLE.validate_python(write, strict=True)
            if len(fields) < 3:
                continue
            value = fields[2]
            candidates.append(
                {
                    "type": f"{type(value).__module__}.{type(value).__qualname__}",
                    "matches_cancelled_error_call": value == "CancelledError()",
                    "matches_cancelled_error_name": value == "CancelledError",
                }
            )
    return candidates


def _emit_checkpoint_delta(
    label: str, before: tuple[object, ...], after: tuple[object, ...]
) -> None:
    before_shape = _safe_checkpoint_shape(before)
    after_shape = _safe_checkpoint_shape(after)
    before_fields = _safe_checkpoint_flatten(before_shape)
    after_fields = _safe_checkpoint_flatten(after_shape)
    before_types = _safe_checkpoint_leaf_types(before_shape)
    after_types = _safe_checkpoint_leaf_types(after_shape)
    changed = [
        path
        for path in sorted(before_fields.keys() | after_fields.keys())
        if before_fields.get(path) != after_fields.get(path)
    ]
    payload = {
        "label": label,
        "before_digest": _safe_checkpoint_leaf(before_shape)["digest"],
        "after_digest": _safe_checkpoint_leaf(after_shape)["digest"],
        "changed_count": len(changed),
        "changed_paths": changed,
        "changed_leaf_types": [
            {
                "path": path,
                "before": before_types.get(path),
                "after": after_types.get(path),
            }
            for path in sorted(before_types.keys() | after_types.keys())
            if before_types.get(path) != after_types.get(path)
        ],
        "before_pending_matches_error": _pending_channel_error_flags(before),
        "after_pending_matches_error": _pending_channel_error_flags(after),
        "before_pending_cancel_candidates": (
            _pending_cancellation_value_candidates(before)
        ),
        "after_pending_cancel_candidates": _pending_cancellation_value_candidates(
            after
        ),
    }
    print("R202_CHECKPOINT_DELTA " + json.dumps(payload, sort_keys=True))


def _emit_cancelled_error_deepcopy_probe() -> None:
    original = asyncio.CancelledError("r202-probe")
    first = deepcopy(original)
    second = deepcopy(original)
    payload = {
        "equal": first == second,
        "same_identity": first is second,
        "first_digest": _safe_checkpoint_leaf(_safe_checkpoint_shape(first))["digest"],
        "second_digest": _safe_checkpoint_leaf(_safe_checkpoint_shape(second))[
            "digest"
        ],
    }
    print("R202_CANCELLED_ERROR_DEEPCOPY " + json.dumps(payload, sort_keys=True))


def _assert_exact_single_cancellation_audit(
    before: tuple[object, ...], after: tuple[object, ...]
) -> None:
    assert len(after) == len(before)
    assert after[1:] == before[1:]
    before_latest = _OBJECT_TUPLE.validate_python(before[0], strict=True)
    after_latest = _OBJECT_TUPLE.validate_python(after[0], strict=True)
    assert len(after_latest) == len(before_latest)
    assert len(after_latest) >= 5
    for index, (before_field, after_field) in enumerate(
        zip(before_latest, after_latest, strict=True)
    ):
        if index != 4:
            assert after_field == before_field
    before_pending = _OBJECT_LIST.validate_python(before_latest[4], strict=True)
    after_pending = _OBJECT_LIST.validate_python(after_latest[4], strict=True)
    assert len(after_pending) == len(before_pending) + 1
    valid_insertions: list[int] = []
    for index, candidate in enumerate(after_pending):
        audit = _OBJECT_TUPLE.validate_python(candidate, strict=True)
        if len(audit) != 3:
            continue
        task_id, channel, value = audit
        if not (
            isinstance(task_id, str)
            and task_id
            and channel == "__error__"
            and type(value) is str
            and value == "CancelledError()"
        ):
            continue
        if after_pending[:index] + after_pending[index + 1 :] == before_pending:
            valid_insertions.append(index)
    assert len(valid_insertions) == 1


def _cancel_args_metadata(error: asyncio.CancelledError) -> list[dict[str, object]]:
    current = asyncio.current_task()
    arguments: list[dict[str, object]] = []
    for argument in error.args:
        is_task = isinstance(argument, asyncio.Task)
        arguments.append(
            {
                "type": f"{type(argument).__module__}.{type(argument).__qualname__}",
                "is_task": is_task,
                "done": argument.done() if is_task else None,
                "cancelled": argument.cancelled() if is_task else None,
                "task_cancelling": argument.cancelling() if is_task else None,
                "is_current_task": argument is current if is_task else False,
                "current_task_cancelling": (
                    current.cancelling() if current is not None else None
                ),
            }
        )
    return arguments


def _trace_leaf_cancellation(
    name: str, function: Callable[_P, Awaitable[_T]]
) -> Callable[_P, Awaitable[_T]]:
    async def traced(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        try:
            return await function(*args, **kwargs)
        except asyncio.CancelledError as error:
            arguments = _cancel_args_metadata(error)
            print(
                "R202_LEAF_CANCEL_ARGS "
                + json.dumps(
                    {"function": name, "count": len(arguments), "args": arguments},
                    sort_keys=True,
                )
            )
            raise

    return traced


def _install_leaf_cancellation_trace(monkeypatch: pytest.MonkeyPatch) -> None:
    module_values = dict[str, object](vars(publish_agent_events))
    for name in (
        "_consume",
        "_consume_messages",
        "_consume_tools",
        "_consume_subagents",
        "_drain_aiter",
        "_pump_text",
        "_pump_reasoning",
    ):
        original = _ASYNC_OPERATION.validate_python(module_values[name], strict=True)
        monkeypatch.setattr(
            publish_agent_events, name, _trace_leaf_cancellation(name, original)
        )


async def _invoke_with_cancel_args_trace(operation: Awaitable[bool]) -> bool:
    try:
        return await operation
    except asyncio.CancelledError as error:
        arguments = _cancel_args_metadata(error)
        print(
            "R202_INVOKE_CANCEL_ARGS "
            + json.dumps({"count": len(arguments), "args": arguments}, sort_keys=True)
        )
        raise


async def _settle_test_task(
    task: asyncio.Task[bool] | None, primary: BaseException | None
) -> None:
    if task is None:
        return
    if not task.done():
        task.cancel()
    result = (await asyncio.gather(task, return_exceptions=True))[0]
    if isinstance(result, asyncio.CancelledError) or not isinstance(
        result, BaseException
    ):
        return
    if primary is None:
        raise result
    primary.add_note("invoke task cleanup also failed: " + type(result).__name__)


async def _settle_prepare_task(
    task: asyncio.Task[object] | None, primary: BaseException | None
) -> None:
    if task is None:
        return
    if not task.done():
        task.cancel()
    result = (await asyncio.gather(task, return_exceptions=True))[0]
    if isinstance(result, asyncio.CancelledError) or not isinstance(
        result, BaseException
    ):
        return
    if primary is None:
        raise result
    primary.add_note("prepare task cleanup also failed: " + type(result).__name__)


async def _close_test_handle(
    handle: AgentHandle, primary: BaseException | None
) -> None:
    try:
        await handle.aclose()
    except BaseException as error:
        if primary is None:
            raise
        primary.add_note("agent handle cleanup also failed: " + type(error).__name__)


async def test_r198_unpublished_billing_fails_closed_and_seals_not_dispatched(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    checkpointer: BaseCheckpointSaver[str],
    memory_store: BaseStore,
) -> None:
    request = _request()
    scope = RunScope.of(request)
    await run_repository.enqueue_dispatch(
        request, scope.namespace, f"r197:{request.run_id}"
    )
    lease = await run_repository.claim_dispatch(request, "r197-worker")
    assert lease is not None

    async with (
        _OwnerAndProviderPeer() as peer,
        SystemModelClient(peer.origin, SecretStr("r197-system-token")) as resolver,
    ):
        config = AppConfig(
            model_allow_loopback_http=True,
            model_credentials=(
                ModelCredential(
                    provider_id="10000000-0000-4000-8000-000000000002",
                    secret_handle_ref="runtime-chain-model-key",
                    api_key=SecretStr("r197-gateway-key"),
                ),
            ),
        )
        dependencies = WorkerDependencies(
            model=config.model,
            sandbox=config.sandbox,
            runtime_policy=RuntimeAssemblyPolicy.from_settings(
                run_token_budget=config.run_token_budget,
                recursion_limit=config.recursion_limit,
                model=config.model,
                sandbox=config.sandbox,
            ),
            manifest=production_manifest(),
            subagent_catalog=build_subagent_catalog(None),
            toolbox=build_toolbox(fetch_allow_private=False, search=None),
            checkpointer=checkpointer,
            run_repository=run_repository,
            model_call_repository=PostgresModelCallRepository(
                run_chat_database_url, run_chat_schema
            ),
            memory_store=memory_store,
            model_resolver=resolver,
        )
        handle = await AgentFactory(dependencies).build(request, lease, _progress)
        emitter = await RunEmitter.attach(
            FakeBus(), request.run_id, outbox=run_repository, lease=lease
        )

        async def finalize_terminal(
            payload: RunCompletedPayload | RunFailedPayload, usage: tuple[int, int]
        ) -> bool:
            await emitter.ensure_delivery_events()
            snapshot = tuple(await run_repository.list_delivery_journal(request.run_id))
            result = await run_repository.finalize_terminal(
                request.run_id,
                ExecutionTerminalAuthority(lease=lease),
                RunTerminalOutcome(
                    payload=payload,
                    usage=RunUsageSegment(
                        input_tokens=usage[0], output_tokens=usage[1]
                    ),
                ),
                snapshot,
            )
            assert result.status != "deferred"
            return result.status in {"committed", "replayed"}

        async def record_usage(
            input_tokens: int, output_tokens: int
        ) -> tuple[int, int]:
            totals = await run_repository.add_usage(
                request.run_id, lease, input_tokens, output_tokens
            )
            assert totals is not None
            return totals

        async def on_native_settled(
            _seal_usage: Callable[[], Awaitable[tuple[int, int]]],
        ) -> Literal["active", "waiting", "unknown"]:
            return "active"

        def source_for(
            _name: str,
        ) -> Literal["built-in", "config-custom", "runtime-custom"]:
            return "runtime-custom"

        try:
            terminal_claimed = await invoke_once(
                emitter,
                handle.runnable,
                scope.scoped_thread_id,
                {"messages": [HumanMessage(content="hello")]},
                initial=True,
                approval_tool_names=frozenset(),
                source_for=source_for,
                finalize_terminal=finalize_terminal,
                record_usage=record_usage,
                on_native_settled=on_native_settled,
                recursion_limit=config.recursion_limit,
            )
        finally:
            await handle.aclose()

        facts = await _lifecycle_facts(
            run_chat_database_url, run_chat_schema, request.run_id
        )
        assert {
            "terminal_claimed": terminal_claimed,
            "owner_requests": peer.owner_requests,
            "provider_requests": peer.provider_requests,
            "unknown_requests": peer.unknown_requests,
            **facts,
        } == {
            "terminal_claimed": True,
            "owner_requests": 1,
            "provider_requests": 0,
            "unknown_requests": [],
            "run": {
                "terminal": True,
                "interaction_phase": "terminal",
                "attempts": 1,
                "dispatched": 0,
                "sealed": 1,
                "unknown": 0,
                "completeness": "complete",
            },
            "attempt": {
                "status": "not_dispatched",
                "authorization_state": "not_requested",
                "dispatched": False,
                "latest_revision": 1,
            },
            "evidence": {
                "outcome": "not_dispatched",
                "usage_state": "unresolved",
                "totals_state": "unknown",
                "classification_state": "unknown",
                "sealed": True,
                "document": {
                    "actual": {"reason": "not_observed", "state": "unknown"},
                    "usage": {"reason": "missing_usage", "state": "unresolved"},
                },
            },
            "terminal": {
                "kind": "run.failed",
                "payload": {
                    "code": "dependency_unavailable",
                    "retryable": False,
                },
            },
        }


async def test_r199_prepare_lock_cancellation_propagates_and_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    checkpointer: BaseCheckpointSaver[str],
    memory_store: BaseStore,
) -> None:
    _install_leaf_cancellation_trace(monkeypatch)
    request = _request()
    scope = RunScope.of(request)
    await run_repository.enqueue_dispatch(
        request, scope.namespace, f"r199:{request.run_id}"
    )
    lease = await run_repository.claim_dispatch(request, "r199-cancel-worker")
    assert lease is not None

    async with (
        _OwnerAndProviderPeer() as peer,
        SystemModelClient(peer.origin, SecretStr("r199-system-token")) as resolver,
    ):
        config = AppConfig(
            model_allow_loopback_http=True,
            model_credentials=(
                ModelCredential(
                    provider_id="10000000-0000-4000-8000-000000000002",
                    secret_handle_ref="runtime-chain-model-key",
                    api_key=SecretStr("r199-gateway-key"),
                ),
            ),
        )
        prepare_trace = _PrepareTaskTrace()
        dependencies = WorkerDependencies(
            model=config.model,
            sandbox=config.sandbox,
            runtime_policy=RuntimeAssemblyPolicy.from_settings(
                run_token_budget=config.run_token_budget,
                recursion_limit=config.recursion_limit,
                model=config.model,
                sandbox=config.sandbox,
            ),
            manifest=production_manifest(),
            subagent_catalog=build_subagent_catalog(None),
            toolbox=build_toolbox(fetch_allow_private=False, search=None),
            checkpointer=checkpointer,
            run_repository=run_repository,
            model_call_repository=_TracingPostgresModelCallRepository(
                run_chat_database_url,
                run_chat_schema,
                prepare_trace,
                total_timeout_seconds=_PREPARE_TOTAL_TIMEOUT_SECONDS,
            ),
            memory_store=memory_store,
            model_resolver=resolver,
        )
        handle = await AgentFactory(dependencies).build(request, lease, _progress)
        invoke_task: asyncio.Task[bool] | None = None
        invoke_task_consumed = False
        checkpoint_after_cancel: tuple[object, ...] = ()
        try:
            emitter = await RunEmitter.attach(
                FakeBus(), request.run_id, outbox=run_repository, lease=lease
            )
            await emitter.emit(RunStartedPayload())
            database_before = await _database_snapshot(
                run_chat_database_url, run_chat_schema, request.run_id
            )

            async def unexpected_finalize(
                _payload: RunCompletedPayload | RunFailedPayload,
                _usage: tuple[int, int],
            ) -> bool:
                raise AssertionError("local cancellation must not finalize the Run")

            async def record_usage(
                input_tokens: int, output_tokens: int
            ) -> tuple[int, int]:
                totals = await run_repository.add_usage(
                    request.run_id, lease, input_tokens, output_tokens
                )
                assert totals is not None
                return totals

            async def on_native_settled(
                _seal_usage: Callable[[], Awaitable[tuple[int, int]]],
            ) -> Literal["active", "waiting", "unknown"]:
                return "active"

            async with connect_pg(run_chat_database_url) as blocker:
                async with blocker.transaction():
                    async with blocker.cursor() as cursor:
                        await execute_sql(cursor, "SELECT pg_backend_pid() AS pid")
                        blocker_row = await fetch_one(cursor)
                        assert blocker_row is not None
                        blocker_pid = int(blocker_row["pid"])
                        await execute_sql(
                            cursor,
                            f"SELECT 1 FROM {qualified(run_chat_schema, 'kokoro_agent_run')} "
                            "WHERE run_id=%s AND tenant_id=%s FOR UPDATE",
                            (request.run_id, request.execution_identity.tenant_ref),
                        )

                        invoke_task = asyncio.create_task(
                            _invoke_with_cancel_args_trace(
                                invoke_once(
                                    emitter,
                                    handle.runnable,
                                    scope.scoped_thread_id,
                                    {"messages": [HumanMessage(content="hello")]},
                                    initial=False,
                                    approval_tool_names=frozenset(),
                                    source_for=lambda _name: "runtime-custom",
                                    finalize_terminal=unexpected_finalize,
                                    record_usage=record_usage,
                                    on_native_settled=on_native_settled,
                                    recursion_limit=config.recursion_limit,
                                )
                            )
                        )
                        try:
                            waiter_pid: int | None = None
                            async with connect_pg(run_chat_database_url) as observer:
                                await observer.set_autocommit(True)
                                async with observer.cursor() as observer_cursor:
                                    async with asyncio.timeout(5):
                                        while waiter_pid is None:
                                            await execute_sql(
                                                observer_cursor,
                                                "SELECT pg_stat_clear_snapshot()",
                                            )
                                            await execute_sql(
                                                observer_cursor,
                                                """SELECT pid,pg_blocking_pids(pid) AS blockers
                                                   FROM pg_stat_activity
                                                   WHERE datname=current_database()
                                                     AND state='active' AND wait_event_type='Lock'
                                                     AND query LIKE %s
                                                     AND query LIKE %s
                                                     AND query LIKE %s""",
                                                (
                                                    "%SELECT tenant_id,request_json,owner,lease_generation,lease_expires_at,%",
                                                    "%assembly_recipe_fingerprint%",
                                                    "%FOR UPDATE%",
                                                ),
                                            )
                                            rows = await observer_cursor.fetchall()
                                            for row in rows:
                                                blockers = tuple(
                                                    int(value)
                                                    for value in row["blockers"]
                                                )
                                                if blocker_pid in blockers:
                                                    waiter_pid = int(row["pid"])
                                                    break
                                            await asyncio.sleep(0)
                            assert waiter_pid is not None
                            assert prepare_trace.entered.is_set()
                            _emit_prepare_trace(
                                "owned-lock", prepare_trace, invoke_task
                            )
                            checkpoint_at_wait = await _checkpoint_snapshot(
                                checkpointer, scope.scoped_thread_id
                            )
                            invoke_task.cancel()
                            invoke_result = (
                                await asyncio.gather(
                                    invoke_task, return_exceptions=True
                                )
                            )[0]
                            invoke_task_consumed = True
                            assert isinstance(invoke_result, asyncio.CancelledError)
                            _emit_prepare_trace(
                                "cancel-before-release", prepare_trace, invoke_task
                            )
                            checkpoint_after_cancel = await _checkpoint_snapshot(
                                checkpointer, scope.scoped_thread_id
                            )
                            _emit_cancelled_error_deepcopy_probe()
                            _emit_checkpoint_delta(
                                "wait-to-cancelled",
                                checkpoint_at_wait,
                                checkpoint_after_cancel,
                            )
                        finally:
                            if not invoke_task_consumed:
                                await _settle_test_task(invoke_task, sys.exception())
                                invoke_task_consumed = True

            assert invoke_task is not None
            async with asyncio.timeout(_PREPARE_TOTAL_TIMEOUT_SECONDS):
                await prepare_trace.finished.wait()
            _emit_prepare_trace("after-release", prepare_trace, invoke_task)
            assert peer.owner_requests == 1
            assert peer.provider_requests == 0
            assert peer.unknown_requests == []
            assert (
                await _database_snapshot(
                    run_chat_database_url, run_chat_schema, request.run_id
                )
                == database_before
            )
            final_checkpoint = await _checkpoint_snapshot(
                checkpointer, scope.scoped_thread_id
            )
            _emit_checkpoint_delta(
                "cancelled-to-released", checkpoint_after_cancel, final_checkpoint
            )
            _assert_exact_single_cancellation_audit(
                checkpoint_at_wait, checkpoint_after_cancel
            )
            assert final_checkpoint == checkpoint_after_cancel
            assert prepare_trace.outcome == "cancelled"
        finally:
            primary = sys.exception()
            task_error: BaseException | None = None
            try:
                if not invoke_task_consumed:
                    await _settle_test_task(invoke_task, primary)
                    invoke_task_consumed = True
                await _settle_prepare_task(prepare_trace.task, primary)
            except BaseException as error:
                task_error = error
            await _close_test_handle(handle, primary or task_error)
            if primary is None and task_error is not None:
                raise task_error


async def test_r199_terminal_supervisor_recovery_is_byte_stable_and_zero_write(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    checkpointer: BaseCheckpointSaver[str],
    memory_store: BaseStore,
) -> None:
    request = _request()
    scope = RunScope.of(request)
    await run_repository.enqueue_dispatch(
        request, scope.namespace, f"r199:{request.run_id}"
    )
    lease = await run_repository.claim_dispatch(request, "r199-terminal-worker")
    assert lease is not None

    async with (
        _OwnerAndProviderPeer() as peer,
        SystemModelClient(peer.origin, SecretStr("r199-system-token")) as resolver,
    ):
        config = AppConfig(
            model_allow_loopback_http=True,
            model_credentials=(
                ModelCredential(
                    provider_id="10000000-0000-4000-8000-000000000002",
                    secret_handle_ref="runtime-chain-model-key",
                    api_key=SecretStr("r199-gateway-key"),
                ),
            ),
        )
        dependencies = WorkerDependencies(
            model=config.model,
            sandbox=config.sandbox,
            runtime_policy=RuntimeAssemblyPolicy.from_settings(
                run_token_budget=config.run_token_budget,
                recursion_limit=config.recursion_limit,
                model=config.model,
                sandbox=config.sandbox,
            ),
            manifest=production_manifest(),
            subagent_catalog=build_subagent_catalog(None),
            toolbox=build_toolbox(fetch_allow_private=False, search=None),
            checkpointer=checkpointer,
            run_repository=run_repository,
            model_call_repository=PostgresModelCallRepository(
                run_chat_database_url, run_chat_schema
            ),
            memory_store=memory_store,
            model_resolver=resolver,
        )
        handle = await AgentFactory(dependencies).build(request, lease, _progress)
        terminal_bus = FakeBus()
        emitter = await RunEmitter.attach(
            terminal_bus, request.run_id, outbox=run_repository, lease=lease
        )

        async def finalize_terminal(
            payload: RunCompletedPayload | RunFailedPayload, usage: tuple[int, int]
        ) -> bool:
            await emitter.ensure_delivery_events()
            snapshot = tuple(await run_repository.list_delivery_journal(request.run_id))
            result = await run_repository.finalize_terminal(
                request.run_id,
                ExecutionTerminalAuthority(lease=lease),
                RunTerminalOutcome(
                    payload=payload,
                    usage=RunUsageSegment(
                        input_tokens=usage[0], output_tokens=usage[1]
                    ),
                ),
                snapshot,
            )
            return result.status in {"committed", "replayed"}

        async def record_usage(
            input_tokens: int, output_tokens: int
        ) -> tuple[int, int]:
            totals = await run_repository.add_usage(
                request.run_id, lease, input_tokens, output_tokens
            )
            assert totals is not None
            return totals

        async def on_native_settled(
            _seal_usage: Callable[[], Awaitable[tuple[int, int]]],
        ) -> Literal["active", "waiting", "unknown"]:
            return "active"

        try:
            assert await invoke_once(
                emitter,
                handle.runnable,
                scope.scoped_thread_id,
                {"messages": [HumanMessage(content="hello")]},
                initial=True,
                approval_tool_names=frozenset(),
                source_for=lambda _name: "runtime-custom",
                finalize_terminal=finalize_terminal,
                record_usage=record_usage,
                on_native_settled=on_native_settled,
                recursion_limit=config.recursion_limit,
            )
        finally:
            await handle.aclose()

        async def forbidden_build(
            _request: RunRequest, _lease: LeaseFence, _progress: SkillProgressSink
        ) -> AgentHandle:
            raise AssertionError("terminal recovery must not rebuild the Agent")

        async def forbidden_interaction(
            *,
            request: RunRequest,
            lease: LeaseFence,
            handle: AgentHandle,
            target: PauseReadTarget | ResumeReadTarget,
        ) -> Never:
            del request, lease, handle, target
            raise AssertionError("terminal recovery must not inspect native state")

        def recovery_supervisor() -> RunSupervisor:
            return RunSupervisor(
                agent_builder=forbidden_build,
                interaction_reader=forbidden_interaction,
                run_repository=run_repository,
                approval_tool_names=lambda _request: frozenset(),
                trace_factory=lambda _request: None,
                source_for=lambda _name: "runtime-custom",
                consumer="r199-recovery-worker",
            )

        # First public startup publishes the already committed terminal outbox.
        # The duplicate recovery below is compared with that stable baseline.
        await recovery_supervisor().serve(FakeBus())
        database_before = await _database_snapshot(
            run_chat_database_url, run_chat_schema, request.run_id
        )
        checkpoint_before = await _checkpoint_snapshot(
            checkpointer, scope.scoped_thread_id
        )
        owner_requests_before = peer.owner_requests

        recovery_bus = FakeBus(
            inbound=(
                StreamItem(
                    cursor="r199-terminal-duplicate",
                    event=request.model_dump(mode="json"),
                ),
            )
        )
        await recovery_supervisor().serve(recovery_bus)

        assert peer.provider_requests == 0
        assert peer.owner_requests == owner_requests_before
        assert (
            await _database_snapshot(
                run_chat_database_url, run_chat_schema, request.run_id
            )
            == database_before
        )
        assert (
            await _checkpoint_snapshot(checkpointer, scope.scoped_thread_id)
            == checkpoint_before
        )
        assert recovery_bus.acked == ["r199-terminal-duplicate"]


async def test_r245_native_final_request_precedes_prepare_without_billing(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    checkpointer: BaseCheckpointSaver[str],
    memory_store: BaseStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx

    from kokoro_agent.model.factory import ModelHttpClients

    sequence: list[str] = []
    final_requests: list[tuple[str, str, bytes, asyncio.Task[object]]] = []
    trace = _PrepareTaskTrace()
    original_validator: Callable[[ModelHttpClients, httpx.Request], Awaitable[None]]
    original_validator = TypeAdapter[
        Callable[[ModelHttpClients, httpx.Request], Awaitable[None]]
    ](Callable[[ModelHttpClients, httpx.Request], Awaitable[None]]).validate_python(
        vars(ModelHttpClients)["_validate_async"], strict=True
    )

    async def observe_final_request(
        clients: ModelHttpClients, final_request: httpx.Request
    ) -> None:
        task = asyncio.current_task()
        assert task is not None
        sequence.append("final_hook_enter")
        final_requests.append(
            (
                final_request.method,
                str(final_request.url),
                final_request.content,
                task,
            )
        )
        await original_validator(clients, final_request)

    class OrderedPostgresModelCallRepository(_TracingPostgresModelCallRepository):
        async def prepare(self, command: PrepareModelCall) -> PreparedModelCall:
            sequence.append("prepare_enter")
            receipt = await super().prepare(command)
            sequence.append("prepare_committed")
            return receipt

    # Observe the production hook in place; keep its sole registration and behavior.
    monkeypatch.setattr(ModelHttpClients, "_validate_async", observe_final_request)
    request = _request()
    scope = RunScope.of(request)
    await run_repository.enqueue_dispatch(
        request, scope.namespace, f"r197:{request.run_id}"
    )
    lease = await run_repository.claim_dispatch(request, "r197-worker")
    assert lease is not None

    async with (
        _OwnerAndProviderPeer() as peer,
        SystemModelClient(peer.origin, SecretStr("r197-system-token")) as resolver,
    ):
        config = AppConfig(
            model_allow_loopback_http=True,
            model_credentials=(
                ModelCredential(
                    provider_id="10000000-0000-4000-8000-000000000002",
                    secret_handle_ref="runtime-chain-model-key",
                    api_key=SecretStr("r197-gateway-key"),
                ),
            ),
        )
        dependencies = WorkerDependencies(
            model=config.model,
            sandbox=config.sandbox,
            runtime_policy=RuntimeAssemblyPolicy.from_settings(
                run_token_budget=config.run_token_budget,
                recursion_limit=config.recursion_limit,
                model=config.model,
                sandbox=config.sandbox,
            ),
            manifest=production_manifest(),
            subagent_catalog=build_subagent_catalog(None),
            toolbox=build_toolbox(fetch_allow_private=False, search=None),
            checkpointer=checkpointer,
            run_repository=run_repository,
            model_call_repository=OrderedPostgresModelCallRepository(
                run_chat_database_url,
                run_chat_schema,
                trace,
                total_timeout_seconds=_PREPARE_TOTAL_TIMEOUT_SECONDS,
            ),
            memory_store=memory_store,
            model_resolver=resolver,
        )
        handle = await AgentFactory(dependencies).build(request, lease, _progress)
        emitter = await RunEmitter.attach(
            FakeBus(), request.run_id, outbox=run_repository, lease=lease
        )

        async def finalize_terminal(
            payload: RunCompletedPayload | RunFailedPayload, usage: tuple[int, int]
        ) -> bool:
            await emitter.ensure_delivery_events()
            snapshot = tuple(await run_repository.list_delivery_journal(request.run_id))
            result = await run_repository.finalize_terminal(
                request.run_id,
                ExecutionTerminalAuthority(lease=lease),
                RunTerminalOutcome(
                    payload=payload,
                    usage=RunUsageSegment(
                        input_tokens=usage[0], output_tokens=usage[1]
                    ),
                ),
                snapshot,
            )
            assert result.status != "deferred"
            return result.status in {"committed", "replayed"}

        async def record_usage(
            input_tokens: int, output_tokens: int
        ) -> tuple[int, int]:
            totals = await run_repository.add_usage(
                request.run_id, lease, input_tokens, output_tokens
            )
            assert totals is not None
            return totals

        async def on_native_settled(
            _seal_usage: Callable[[], Awaitable[tuple[int, int]]],
        ) -> Literal["active", "waiting", "unknown"]:
            return "active"

        def source_for(
            _name: str,
        ) -> Literal["built-in", "config-custom", "runtime-custom"]:
            return "runtime-custom"

        try:
            terminal_claimed = await invoke_once(
                emitter,
                handle.runnable,
                scope.scoped_thread_id,
                {"messages": [HumanMessage(content="hello")]},
                initial=True,
                approval_tool_names=frozenset(),
                source_for=source_for,
                finalize_terminal=finalize_terminal,
                record_usage=record_usage,
                on_native_settled=on_native_settled,
                recursion_limit=config.recursion_limit,
            )
        finally:
            await handle.aclose()

        facts = await _lifecycle_facts(
            run_chat_database_url, run_chat_schema, request.run_id
        )
        assert {
            "terminal_claimed": terminal_claimed,
            "owner_requests": peer.owner_requests,
            "provider_requests": peer.provider_requests,
            "unknown_requests": peer.unknown_requests,
            **facts,
        } == {
            "terminal_claimed": True,
            "owner_requests": 1,
            "provider_requests": 0,
            "unknown_requests": [],
            "run": {
                "terminal": True,
                "interaction_phase": "terminal",
                "attempts": 1,
                "dispatched": 0,
                "sealed": 1,
                "unknown": 0,
                "completeness": "complete",
            },
            "attempt": {
                "status": "not_dispatched",
                "authorization_state": "not_requested",
                "dispatched": False,
                "latest_revision": 1,
            },
            "evidence": {
                "outcome": "not_dispatched",
                "usage_state": "unresolved",
                "totals_state": "unknown",
                "classification_state": "unknown",
                "sealed": True,
                "document": {
                    "actual": {"reason": "not_observed", "state": "unknown"},
                    "usage": {"reason": "missing_usage", "state": "unresolved"},
                },
            },
            "terminal": {
                "kind": "run.failed",
                "payload": {
                    "code": "dependency_unavailable",
                    "retryable": False,
                },
            },
        }

        assert trace.entered.is_set()
        assert trace.finished.is_set()
        assert trace.outcome == "committed"

        # Billing remains unavailable: only the freeze/prepare order should change.
        assert sequence == ["final_hook_enter", "prepare_enter", "prepare_committed"]
        assert len(final_requests) == 1
        method, url, raw, hook_task = final_requests[0]
        assert method == "POST"
        assert url == f"{peer.origin}/v1/chat/completions"
        assert hook_task is trace.task
        body = _JSON_OBJECT.validate_json(raw, strict=True)
        assert body["model"] == "gateway-model-r197"
        assert body["n"] == 1
        assert body["max_completion_tokens"] == 256
        messages = _OBJECT_LIST.validate_python(body["messages"], strict=True)
        assert any(
            message.get("role") == "user" and message.get("content") == "hello"
            for message in (
                _JSON_OBJECT.validate_python(value, strict=True) for value in messages
            )
        )
        from kokoro_agent.model.call_artifacts import encode_final_request_digest

        async with (
            connect_pg(run_chat_database_url) as connection,
            connection.cursor() as cursor,
        ):
            await execute_sql(
                cursor,
                f"""SELECT final_request_binding_version,final_request_digest,
                           planned_digest,attempt_binding_bytes
                    FROM {qualified(run_chat_schema, "kokoro_agent_model_call_attempt")}
                    WHERE run_id=%s""",
                (request.run_id,),
            )
            stored = await fetch_one(cursor)
            assert stored is not None
        assert stored["final_request_binding_version"] == 1
        assert stored["final_request_digest"] == encode_final_request_digest(
            endpoint=url, planned_digest=str(stored["planned_digest"]), raw=raw
        )
        binding_bytes = stored["attempt_binding_bytes"]
        assert isinstance(binding_bytes, bytes)
        binding = _JSON_OBJECT.validate_json(binding_bytes, strict=True)
        import rfc8785

        assert (
            binding["input_digest"]
            == hashlib.sha256(
                rfc8785.dumps(request.input.model_dump(mode="json"))
            ).hexdigest()
        )


@pytest.mark.parametrize(
    "mode",
    [
        "sdk_child",
        "swallowed_denial",
        "unrelated_error",
        "two_claimants",
        "late_child",
        "owner_cancel",
        "invalid_profile",
        "materialization_failure",
        "no_request",
        "replay_drift",
    ],
)
async def test_r246_final_request_lifecycle_real_sdk_and_pg(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    checkpointer: BaseCheckpointSaver[str],
    memory_store: BaseStore,
    mode: str,
) -> None:
    """Supplemental lifecycle seam; the original native graph case stays separate."""
    from contextlib import AsyncExitStack
    from openai import APIConnectionError
    from langchain.agents.middleware import ModelRequest, ModelResponse
    from langgraph.runtime import ExecutionInfo, Runtime
    from kokoro_agent.domain.run.models import StaticRecipeBinding
    from kokoro_agent.model.call_models import (
        ModelCallAuthorizationUnavailable,
        ModelCallInputInvalid,
    )
    from kokoro_agent.model.call_runtime import ModelCallRuntimeMiddleware
    from kokoro_agent.model.call_scope import ModelCallScope, bind_model_call_scope
    from kokoro_agent.model.factory import (
        ModelHttpClients,
        make_chat_model,
        model_from_route,
    )
    from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding

    request = _request()
    run_scope = RunScope.of(request)
    await run_repository.enqueue_dispatch(
        request, run_scope.namespace, f"r246:{request.run_id}"
    )
    lease = await run_repository.claim_dispatch(request, "r246-worker")
    assert lease is not None
    trace = _PrepareTaskTrace()
    repository = _TracingPostgresModelCallRepository(
        run_chat_database_url,
        run_chat_schema,
        trace,
        total_timeout_seconds=_PREPARE_TOTAL_TIMEOUT_SECONDS,
    )
    async with AsyncExitStack() as stack:
        peer = await stack.enter_async_context(_OwnerAndProviderPeer())
        resolver = await stack.enter_async_context(
            SystemModelClient(peer.origin, SecretStr("r246-system-qa"))
        )
        config = AppConfig(
            model_allow_loopback_http=True,
            model_credentials=(
                ModelCredential(
                    provider_id="10000000-0000-4000-8000-000000000002",
                    secret_handle_ref="runtime-chain-model-key",
                    api_key=SecretStr("r246-synthetic-key"),
                ),
            ),
        )
        dependencies = WorkerDependencies(
            model=config.model,
            sandbox=config.sandbox,
            runtime_policy=RuntimeAssemblyPolicy.from_settings(
                run_token_budget=config.run_token_budget,
                recursion_limit=config.recursion_limit,
                model=config.model,
                sandbox=config.sandbox,
            ),
            manifest=production_manifest(),
            subagent_catalog=build_subagent_catalog(None),
            toolbox=build_toolbox(fetch_allow_private=False, search=None),
            checkpointer=checkpointer,
            run_repository=run_repository,
            model_call_repository=repository,
            memory_store=memory_store,
            model_resolver=resolver,
        )
        handle = await AgentFactory(dependencies).build(request, lease, _progress)
        stack.push_async_callback(handle.aclose)
        route = await resolver.resolve(
            tenant_id=request.execution_identity.tenant_ref,
            feature_key=request.feature_key,
            label=request.requested_model_label,
            request_id="r246-lifecycle-route",
        )
        planned = PlannedBinding(
            binding_version="2.0.0",
            model=route.model_id,
            provider=route.provider_id,
            revision_id=route.revision_id,
            provider_model_name=route.provider_model_name,
            request_model=route.request_model,
            feature_key=route.feature_key,
            revision=str(route.revision),
            generation=route.generation,
            tenant_generation=route.tenant_generation,
            digest=route.digest,
            label=route.label_key,
        )
        async with (
            connect_pg(run_chat_database_url) as connection,
            connection.cursor() as cursor,
        ):
            await execute_sql(
                cursor,
                f"SELECT assembly_recipe_bytes,assembly_recipe_fingerprint FROM {qualified(run_chat_schema, 'kokoro_agent_run')} WHERE run_id=%s",
                (request.run_id,),
            )
            row = await fetch_one(cursor)
            assert row is not None
        raw_recipe = row["assembly_recipe_bytes"]
        assert isinstance(raw_recipe, bytes)
        recipe = StaticRecipeBinding(
            canonical_bytes=raw_recipe,
            fingerprint=str(row["assembly_recipe_fingerprint"]),
        )
        frozen_recipe = _JSON_OBJECT.validate_json(raw_recipe, strict=True)
        feature = _JSON_OBJECT.validate_python(frozen_recipe["feature"], strict=True)
        entry_agent = feature["entry_agent"]
        assert isinstance(entry_agent, str) and entry_agent
        clients = ModelHttpClients(config.model, route)
        stack.push_async_callback(clients.aclose)
        model = make_chat_model(
            config.model, model_from_route(route, None), model_clients=clients
        )
        middleware = ModelCallRuntimeMiddleware(
            repository=repository,
            request=request,
            lease=lease,
            planned=planned,
            recipe=recipe,
            peer_path=(entry_agent,),
        )
        model_request = ModelRequest(
            model=model,
            messages=[HumanMessage(content="r246-final-message")],
            runtime=Runtime(
                execution_info=ExecutionInfo(
                    checkpoint_id=str(uuid4()),
                    checkpoint_ns="",
                    task_id=str(uuid4()),
                )
            ),
        )
        if mode in {"two_claimants", "owner_cancel"}:
            blocker = await stack.enter_async_context(connect_pg(run_chat_database_url))
            await stack.enter_async_context(blocker.transaction())
            cursor = await stack.enter_async_context(blocker.cursor())
            await execute_sql(
                cursor,
                f"SELECT run_id FROM {qualified(run_chat_schema, 'kokoro_agent_run')} WHERE run_id=%s FOR UPDATE",
                (request.run_id,),
            )
        late_release = asyncio.Event()
        late_tasks: list[asyncio.Task[object]] = []
        unrelated = RuntimeError("r246-unrelated-handler-error")
        sdk_failures: list[Exception] = []

        async def handler(current: ModelRequest) -> ModelResponse:
            if mode == "no_request":
                return ModelResponse(result=[])
            if mode == "invalid_profile":
                await model.bind(extra_body={"n": 2}).ainvoke(current.messages)
                pytest.fail("invalid final profile returned a response")
            if mode == "materialization_failure":
                await model.bind(extra_body={"invalid_json": object()}).ainvoke(
                    current.messages
                )
                pytest.fail("non-JSON SDK payload returned a response")
            if mode == "late_child":

                async def late() -> object:
                    await late_release.wait()
                    return await model.ainvoke(current.messages)

                late_tasks.append(asyncio.create_task(late()))
                return ModelResponse(result=[])
            if mode == "two_claimants":
                first = asyncio.create_task(model.ainvoke(current.messages))
                await trace.entered.wait()
                second = asyncio.create_task(model.ainvoke(current.messages))
                await asyncio.gather(first, second, return_exceptions=True)
                return ModelResponse(result=[])
            try:
                response = await model.ainvoke(current.messages)
            except Exception as error:
                sdk_failures.append(error)
                if mode == "swallowed_denial":
                    return ModelResponse(result=[])
                if mode == "unrelated_error":
                    raise unrelated from None
                raise
            return ModelResponse(result=[response])

        invocation = ModelCallScope(timeout_s=5.0)
        failure: BaseException | None = None
        with bind_model_call_scope(invocation):
            owner = asyncio.create_task(
                middleware.awrap_model_call(model_request, handler)
            )
            try:
                async with asyncio.timeout(5.0):
                    if mode == "owner_cancel":
                        await trace.entered.wait()
                        owner.cancel()
                    await owner
            except BaseException as error:
                failure = error
            finally:
                if not owner.done():
                    owner.cancel()
                await asyncio.gather(owner, return_exceptions=True)
                await invocation.settle(cancel_active=True, primary=failure)
                late_release.set()
                late_results = await asyncio.gather(*late_tasks, return_exceptions=True)
        assert peer.provider_requests == 0
        assert peer.unknown_requests == []
        if mode == "unrelated_error":
            assert failure is unrelated
        elif mode in {"two_claimants", "invalid_profile"}:
            assert isinstance(failure, ModelCallInputInvalid), {
                "failure_type": type(failure).__name__,
                "prepare_outcome": trace.outcome,
                "prepare_finished": trace.finished.is_set(),
                "prepare_task": None
                if trace.task is None
                else _task_snapshot(trace.task),
            }
        elif mode == "materialization_failure":
            assert isinstance(failure, (TypeError, ValueError))
            assert not isinstance(failure, ModelCallInputInvalid)
        elif mode == "owner_cancel":
            assert isinstance(failure, asyncio.CancelledError)
        else:
            assert isinstance(failure, ModelCallAuthorizationUnavailable)
        if mode in {"invalid_profile", "materialization_failure", "no_request"}:
            assert not trace.entered.is_set()
        elif mode == "late_child":
            assert not trace.entered.is_set()
            assert len(late_results) == 1
            assert isinstance(late_results[0], APIConnectionError)
            assert isinstance(
                late_results[0].__cause__, ModelCallAuthorizationUnavailable
            )
        else:
            assert trace.entered.is_set() and trace.finished.is_set()
            assert trace.task is not None and trace.task.done()
            assert (
                trace.task is not owner
            )  # normal ainvoke uses locked SDK gather child
        expected_attempts = (
            0
            if mode
            in {
                "late_child",
                "two_claimants",
                "owner_cancel",
                "invalid_profile",
                "materialization_failure",
                "no_request",
            }
            else 1
        )
        async with (
            connect_pg(run_chat_database_url) as connection,
            connection.cursor() as cursor,
        ):
            await execute_sql(
                cursor,
                f"SELECT count(*) AS total FROM {qualified(run_chat_schema, 'kokoro_agent_model_call_attempt')} WHERE run_id=%s",
                (request.run_id,),
            )
            assert await fetch_one(cursor) == {"total": expected_attempts}
        if sdk_failures and mode != "unrelated_error":
            assert sdk_failures[0].__cause__ is failure

        if mode == "replay_drift":
            from kokoro_agent.model.call_models import ModelCallIdentityConflict

            async def persisted() -> dict[str, object]:
                async with (
                    connect_pg(run_chat_database_url) as connection,
                    connection.cursor() as cursor,
                ):
                    await execute_sql(
                        cursor,
                        f"SELECT * FROM {qualified(run_chat_schema, 'kokoro_agent_model_call_attempt')} WHERE run_id=%s",
                        (request.run_id,),
                    )
                    result = await fetch_one(cursor)
                    assert result is not None
                    return dict(result)

            before = await persisted()
            for changed in (True, False):
                current = (
                    model_request.override(
                        messages=[HumanMessage(content="different final SDK body")]
                    )
                    if changed
                    else model_request
                )
                next_scope = ModelCallScope(timeout_s=5.0)
                next_error: BaseException | None = None
                with bind_model_call_scope(next_scope):
                    next_owner = asyncio.create_task(
                        middleware.awrap_model_call(current, handler)
                    )
                    try:
                        await next_owner
                    except BaseException as error:
                        next_error = error
                    finally:
                        await next_scope.settle(cancel_active=True, primary=next_error)
                assert isinstance(
                    next_error,
                    ModelCallIdentityConflict
                    if changed
                    else ModelCallAuthorizationUnavailable,
                )
                assert await persisted() == before
                assert peer.provider_requests == 0
