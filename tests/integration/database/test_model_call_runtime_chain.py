"""System gate for the real native model path before Billing is published."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal
from uuid import uuid4

from langchain_core.messages import HumanMessage
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore
from pydantic import SecretStr, TypeAdapter

from kokoro_agent.agent_factory import AgentFactory
from kokoro_agent.agents.subagent_catalog import build_subagent_catalog
from kokoro_agent.clients.system import SystemModelClient
from kokoro_agent.config import AppConfig
from kokoro_agent.domain.run.models import (
    ExecutionTerminalAuthority,
    RunTerminalOutcome,
    RunUsageSegment,
)
from kokoro_agent.domain.run.repository import RunRepository
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.execution.runtime_profile_plan import RuntimeAssemblyPolicy
from kokoro_agent.execution.runtime_profile_sources import production_manifest
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_model_call_repository import (
    PostgresModelCallRepository,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one
from kokoro_agent.protocol import (
    ExecutionIdentity,
    IdentityRef,
    RunInput,
    RunCompletedPayload,
    RunFailedPayload,
    RunRequest,
    SkillPhase,
    SkillProgressCommitted,
)
from kokoro_agent.tools.toolbox import build_toolbox
from kokoro_agent.worker.dependencies import WorkerDependencies
from support.fakes import FakeBus

_JSON_OBJECT = TypeAdapter(dict[str, object])


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
                body = _system_route_body()
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


def _system_route_body() -> bytes:
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
                "gateway_model_name": "gateway-model-r197",
                "feature_key": "chat",
                "label_key": "default",
                "transport": "litellm",
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
        config = AppConfig.from_env(
            {
                "KOKORO_LITELLM_ENABLED": "1",
                "KOKORO_LITELLM_BASE_URL": f"{peer.origin}/v1",
                "KOKORO_LITELLM_API_KEY": "r197-gateway-key",
            }
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
