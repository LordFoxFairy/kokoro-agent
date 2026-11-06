"""Locked SDK + production gates; owned loopback and injected low-level faults only.

The journal/financial peers are named doubles, not PostgreSQL or real Billing.
HTTPX transport and httpcore retry implementation are not replaced.
"""

from __future__ import annotations

import asyncio
import json
import ssl
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Literal
from unittest.mock import Mock

import httpcore
import httpx
import pytest
from httpcore._backends.anyio import AnyIOBackend
from langchain.agents.middleware import ModelRequest, ModelResponse
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.runtime import ExecutionInfo, Runtime
from openai import APIConnectionError, APIStatusError
from pydantic import SecretStr

import kokoro_agent.model.call_runtime as call_runtime
from kokoro_agent.clients.system import ModelConnection, ResolvedModel
from kokoro_agent.config import AppConfig
from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
from kokoro_agent.model.call_artifacts import (
    encode_attempt_binding,
    encode_planned_artifact,
)
from kokoro_agent.model.call_models import (
    ModelCallAuthorizationReceipt,
    ModelCallDispatchPermit,
    ModelCallEvidenceReceipt,
    ModelCallRepository,
    PrepareModelCall,
    PreparedModelCall,
    SealModelCallObservation,
    StartModelCallDispatch,
)
from kokoro_agent.model.call_runtime import ModelCallRuntimeMiddleware
from kokoro_agent.model.call_scope import ModelCallScope, bind_model_call_scope
from kokoro_agent.model.credentials import ModelCredential
from kokoro_agent.model.execution import (
    CallAuthorizer,
    CallFinancialDelivery,
    ModelCallExecutor,
)
from kokoro_agent.model.factory import (
    ModelHttpClients,
    make_chat_model,
    model_from_route,
)
from kokoro_agent.model.settlement import ModelSettlementReceipt
from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding
from support.fakes import request as run_request


@dataclass
class _Peer:
    status: int = 200
    hang: bool = False
    requests: list[bytes] = field(default_factory=lambda: list[bytes]())
    release: asyncio.Event = field(default_factory=asyncio.Event)
    entered: asyncio.Event = field(default_factory=asyncio.Event)

    async def handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            length = next(
                int(line.split(b":", 1)[1])
                for line in headers.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            self.requests.append(await reader.readexactly(length))
            self.entered.set()
            if self.hang:
                await self.release.wait()
                return
            payload: dict[str, object] = {
                "id": "chatcmpl-network-fixture",
                "object": "chat.completion",
                "created": 1,
                "model": "network-fixture",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "ok"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
                    "prompt_tokens_details": {"cached_tokens": 0},
                    "completion_tokens_details": {"reasoning_tokens": 0},
                },
            }
            body = json.dumps(payload).encode()
            content_type = "application/json"
            if json.loads(self.requests[-1]).get("stream"):
                content_type = "text/event-stream"
                chunks: list[dict[str, object]] = [
                    {
                        **payload,
                        "choices": [
                            {
                                "index": 0,
                                "delta": {"content": "ok"},
                                "finish_reason": None,
                            }
                        ],
                        "usage": None,
                    },
                    {
                        **payload,
                        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                        "usage": None,
                    },
                    {**payload, "choices": []},
                ]
                body = (
                    b"".join(
                        b"data: " + json.dumps(chunk).encode() + b"\n\n"
                        for chunk in chunks
                    )
                    + b"data: [DONE]\n\n"
                )
            writer.write(
                f"HTTP/1.1 {self.status} Result\r\nContent-Type: {content_type}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()


class _Backend(httpcore.AsyncNetworkBackend):
    def __init__(self, mode: str = "ok") -> None:
        self.mode = mode
        self.calls = 0
        self.entered = asyncio.Event()
        self.timeouts: list[float | None] = []
        self.tls_timeouts: list[float | None] = []
        self.streams: list[_Stream] = []
        self.retrying = asyncio.Event()
        self.inner = AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        self.calls += 1
        self.timeouts.append(timeout)
        self.entered.set()
        if self.mode in {"connect", "connect_pause"} and self.calls == 1:
            raise httpcore.ConnectError("injected pre-TCP failure")
        if self.mode == "always_connect":
            raise httpcore.ConnectTimeout("injected connect timeout")
        if self.mode == "connect_hang":
            await asyncio.Event().wait()
        stream = await self.inner.connect_tcp(
            host, port, timeout, local_address, socket_options
        )
        mode = (
            ("tls_first" if self.calls == 1 else "tls_ok")
            if self.mode == "tls"
            else self.mode
        )
        wrapped = _Stream(stream, mode, self.tls_timeouts)
        self.streams.append(wrapped)
        return wrapped

    async def sleep(self, seconds: float) -> None:
        if self.mode == "connect_pause":
            self.retrying.set()
            await asyncio.Event().wait()
        await asyncio.sleep(seconds)


class _Stream(httpcore.AsyncNetworkStream):
    def __init__(
        self,
        inner: httpcore.AsyncNetworkStream,
        mode: str,
        tls_timeouts: list[float | None],
    ) -> None:
        self.inner, self.mode = inner, mode
        self.tls_timeouts = tls_timeouts
        self.closed = False

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self.mode == "read":
            raise httpcore.ReadError("injected post-send read error")
        if self.mode == "protocol":
            return b"not HTTP\r\n\r\n"
        return await self.inner.read(max_bytes, timeout)

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        await self.inner.write(buffer, timeout)
        if self.mode == "write":
            await self.inner.aclose()
            raise httpcore.WriteError("injected error after first write")

    async def aclose(self) -> None:
        self.closed = True
        await self.inner.aclose()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> httpcore.AsyncNetworkStream:
        self.tls_timeouts.append(timeout)
        assert (
            ssl_context.verify_mode == ssl.CERT_REQUIRED and ssl_context.check_hostname
        )
        # Named TLS-boundary injection; loopback remains plaintext, not a real TLS handshake.
        if self.mode == "tls_first":
            await self.aclose()
            raise httpcore.ConnectError("injected TLS establishment failure")
        if self.mode == "tls_ok":
            return self
        return await self.inner.start_tls(ssl_context, server_hostname, timeout)


class _Journal:
    """Typed receipts behind production ModelCallExecutor, no durable I/O."""

    def __init__(self) -> None:
        self.repository = Mock(spec=ModelCallRepository)
        self.authorizer = Mock(spec=CallAuthorizer)
        self.financial = Mock(spec=CallFinancialDelivery)
        self.prepared: list[PreparedModelCall] = []
        self.sealed: list[SealModelCallObservation] = []
        self.pending = False
        self.seal_delay = 0.0
        self.repository.prepare.side_effect = self.prepare
        self.repository.start_dispatch.side_effect = self.dispatch
        self.repository.seal_observation.side_effect = self.seal
        self.authorizer.authorize.side_effect = self.authorize
        self.financial.reconcile.side_effect = self.reconcile
        self.execution = ModelCallExecutor(
            repository=self.repository,
            authorizer=self.authorizer,
            financial=self.financial,
        )

    async def prepare(self, command: PrepareModelCall) -> PreparedModelCall:
        binding, binding_digest = encode_attempt_binding(command, command.request, 1)
        planned, planned_digest = encode_planned_artifact(command.planned)
        result = PreparedModelCall(
            tenant_id=command.request.execution_identity.tenant_ref,
            run_id=command.request.run_id,
            call_id=command.call_id,
            attempt_id=command.attempt_id,
            attempt_ordinal=1,
            lease=command.lease,
            attempt_binding_bytes=binding,
            attempt_binding_digest=binding_digest,
            planned_bytes=planned,
            planned_digest=planned_digest,
            final_request_binding_version=1,
            final_request_digest=command.final_request_digest,
            prepared_at_ms=1,
            replayed=False,
        )
        self.prepared.append(result)
        return result

    async def authorize(
        self, prepared: PreparedModelCall
    ) -> ModelCallAuthorizationReceipt:
        return ModelCallAuthorizationReceipt(
            tenant_id=prepared.tenant_id,
            run_id=prepared.run_id,
            attempt_id=prepared.attempt_id,
            request_ref="fixture-request",
            admission_ref="fixture-admission",
            authorization_bytes=b"{}",
            authorization_digest="b" * 64,
            newly_recorded=True,
        )

    async def dispatch(
        self, command: StartModelCallDispatch
    ) -> ModelCallDispatchPermit:
        return ModelCallDispatchPermit(
            tenant_id=command.tenant_id,
            run_id=command.run_id,
            attempt_id=command.attempt_id,
            authorization_digest=command.authorization_digest,
            dispatch_started_at_ms=2,
            newly_started=True,
        )

    async def seal(self, command: SealModelCallObservation) -> ModelCallEvidenceReceipt:
        await asyncio.sleep(self.seal_delay)
        self.sealed.append(command)
        return ModelCallEvidenceReceipt(
            tenant_id=command.tenant_id,
            run_id=command.run_id,
            attempt_id=command.attempt_id,
            event_id="11111111-1111-5111-8111-111111111111",
            revision=1,
            evidence_digest="c" * 64,
            observed_at_ms=3,
            replayed=False,
        )

    async def reconcile(
        self, tenant_id: str, attempt_id: str
    ) -> ModelSettlementReceipt:
        command = self.sealed[-1]
        assert command.tenant_id == tenant_id and command.attempt_id == attempt_id
        return ModelSettlementReceipt(
            admission_ref="fixture-admission",
            run_id=command.run_id,
            attempt_id=attempt_id,
            event_id="11111111-1111-5111-8111-111111111111",
            revision="1",
            digest="c" * 64,
            status="pending_reconciliation" if self.pending else "captured",
            reason="unknown_usage_or_outcome" if self.pending else None,
            captured_credits="0" if self.pending else "1",
            released_credits="0",
            usage_event_ref=None,
            settlement_ref=None,
            updated_at="2026-10-25T00:00:00.000Z",
        )


@asynccontextmanager
async def _fixture(
    monkeypatch: pytest.MonkeyPatch,
    *,
    mode: str = "ok",
    status: int = 200,
    hang: bool = False,
) -> AsyncGenerator[tuple[ModelHttpClients, _Backend, _Journal, _Peer]]:
    peer = _Peer(status=status, hang=hang)
    server = await asyncio.start_server(peer.handle, "127.0.0.1", 0)
    assert server.sockets
    port = server.sockets[0].getsockname()[1]
    scheme = "https" if mode == "tls" else "http"
    route = ResolvedModel(
        model_id="fixture-model",
        provider_id="fixture-provider",
        revision_id="fixture-revision",
        revision=1,
        digest="a" * 64,
        generation="1",
        tenant_generation="1",
        provider_model_name="provider-name",
        request_model="network-fixture",
        feature_key="chat",
        label_key="default",
        context_window=4096,
        max_output_tokens=256,
        connection=ModelConnection(
            protocol="openai_chat_completions",
            connection_type="direct",
            base_url=f"{scheme}://127.0.0.1:{port}/v1",
            secret_handle_ref="fixture-key",
        ),
    )
    clients = ModelHttpClients(
        AppConfig.from_env({"KOKORO_MODEL_ALLOW_LOOPBACK_HTTP": "true"}).model,
        route,
        ModelCredential(
            provider_id=route.provider_id,
            secret_handle_ref=route.connection.secret_handle_ref,
            api_key=SecretStr("isolated-fixture-not-a-credential"),
        ),
    )
    backend = _Backend(mode)
    transport = vars(clients.async_client)["_transport"]
    assert isinstance(transport, httpx.AsyncHTTPTransport)
    pool = vars(transport)["_pool"]
    assert isinstance(pool, httpcore.AsyncConnectionPool)
    monkeypatch.setattr(pool, "_network_backend", backend)
    try:
        yield clients, backend, _Journal(), peer
    finally:
        peer.release.set()
        await clients.aclose()
        server.close()
        await server.wait_closed()


async def _invoke(
    clients: ModelHttpClients,
    journal: _Journal,
    *,
    kind: Literal["model", "summarization"] = "model",
    task_id: str = "task",
    before_main: Callable[[], Awaitable[None]] | None = None,
    streaming: bool = False,
    nested: bool = False,
) -> ModelResponse:
    route = clients.route
    model = make_chat_model(
        clients.settings, model_from_route(route, None), model_clients=clients
    )
    planned = PlannedBinding(
        binding_version="2.0.0",
        model=route.model_id,
        provider=route.provider_id,
        revision_id=route.revision_id,
        provider_model_name=route.provider_model_name,
        request_model=route.request_model,
        feature_key=route.feature_key,
        revision="1",
        generation="1",
        tenant_generation="1",
        digest=route.digest,
        label=route.label_key,
    )
    gate = ModelCallRuntimeMiddleware(
        repository=journal.repository,
        execution=journal.execution,
        route=route,
        request=run_request("network-fixture-run"),
        lease=LeaseFence(owner="fixture-owner", generation=1),
        planned=planned,
        recipe=StaticRecipeBinding(canonical_bytes=b"{}", fingerprint="a" * 64),
        peer_path=("main",),
        logical_call_kind=kind,
    )
    runtime: Runtime[None] = Runtime(
        execution_info=ExecutionInfo(
            checkpoint_id="checkpoint", checkpoint_ns="model:fixture", task_id=task_id
        )
    )
    request = ModelRequest(
        model=model,
        messages=[HumanMessage(content="fixture")],
        runtime=runtime,
        model_settings={"kokoro_summary_prompt_digest": "d" * 64}
        if kind == "summarization"
        else {},
    )

    async def handler(request: ModelRequest) -> ModelResponse:
        if before_main is not None:
            await before_main()
        if streaming:
            chunks = [chunk async for chunk in request.model.astream(request.messages)]
            return ModelResponse(
                result=[AIMessage(content="".join(chunk.text for chunk in chunks))]
            )
        return ModelResponse(result=[await request.model.ainvoke(request.messages)])

    if nested:
        # The real native summary gate runs inside the outer handler's task/scope.
        return await gate.awrap_model_call(request, handler)
    scope = ModelCallScope(timeout_s=1)
    with bind_model_call_scope(scope):
        task = asyncio.create_task(gate.awrap_model_call(request, handler))
        primary: BaseException | None = None
        try:
            return await asyncio.wait_for(task, 2)
        except BaseException as error:
            primary = error
            raise
        finally:
            await scope.settle(cancel_active=False, primary=primary)


@pytest.mark.parametrize("streaming", [False, True])
async def test_sdk_final_wire_timeouts_and_shared_main_summary_clients(
    monkeypatch: pytest.MonkeyPatch, streaming: bool
) -> None:
    async with _fixture(monkeypatch) as (clients, backend, journal, peer):
        extensions: list[object] = []

        async def observe(request: httpx.Request) -> None:
            extensions.append(request.extensions.get("timeout"))
            assert request.headers["x-stainless-retry-count"] == "0"

        clients.async_client.event_hooks["request"].append(observe)
        await _invoke(clients, journal, streaming=streaming)
        await _invoke(clients, journal, kind="summarization", streaming=streaming)
        assert extensions == [{"connect": 5, "read": 120, "write": 30, "pool": 5}] * 2
        assert backend.timeouts == [5, 5]
        assert len(peer.requests) == len(journal.prepared) == len(journal.sealed) == 2
        assert journal.sealed[0].summary is None
        assert journal.sealed[1].summary is not None
        assert all(seal.usage.state == "normalized" for seal in journal.sealed)


async def test_native_connect_retry_is_one_authorization_post_and_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _fixture(monkeypatch, mode="connect") as (
        clients,
        backend,
        journal,
        peer,
    ):
        result = await _invoke(clients, journal)
        assert result.result[0].text == "ok"
        assert backend.calls == 2
        assert len(peer.requests) == len(journal.prepared) == len(journal.sealed) == 1
        journal.authorizer.authorize.assert_awaited_once()
        journal.repository.start_dispatch.assert_awaited_once()
        assert journal.sealed[0].outcome == "completed"
        assert journal.sealed[0].usage.state == "normalized"


async def test_native_tls_boundary_retry_closes_failed_socket_without_replaying_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _fixture(monkeypatch, mode="tls") as (clients, backend, journal, peer):
        await _invoke(clients, journal)
        assert backend.calls == 2 and backend.tls_timeouts == [5, 5]
        assert backend.streams[0].closed
        assert len(peer.requests) == len(journal.prepared) == len(journal.sealed) == 1
        journal.authorizer.authorize.assert_awaited_once()


@pytest.mark.parametrize(
    "mode,status", [("read", 200), ("write", 200), ("protocol", 200), ("ok", 500)]
)
async def test_post_send_failures_never_retry(
    monkeypatch: pytest.MonkeyPatch, mode: str, status: int
) -> None:
    async with _fixture(monkeypatch, mode=mode, status=status) as (
        clients,
        backend,
        journal,
        peer,
    ):
        journal.pending = True
        with pytest.raises((APIConnectionError, APIStatusError)):
            await _invoke(clients, journal)
        assert backend.calls == 1
        assert len(peer.requests) <= 1
        assert len(journal.prepared) == len(journal.sealed) == 1
        journal.authorizer.authorize.assert_awaited_once()
        assert journal.sealed[0].outcome == "failed"
        assert journal.sealed[0].usage.state == "unresolved"


async def test_connect_retry_exhaustion_and_cancel_keep_one_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _fixture(monkeypatch, mode="always_connect") as (
        clients,
        backend,
        journal,
        peer,
    ):
        journal.pending = True
        with pytest.raises(APIConnectionError):
            await _invoke(clients, journal)
        assert backend.calls == 2
        assert not peer.requests
        assert len(journal.prepared) == len(journal.sealed) == 1
    async with _fixture(monkeypatch, mode="connect_hang") as (
        clients,
        backend,
        journal,
        peer,
    ):
        journal.pending = True
        task = asyncio.create_task(_invoke(clients, journal))
        await backend.entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert backend.calls == 1 and not peer.requests
        assert len(journal.sealed) == 1 and journal.sealed[0].outcome == "cancelled"


async def test_cancel_during_native_retry_prevents_second_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _fixture(monkeypatch, mode="connect_pause") as (
        clients,
        backend,
        journal,
        peer,
    ):
        journal.pending = True
        task = asyncio.create_task(_invoke(clients, journal))
        await asyncio.wait_for(backend.retrying.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert backend.calls == 1 and not peer.requests
        assert len(journal.prepared) == len(journal.sealed) == 1
        assert journal.sealed[0].outcome == "cancelled"


async def test_expired_handler_seals_unknown_and_keeps_pending_not_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        call_runtime, "_MODEL_HANDLER_TIMEOUT_SECONDS", 0.025, raising=False
    )
    async with _fixture(monkeypatch, hang=True) as (clients, backend, journal, peer):
        journal.pending = True
        task = asyncio.create_task(_invoke(clients, journal))
        await asyncio.wait_for(peer.entered.wait(), 1)
        try:
            await asyncio.wait_for(asyncio.shield(task), 0.2)
        except BaseException as error:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            assert type(error).__name__ == "ModelCallDeadlineExceeded"
        else:
            pytest.fail("expired handler unexpectedly succeeded")
        assert (
            backend.calls
            == len(peer.requests)
            == len(journal.prepared)
            == len(journal.sealed)
            == 1
        )
        assert journal.sealed[0].outcome == "failed"
        assert journal.sealed[0].usage.state == "unresolved"
        assert journal.sealed[0].actual.state == "unknown"
        journal.financial.reconcile.assert_awaited_once()


async def test_seal_is_outside_handler_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        call_runtime, "_MODEL_HANDLER_TIMEOUT_SECONDS", 0.04, raising=False
    )
    async with _fixture(monkeypatch) as (clients, _backend, journal, _peer):
        journal.seal_delay = 0.08
        result = await _invoke(clients, journal)
        assert result.result[0].text == "ok"
        assert len(journal.sealed) == 1 and journal.sealed[0].outcome == "completed"


async def test_outer_handler_budget_includes_nested_summary_without_faking_main_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        call_runtime, "_MODEL_HANDLER_TIMEOUT_SECONDS", 0.1, raising=False
    )
    async with _fixture(monkeypatch) as (clients, _backend, journal, peer):
        journal.seal_delay = 0.06

        async def summary_then_wait() -> None:
            await _invoke(
                clients, journal, kind="summarization", task_id="summary", nested=True
            )
            journal.seal_delay = 0
            await asyncio.sleep(0.06)

        with pytest.raises(Exception) as captured:
            await _invoke(clients, journal, before_main=summary_then_wait)
        assert type(captured.value).__name__ == "ModelCallDeadlineExceeded"
        assert len(peer.requests) == len(journal.prepared) == len(journal.sealed) == 1
        assert journal.sealed[0].summary is not None
        assert journal.sealed[0].outcome == "completed"


async def test_outer_expiry_cancels_same_task_nested_summary_without_new_main_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(call_runtime, "_MODEL_HANDLER_TIMEOUT_SECONDS", 0.1)
    async with _fixture(monkeypatch, hang=True) as (clients, backend, journal, peer):
        journal.pending = True

        async def delayed_summary() -> None:
            await asyncio.sleep(0.04)
            await _invoke(
                clients, journal, kind="summarization", task_id="summary", nested=True
            )

        with pytest.raises(Exception) as captured:
            await _invoke(clients, journal, before_main=delayed_summary)
        assert type(captured.value).__name__ == "ModelCallDeadlineExceeded"
        assert (
            backend.calls
            == len(peer.requests)
            == len(journal.prepared)
            == len(journal.sealed)
            == 1
        )
        assert journal.sealed[0].outcome == "cancelled"
        assert journal.sealed[0].usage.state == "unresolved"
        assert journal.sealed[0].summary is not None
        assert journal.sealed[0].summary.text is None
