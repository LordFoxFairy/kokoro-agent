from __future__ import annotations

import asyncio
import logging
import socket
import ssl
from collections.abc import Awaitable, Callable
from typing import Never
from unittest.mock import Mock
from uuid import UUID

import httpx
from langchain.agents.middleware import ModelRequest, ModelResponse
from langgraph.runtime import ExecutionInfo, Runtime
from openai import APIConnectionError, APITimeoutError

import pytest

from kokoro_agent.clients.system import ResolvedModel
from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
from kokoro_agent.model.call_models import ModelCallInputInvalid, ModelCallRepository
import kokoro_agent.model.call_runtime as call_runtime
from kokoro_agent.model.call_runtime import (
    ModelCallRuntimeMiddleware,
    reject_final_model_request,
)
from kokoro_agent.model.execution import ModelCallExecution
from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding
from support.fakes import request as run_request
from support.local_fake import LocalFakeChatModel

from kokoro_agent.model.call_scope import (
    ModelCallScope,
    ModelCallScopeUnavailable,
    ModelCallSettlementTimeout,
    bind_model_call_scope,
    register_current_model_call_task,
)


async def test_scope_cancels_and_joins_exact_registered_task() -> None:
    scope = ModelCallScope(timeout_s=1.0)
    entered = asyncio.Event()
    finished = asyncio.Event()

    async def model_node() -> None:
        register_current_model_call_task()
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            finished.set()

    with bind_model_call_scope(scope):
        task = asyncio.create_task(model_node())
        await entered.wait()
        await scope.settle(cancel_active=True, primary=None)

    assert task.cancelled()
    assert finished.is_set()


async def test_scope_uses_first_deadline_across_repeated_cancellation() -> None:
    scope = ModelCallScope(timeout_s=0.01)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def uncooperative_node() -> None:
        register_current_model_call_task()
        entered.set()
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue

    with bind_model_call_scope(scope):
        task = asyncio.create_task(uncooperative_node())
        await entered.wait()
        with pytest.raises(ModelCallSettlementTimeout) as captured:
            await scope.settle(cancel_active=True, primary=None)
        with pytest.raises(ModelCallSettlementTimeout):
            await scope.settle(cancel_active=True, primary=None)

    assert captured.value.active_count == 1
    release.set()
    await task


async def test_scope_rejects_late_registration_before_work() -> None:
    scope = ModelCallScope(timeout_s=1.0)
    await scope.settle(cancel_active=False, primary=None)

    async def late_node() -> None:
        register_current_model_call_task()
        raise AssertionError("late task continued")

    with bind_model_call_scope(scope):
        task = asyncio.create_task(late_node())
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_normal_join_cancellation_cancels_owned_task_and_propagates() -> None:
    scope = ModelCallScope(timeout_s=1.0)
    entered = asyncio.Event()
    finished = asyncio.Event()

    async def model_node() -> None:
        register_current_model_call_task()
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            finished.set()

    async def settle_normally() -> None:
        await scope.settle(cancel_active=False, primary=None)

    with bind_model_call_scope(scope):
        model_task = asyncio.create_task(model_node())
        await entered.wait()
        invoke_task = asyncio.create_task(settle_normally())
        await asyncio.sleep(0)
        invoke_task.cancel()
        invoke_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await invoke_task

    assert model_task.cancelled()
    assert finished.is_set()


async def test_registration_requires_invocation_scope() -> None:
    with pytest.raises(ModelCallScopeUnavailable):
        register_current_model_call_task()


_DIAGNOSTIC_LOGGER = "kokoro_agent.model.call_runtime"
_PRIVATE = "SENTINEL_SECRET_HEADER_BODY_QUERY"


def _sdk_error(error_type: type[APIConnectionError]) -> APIConnectionError:
    request = httpx.Request(
        "POST",
        f"https://sensitive.invalid/PAYLOAD?TOKEN={_PRIVATE}",
        headers={"Authorization": f"Bearer {_PRIVATE}"},
        content=_PRIVATE,
    )
    if error_type is APITimeoutError:
        return APITimeoutError(request=request)
    return APIConnectionError(message=_PRIVATE, request=request)


async def _invoke_gate_failure(
    handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
) -> BaseException:
    gate = ModelCallRuntimeMiddleware(
        repository=Mock(spec=ModelCallRepository),
        execution=Mock(spec=ModelCallExecution),
        route=Mock(spec=ResolvedModel),
        request=run_request("private-diagnostic-run"),
        lease=LeaseFence(owner="owner", generation=1),
        planned=Mock(spec=PlannedBinding),
        recipe=StaticRecipeBinding(canonical_bytes=b"{}", fingerprint="a" * 64),
        peer_path=("main",),
    )
    runtime: Runtime[None] = Runtime(
        execution_info=ExecutionInfo(
            checkpoint_id="checkpoint", checkpoint_ns="model:task", task_id="task"
        )
    )
    request = ModelRequest(model=LocalFakeChatModel(), messages=[], runtime=runtime)
    scope = ModelCallScope(timeout_s=1.0)
    with bind_model_call_scope(scope):
        task = asyncio.create_task(gate.awrap_model_call(request, handler))
        with pytest.raises(BaseException) as captured:
            await task
        await scope.settle(cancel_active=False, primary=captured.value)
    return captured.value


async def test_handler_uses_one_absolute_budget_not_one_per_progress_step(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        call_runtime, "_MODEL_HANDLER_TIMEOUT_SECONDS", 0.025, raising=False
    )
    steps: list[int] = []

    async def handler(_request: ModelRequest) -> ModelResponse:
        for index in range(8):
            await asyncio.sleep(0.01)
            steps.append(index)
        raise AssertionError("handler exceeded its total budget")

    error = await _invoke_gate_failure(handler)
    assert type(error).__name__ == "ModelCallDeadlineExceeded"
    assert len(steps) < 8


async def test_production_handler_budget_survives_599_seconds_but_expires_at_601(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    real_time = loop.time
    offset = 0.0
    reached_599 = False
    monkeypatch.setattr(loop, "time", lambda: real_time() + offset)

    async def handler(_request: ModelRequest) -> ModelResponse:
        nonlocal offset, reached_599
        try:
            offset = 599
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            reached_599 = True
            offset = 601
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            raise AssertionError("production 600 second budget did not expire")
        finally:
            offset = 0

    error = await _invoke_gate_failure(handler)
    assert reached_599
    assert type(error).__name__ == "ModelCallDeadlineExceeded"


async def test_inner_timeout_error_is_not_the_handler_deadline() -> None:
    error = TimeoutError("inner operation, not the model handler budget")

    async def handler(_request: ModelRequest) -> ModelResponse:
        raise error

    assert await _invoke_gate_failure(handler) is error


async def test_handler_own_timeout_during_deadline_cleanup_keeps_its_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(call_runtime, "_MODEL_HANDLER_TIMEOUT_SECONDS", 0.01)
    error = TimeoutError("handler's own timeout during cleanup")

    async def handler(_request: ModelRequest) -> ModelResponse:
        try:
            await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            raise error from None
        raise error

    assert await _invoke_gate_failure(handler) is error


async def test_handler_suppressing_deadline_cancel_cannot_return_late_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(call_runtime, "_MODEL_HANDLER_TIMEOUT_SECONDS", 0.01)

    async def handler(_request: ModelRequest) -> ModelResponse:
        try:
            await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            return ModelResponse(result=[])
        raise AssertionError("deadline did not cancel the handler")

    error = await _invoke_gate_failure(handler)
    assert type(error).__name__ == "ModelCallDeadlineExceeded"


async def test_handler_cancellation_remains_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        call_runtime, "_MODEL_HANDLER_TIMEOUT_SECONDS", 0.01, raising=False
    )
    error = asyncio.CancelledError()

    async def handler(_request: ModelRequest) -> ModelResponse:
        raise error

    assert isinstance(await _invoke_gate_failure(handler), asyncio.CancelledError)


async def test_recorded_denial_keeps_priority_when_handler_deadline_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        call_runtime, "_MODEL_HANDLER_TIMEOUT_SECONDS", 0.01, raising=False
    )
    denial = ModelCallInputInvalid("original authority denial")

    async def handler(_request: ModelRequest) -> ModelResponse:
        try:
            reject_final_model_request(denial)
        except ModelCallInputInvalid:
            pass
        await asyncio.sleep(0.05)
        raise AssertionError("budget did not cancel the handler")

    assert await _invoke_gate_failure(handler) is denial


async def test_lost_authority_raised_during_deadline_cleanup_is_not_reclassified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.model.call_models import ModelCallAuthorityLost

    monkeypatch.setattr(call_runtime, "_MODEL_HANDLER_TIMEOUT_SECONDS", 0.01)
    lost = ModelCallAuthorityLost("original lease loss")

    async def handler(_request: ModelRequest) -> ModelResponse:
        try:
            await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            raise lost from None
        raise AssertionError("deadline did not cancel the handler")

    assert await _invoke_gate_failure(handler) is lost


async def test_external_cancel_racing_own_deadline_still_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(call_runtime, "_MODEL_HANDLER_TIMEOUT_SECONDS", 0.01)

    async def handler(_request: ModelRequest) -> ModelResponse:
        try:
            await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            owner = asyncio.current_task()
            assert owner is not None
            owner.cancel()
            raise
        raise AssertionError("deadline did not cancel the handler")

    assert isinstance(await _invoke_gate_failure(handler), asyncio.CancelledError)


def _diagnostic(caplog: pytest.LogCaptureFixture) -> str:
    records = [record for record in caplog.records if record.name == _DIAGNOSTIC_LOGGER]
    assert len(records) == 1
    record = records[0]
    message = record.getMessage()
    assert record.exc_info is None and record.stack_info is None
    assert _PRIVATE not in repr(record.__dict__)
    assert "sensitive.invalid" not in repr(record.__dict__)
    assert "Authorization" not in message and "PAYLOAD" not in message
    fields = dict(part.split("=", 1) for part in message.split()[1:])
    assert str(UUID(fields["call_id"])) == fields["call_id"]
    assert str(UUID(fields["attempt_id"])) == fields["attempt_id"]
    return message


@pytest.mark.parametrize("sdk_type", [APIConnectionError, APITimeoutError])
@pytest.mark.parametrize(
    ("cause_type", "expected"),
    [
        (socket.gaierror, "dns_error"),
        (ssl.SSLCertVerificationError, "tls_certificate_error"),
        (ssl.SSLError, "tls_error"),
        (ConnectionRefusedError, "connection_refused"),
        (ConnectionResetError, "connection_reset"),
        (ConnectionAbortedError, "connection_aborted"),
        (RuntimeError, "runtime_error"),
        (httpx.ConnectTimeout, "connect_timeout"),
        (httpx.ReadTimeout, "read_timeout"),
        (httpx.WriteTimeout, "write_timeout"),
        (httpx.PoolTimeout, "pool_timeout"),
        (httpx.ConnectError, "connect_error"),
        (httpx.ReadError, "read_error"),
        (httpx.WriteError, "write_error"),
        (httpx.LocalProtocolError, "local_protocol_error"),
        (httpx.RemoteProtocolError, "remote_protocol_error"),
        (httpx.UnsupportedProtocol, "unsupported_protocol"),
        (ValueError, "unknown"),
    ],
)
async def test_sdk_failure_logs_only_typed_private_cause_categories(
    sdk_type: type[APIConnectionError],
    cause_type: type[Exception],
    expected: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger=_DIAGNOSTIC_LOGGER)
    error = _sdk_error(sdk_type)
    error.__cause__ = cause_type(_PRIVATE)

    async def handler(_request: ModelRequest) -> ModelResponse:
        raise error

    assert await _invoke_gate_failure(handler) is error
    message = _diagnostic(caplog)
    sdk_category = (
        "timeout_error" if sdk_type is APITimeoutError else "connection_error"
    )
    assert f"sdk_category={sdk_category}" in message
    assert f"root_cause_category={expected}" in message
    assert f"cause_categories={expected}" in message
    assert "cause_end=complete" in message


async def test_nested_explicit_cause_records_root_without_exception_text(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger=_DIAGNOSTIC_LOGGER)
    error = _sdk_error(APIConnectionError)
    middle = httpx.ConnectError(_PRIVATE)
    middle.__cause__ = ssl.SSLCertVerificationError(_PRIVATE)
    error.__cause__ = middle

    async def handler(_request: ModelRequest) -> ModelResponse:
        raise error

    assert await _invoke_gate_failure(handler) is error
    message = _diagnostic(caplog)
    assert "cause_categories=connect_error,tls_certificate_error" in message
    assert "root_cause_category=tls_certificate_error" in message


@pytest.mark.parametrize("ending", ["cycle", "truncated"])
async def test_explicit_cause_cycle_or_limit_never_claims_a_known_root(
    ending: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger=_DIAGNOSTIC_LOGGER)
    error = _sdk_error(APIConnectionError)
    first = RuntimeError(_PRIVATE)
    if ending == "cycle":
        first.__cause__ = first
    else:
        current = first
        for _ in range(8):
            next_error = RuntimeError(_PRIVATE)
            current.__cause__ = next_error
            current = next_error
        current.__cause__ = socket.gaierror(_PRIVATE)
    error.__cause__ = first

    async def handler(_request: ModelRequest) -> ModelResponse:
        raise error

    assert await _invoke_gate_failure(handler) is error
    message = _diagnostic(caplog)
    assert f"cause_end={ending}" in message
    assert "root_cause_category=unknown" in message
    assert "dns_error" not in message
    categories = next(
        field for field in message.split() if field.startswith("cause_categories=")
    )
    assert len(categories.split("=", 1)[1].split(",")) <= 4


@pytest.mark.parametrize(
    "name",
    [
        "ConnectError",
        "RuntimeError",
        "ConnectionRefusedError",
        "ConnectionResetError",
        "ConnectionAbortedError",
        "SSLError",
        "SSLCertVerificationError",
    ],
)
async def test_same_named_cause_is_unknown_and_implicit_context_is_not_read(
    name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger=_DIAGNOSTIC_LOGGER)
    error = _sdk_error(APIConnectionError)
    error.__cause__ = type(name, (Exception,), {})(_PRIVATE)
    error.__cause__.__context__ = socket.gaierror(_PRIVATE)

    async def handler(_request: ModelRequest) -> ModelResponse:
        raise error

    assert await _invoke_gate_failure(handler) is error
    message = _diagnostic(caplog)
    assert "root_cause_category=unknown" in message
    assert "dns_error" not in message


@pytest.mark.parametrize("name", ["APIConnectionError", "APITimeoutError"])
async def test_same_named_non_sdk_failure_does_not_emit_sdk_diagnostic(
    name: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger=_DIAGNOSTIC_LOGGER)
    error = type(name, (Exception,), {})(_PRIVATE)
    error.__cause__ = socket.gaierror(_PRIVATE)

    async def handler(_request: ModelRequest) -> ModelResponse:
        raise error

    assert await _invoke_gate_failure(handler) is error
    assert not [
        record for record in caplog.records if record.name == _DIAGNOSTIC_LOGGER
    ]


async def test_diagnostic_logger_failure_never_replaces_the_sdk_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = _sdk_error(APIConnectionError)
    calls: list[object] = []

    def fail(*args: object, **kwargs: object) -> Never:
        calls.append((args, kwargs))
        raise RuntimeError(_PRIVATE)

    monkeypatch.setattr(logging.getLogger(_DIAGNOSTIC_LOGGER), "warning", fail)

    async def handler(_request: ModelRequest) -> ModelResponse:
        raise error

    assert await _invoke_gate_failure(handler) is error
    assert len(calls) == 1


async def test_diagnostic_keeps_existing_denial_type_restoration(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger=_DIAGNOSTIC_LOGGER)
    denial = ModelCallInputInvalid(_PRIVATE)
    wrapped = _sdk_error(APIConnectionError)

    async def handler(_request: ModelRequest) -> ModelResponse:
        try:
            reject_final_model_request(denial)
        except ModelCallInputInvalid as error:
            raise wrapped from error

    assert await _invoke_gate_failure(handler) is denial
    assert "root_cause_category=unknown" in _diagnostic(caplog)
