from __future__ import annotations

import asyncio

import pytest

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
