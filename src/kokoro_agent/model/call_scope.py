"""Invocation-local ownership and finite settlement for native model-call tasks."""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from math import isfinite


DEFAULT_MODEL_CALL_SETTLE_TIMEOUT_S = 5.0


class ModelCallScopeUnavailable(RuntimeError):
    """The native middleware ran outside the required invocation scope."""


class ModelCallSettlementTimeout(BaseException):
    """Registered native model tasks did not settle before the fixed deadline."""

    def __init__(self, *, active_count: int) -> None:
        super().__init__(
            f"model-call task settlement timed out with {active_count} active task(s)"
        )
        self.active_count = active_count


_CURRENT_SCOPE: ContextVar[ModelCallScope | None] = ContextVar(
    "kokoro_agent_model_call_scope", default=None
)


class ModelCallScope:
    """Own exact model-node tasks created during one native invocation."""

    def __init__(self, *, timeout_s: float) -> None:
        if not isfinite(timeout_s) or timeout_s <= 0:
            raise ValueError("model-call settle timeout must be positive and finite")
        self._timeout_s = timeout_s
        self._phase = "accepting"
        self._active: set[asyncio.Task[object]] = set()
        self._registered: set[asyncio.Task[object]] = set()
        self._settled_errors: list[BaseException] = []
        self._changed = asyncio.Event()
        self._deadline: float | None = None
        self._cleanup: asyncio.Task[None] | None = None
        self._primary: BaseException | None = None

    def register_current(self) -> None:
        task = asyncio.current_task()
        if task is None:
            raise ModelCallScopeUnavailable("model-call task is unavailable")
        self.register_owned_task(task)

    def register_owned_task(self, task: asyncio.Task[object]) -> None:
        """Register one task explicitly created by this invocation."""

        if self._phase != "accepting":
            task.cancel()
            raise asyncio.CancelledError
        if task in self._registered:
            return
        self._registered.add(task)
        self._active.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[object]) -> None:
        self._active.discard(task)
        try:
            error = task.exception()
        except asyncio.CancelledError:
            error = None
        if error is not None:
            self._settled_errors.append(error)
        self._changed.set()

    async def settle(
        self, *, cancel_active: bool, primary: BaseException | None
    ) -> None:
        if self._cleanup is None:
            loop = asyncio.get_running_loop()
            self._phase = "settling"
            self._deadline = loop.time() + self._timeout_s
            self._primary = primary
            if cancel_active:
                for task in tuple(self._active):
                    task.cancel()
            self._cleanup = asyncio.create_task(self._settle_owned())
        cleanup = self._cleanup
        cancelled: asyncio.CancelledError | None = None
        while True:
            try:
                await asyncio.shield(cleanup)
                if cancelled is not None:
                    raise cancelled
                return
            except asyncio.CancelledError as error:
                if cleanup.done():
                    await cleanup
                    raise
                if cancelled is None:
                    cancelled = error
                    if self._primary is None:
                        self._primary = error
                for task in tuple(self._active):
                    task.cancel()
                continue

    async def _settle_owned(self) -> None:
        deadline = self._deadline
        assert deadline is not None
        try:
            async with asyncio.timeout_at(deadline):
                while self._active:
                    self._changed.clear()
                    if self._active:
                        await self._changed.wait()
        except TimeoutError:
            raise ModelCallSettlementTimeout(active_count=len(self._active)) from None
        self._phase = "settled"
        errors, self._settled_errors = self._settled_errors, []
        primary = self._primary
        secondary = [error for error in errors if error is not primary]
        if not secondary:
            return
        if primary is None:
            error = secondary.pop(0)
            for extra in secondary:
                error.add_note("additional model task error: " + type(extra).__name__)
            raise error
        for error in secondary:
            primary.add_note("model task also failed: " + type(error).__name__)


@contextmanager
def bind_model_call_scope(scope: ModelCallScope) -> Generator[None]:
    token = _CURRENT_SCOPE.set(scope)
    try:
        yield
    finally:
        _CURRENT_SCOPE.reset(token)


def register_current_model_call_task() -> None:
    scope = _CURRENT_SCOPE.get()
    if scope is None:
        raise ModelCallScopeUnavailable("model-call invocation scope is unavailable")
    scope.register_current()


__all__ = [
    "DEFAULT_MODEL_CALL_SETTLE_TIMEOUT_S",
    "ModelCallScope",
    "ModelCallScopeUnavailable",
    "ModelCallSettlementTimeout",
    "bind_model_call_scope",
    "register_current_model_call_task",
]
