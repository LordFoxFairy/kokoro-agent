"""Agent execution repository ports and transport-neutral result records.

The concrete PostgreSQL adapter lives in ``infrastructure/postgres_run_repository.py``.
This module contains no database driver, SQL, or connection factory.
"""

from __future__ import annotations

from typing import Protocol

from kokoro_agent.domain.run.repositories import (
    RunAdmissionPort,
    RunControlPort,
    RunEffectPort,
    RunEventPort,
    RunLifecyclePort,
    RunSandboxCleanupPort,
)
from kokoro_agent.domain.run.models import (
    ControlAdmission,
    ControlAdmissionReceipt,
    ControlAdmissionStatus,
    DispatchAdmission,
    LeaseFence,
    LeasedRun,
    OutboxFrame,
    ReceiptReconcile,
    RunControlCommandRecord,
    SandboxBackendKind,
    SandboxCleanupIntent,
    StagedFrame,
    ToolJournalRecord,
)


class RunRepository(
    RunAdmissionPort,
    RunControlPort,
    RunEffectPort,
    RunEventPort,
    RunLifecyclePort,
    RunSandboxCleanupPort,
    Protocol,
):
    """Composite port used by the worker composition root.

    Runtime wiring still injects one Agent-owned implementation. Individual
    consumers should type against one of the narrow ports above.
    """

    pass


class DispatchConflict(RuntimeError):
    """A run id was reused with a different immutable launch envelope."""


class ControlCommandConflict(RuntimeError):
    """A command id was reused with a different immutable request digest."""


class UsageIdentityConflict(RuntimeError):
    """A lease generation was replayed with different immutable usage totals."""


__all__ = [
    "ControlAdmission",
    "ControlAdmissionReceipt",
    "ControlAdmissionStatus",
    "ControlCommandConflict",
    "DispatchAdmission",
    "DispatchConflict",
    "LeaseFence",
    "LeasedRun",
    "OutboxFrame",
    "ReceiptReconcile",
    "RunControlCommandRecord",
    "RunRepository",
    "SandboxBackendKind",
    "SandboxCleanupIntent",
    "StagedFrame",
    "ToolJournalRecord",
    "UsageIdentityConflict",
]
