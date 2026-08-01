"""Owner-event durability policy shared by orchestration and storage."""

from __future__ import annotations

CRITICAL_EVENT_KINDS: frozenset[str] = frozenset(
    {
        "run.started",
        "tool.awaiting_approval",
        "plan.proposed",
        "run.control.receipt",
        "run.owner.completed",
        "run.completed",
        "run.failed",
    }
)

TERMINAL_EVENT_KINDS: frozenset[str] = frozenset({"run.completed", "run.failed"})


def is_critical_event_kind(kind: str) -> bool:
    return kind in CRITICAL_EVENT_KINDS


def is_terminal_event_kind(kind: str) -> bool:
    return kind in TERMINAL_EVENT_KINDS
