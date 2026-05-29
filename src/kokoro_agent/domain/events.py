from __future__ import annotations

from typing import Any


def run_created(run_id: str) -> dict[str, Any]:
    return {"event": "run.created", "run_id": run_id}


def message_delta(run_id: str, delta: str) -> dict[str, Any]:
    return {"event": "message.delta", "run_id": run_id, "delta": delta}


def message_completed(run_id: str, content: str) -> dict[str, Any]:
    return {"event": "message.completed", "run_id": run_id, "content": content}


def run_completed(run_id: str) -> dict[str, Any]:
    return {"event": "run.completed", "run_id": run_id}
