"""Bound public action intent; never execution authority or a success claim."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass


def arguments_digest(arguments: object) -> str | None:
    try:
        encoded = json.dumps(
            arguments,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError):
        return None
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class ActionObservation:
    """An intent claim bound to one call, not proof of execution or success."""

    call_id: str
    tool_name: str
    arguments_digest: str
    title: str

    def matches(self, call_id: str, tool_name: str, arguments: object) -> bool:
        return (
            self.call_id == call_id
            and self.tool_name == tool_name
            and self.arguments_digest == arguments_digest(arguments)
        )


ActionObservationSink = Callable[[ActionObservation], None]
