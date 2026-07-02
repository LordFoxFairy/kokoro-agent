"""Agent run lifecycle states."""

from __future__ import annotations

from typing import Literal


RunStatus = Literal["pending", "running", "awaiting_approval", "completed", "failed", "cancelled"]
TerminalRunStatus = Literal["completed", "failed", "cancelled"]
