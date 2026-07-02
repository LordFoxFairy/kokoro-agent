"""Skill mounts authorized for a single agent run."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class SkillMount(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str
    path: str
    lock: str | None = None
