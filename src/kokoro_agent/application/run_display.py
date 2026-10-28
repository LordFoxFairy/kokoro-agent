"""Read-only display facts; never execution authority or historical event patches."""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from typing import Any

from langchain_core.messages import AIMessage, BaseMessage
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator
from typing import Self

from kokoro_agent.domain.chat.projection import (
    SkillDisplay,
    ToolDisplaySource,
    ToolDisplayDetail,
    safe_progress_id,
    safe_virtual_path,
)


class ToolPath(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    activity_id: str = Field(pattern=r"^act_[0-9a-f]{64}$")
    virtual_path: str

    @model_validator(mode="after")
    def safe_path(self) -> Self:
        if not safe_virtual_path(self.virtual_path):
            raise ValueError("unsafe display path")
        return self


class RunDisplay(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    run_id: str
    display_skills: list[SkillDisplay] = Field(max_length=16)
    tool_paths: list[ToolPath] = Field(max_length=128)


def recover_display(
    *,
    scope: tuple[str, str, str, str],
    selected: tuple[str, ...],
    events: Sequence[Mapping[str, Any]],
    metadata: Sequence[Mapping[str, Any]],
    messages: Sequence[BaseMessage],
    journal: Mapping[str, str],
) -> RunDisplay:
    """Intersect native facts with original ready/activity and real journal IDs.

    Legacy metadata alone is not loader completion or per-Run authorization.
    Native messages may include earlier Runs: journal plus opaque activity identity
    prevents those messages from manufacturing this Run's tool provenance.
    """
    skills: dict[str, SkillDisplay] = {}
    tools: dict[str, ToolPath] = {}
    ready_refs: set[str] = set()
    activities: dict[str, Mapping[str, Any]] = {}
    for event in events:
        if event.get("activity") == "skill" and event.get("phase") == "ready":
            ready_refs.update(
                ref for ref in event.get("source_refs", []) if ref in selected
            )
            for value in event.get("display_skills", []):
                fact = SkillDisplay.model_validate(value)
                if fact.source_ref not in ready_refs:
                    raise ValueError("display skill is outside ready selection")
                if fact.source_ref in skills and skills[fact.source_ref] != fact:
                    raise ValueError("conflicting Skill display fact")
                skills[fact.source_ref] = fact
        elif event.get("activity") == "tool":
            activity_id = event["activity_id"]
            old = activities.get(activity_id)
            if old is not None and (
                old.get("display_code"),
                old.get("display_detail"),
            ) != (event.get("display_code"), event.get("display_detail")):
                raise ValueError("conflicting tool display fact")
            activities[activity_id] = event
            detail: Any = event.get("display_detail")
            if detail is not None:
                parsed_detail = ToolDisplayDetail.model_validate(detail)
                if parsed_detail.virtual_path is not None:
                    tools[activity_id] = ToolPath(
                        activity_id=activity_id, virtual_path=parsed_detail.virtual_path
                    )
    for ref in selected:
        if ref not in ready_refs or ref in skills:
            continue
        path = (
            "/.skills/"
            + base64.urlsafe_b64encode(ref[6:].encode("ascii")).rstrip(b"=").decode()
            + "/SKILL.md"
        )
        matches = [item for item in metadata if item.get("path") == path]
        names = {item.get("name") for item in matches}
        if len(names) > 1:
            raise ValueError("conflicting native Skill metadata")
        if len(names) == 1 and isinstance(next(iter(names)), str):
            try:
                skills[ref] = SkillDisplay(
                    source_ref=ref, name=str(next(iter(names))), virtual_path=path
                )
            except ValueError:
                pass  # Unsafe optional metadata is unknown, never execution failure.
    prefixes = tuple(
        fact.virtual_path.removesuffix("SKILL.md") for fact in skills.values()
    )
    for message in messages:
        if not isinstance(message, AIMessage) or not message.id:
            continue
        for call in message.tool_calls:
            call_id = call.get("id")
            if not call_id or journal.get(call_id) != call["name"]:
                continue
            activity_id = "act_" + safe_progress_id(
                "tool-activity", *scope, call_id, call_id
            )
            activity = activities.get(activity_id)
            if (
                activity is None
                or activity.get("display_code") != "tool.file.read"
                or call["name"] != "read_file"
            ):
                continue
            # Old frozen recipe does not prove a general host backend is virtual.
            # Exact read-only Skill mounts do prove their public virtual prefix.
            detail = ToolDisplaySource(
                name="read_file",
                code="tool.file.read",
                origin="native",
                skill_prefixes=prefixes,
            ).detail(call["args"])
            old_detail = (
                TypeAdapter(dict[str, Any]).validate_python(
                    activity.get("display_detail")
                )
                if activity.get("display_detail") is not None
                else {}
            )
            if (
                detail is not None
                and old_detail.get("kind") == "file"
                and old_detail.get("name") == detail.name
                and detail.virtual_path
            ):
                fact = ToolPath(
                    activity_id=activity_id, virtual_path=detail.virtual_path
                )
                if activity_id in tools and tools[activity_id] != fact:
                    raise ValueError("conflicting native tool path")
                tools[activity_id] = fact
    return RunDisplay(
        run_id=scope[3],
        display_skills=list(skills.values()),
        tool_paths=list(tools.values()),
    )
