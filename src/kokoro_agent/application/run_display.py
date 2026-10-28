"""Read-only display facts; never execution authority or historical event patches."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from mcp.types import CallToolResult
from kokoro_agent.domain.run.models import ToolJournalRecord
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator
from typing import Self

from kokoro_agent.domain.chat.projection import (
    SkillDisplay,
    ToolDisplaySource,
    ToolDisplayDetail,
    safe_progress_id,
    safe_virtual_path,
)


from kokoro_agent.domain.chat.tool_display_preview import (
    ActivityDetail,
    ToolPreview,
    clip_text,
    described_tool,
    mcp_output,
    preparation_output,
    preview,
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


class ToolFact(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    activity_id: str = Field(
        pattern=r"^act_[0-9a-f]{64}$", min_length=68, max_length=68
    )
    display_detail: ToolDisplayDetail | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    status: Literal["failed"] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def meaningful_mcp_fact(self) -> Self:
        if not self.model_fields_set.intersection({"display_detail", "status"}):
            raise ValueError("empty tool fact")
        if "status" in self.model_fields_set and self.status is None:
            raise ValueError("null tool fact status")
        if "display_detail" in self.model_fields_set and (
            self.display_detail is None
            or self.display_detail.kind != "mcp_tool"
            or "virtual_path" in self.display_detail.model_fields_set
        ):
            raise ValueError("tool fact requires an exact MCP name")
        return self


class RunDisplay(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    run_id: str
    display_skills: list[SkillDisplay] = Field(max_length=16)
    tool_paths: list[ToolPath] = Field(max_length=128)
    tool_facts: list[ToolFact] = Field(
        default_factory=lambda: list[ToolFact](), max_length=128
    )

    activity_details: list[ActivityDetail] = Field(
        default_factory=lambda: list[ActivityDetail](), max_length=128
    )

    @model_validator(mode="after")
    def unique_facts(self) -> Self:
        if len({fact.activity_id for fact in self.tool_facts}) != len(self.tool_facts):
            raise ValueError("duplicate tool display fact")
        if len({item.activity_id for item in self.activity_details}) != len(
            self.activity_details
        ):
            raise ValueError("duplicate private activity detail")
        return self


def recover_display(
    *,
    scope: tuple[str, str, str, str],
    selected: tuple[str, ...],
    events: Sequence[Mapping[str, Any]],
    metadata: Sequence[Mapping[str, Any]],
    messages: Sequence[BaseMessage],
    journal: Mapping[str, str],
    journal_records: Mapping[str, ToolJournalRecord] | None = None,
    mcp_connections: tuple[str, ...] = (),
    mcp_source_sha256: str | None = None,
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
    result = RunDisplay(
        run_id=scope[3],
        display_skills=list(skills.values()),
        tool_paths=list(tools.values()),
        activity_details=_activity_details(
            scope, activities, messages, journal_records or {}, mcp_connections
        ),
        tool_facts=_mcp_facts(
            scope,
            activities,
            messages,
            journal_records or {},
            mcp_connections,
            mcp_source_sha256,
        ),
    )

    return _fit_display(result)


def _mcp_facts(
    scope: tuple[str, str, str, str],
    activities: Mapping[str, Mapping[str, Any]],
    messages: Sequence[BaseMessage],
    journal: Mapping[str, ToolJournalRecord],
    connections: tuple[str, ...],
    mcp_source_sha256: str | None,
) -> list[ToolFact]:
    codes = {
        "mcp_call": "tool.mcp.call",
        "mcp_describe_tool": "tool.mcp.describe",
        "mcp_list_tools": "tool.mcp.discover",
    }
    matched: dict[str, tuple[str, ToolJournalRecord]] = {}
    for call_id, row in journal.items():
        activity_id = "act_" + safe_progress_id(
            "tool-activity", *scope, call_id, call_id
        )
        activity = activities.get(activity_id)
        if (
            row.name in codes
            and activity is not None
            and activity.get("display_code") == codes[row.name]
        ):
            matched[call_id] = (activity_id, row)
    targets: dict[str, tuple[str, str]] = {}
    for message in messages:
        if not isinstance(message, AIMessage) or not message.id:
            continue
        for call in message.tool_calls:
            call_id = call.get("id")
            if not call_id:
                continue
            pair = matched.get(call_id)
            if (
                pair is None
                or call["name"] != pair[1].name
                or call["name"] not in {"mcp_call", "mcp_describe_tool"}
            ):
                continue
            args = call["args"]
            if (
                type(args.get("server")) is not str
                or args["server"] not in connections
                or type(args.get("tool")) is not str
            ):
                continue
            try:
                ToolDisplayDetail(kind="mcp_tool", name=args["tool"])
            except ValueError:
                continue
            target = (args["server"], args["tool"])
            if call_id in targets and targets[call_id] != target:
                raise ValueError("conflicting native MCP call target")
            targets[call_id] = target
    approved: set[tuple[str, str]] = set()
    for call_id, target in targets.items():
        row = matched[call_id][1]
        if row.name != "mcp_call" or row.status != "succeeded" or row.is_error:
            continue
        try:
            result = CallToolResult.model_validate_json(row.result)
        except ValueError:
            continue
        if not result.isError:
            approved.add(target)
    legacy_failed: set[str] = set()
    if (
        mcp_source_sha256
        == "f4f1b0c9ff95221cbf619cdf6376e79d8858c07f32b7c33c72f989458417ddea"
    ):
        # This exact reviewed implementation returned these local errors as
        # successful ToolMessages. No arbitrary output or other version is parsed.
        constants = {
            "error: MCP_CONNECTION_NOT_DECLARED",
            "error: MCP_TOOL_NOT_ALLOWED",
            "error: MCP_CURRENT_READ_FAILED",
        }
        for message in messages:
            if (
                not isinstance(message, ToolMessage)
                or message.name != "mcp_describe_tool"
            ):
                continue
            call_id = message.tool_call_id
            pair = matched.get(call_id)
            if pair is None or call_id not in targets:
                continue
            row = pair[1]
            content = message.model_dump().get("content")
            if (
                row.name == "mcp_describe_tool"
                and row.status == "succeeded"
                and not row.is_error
                and row.result in constants
                and isinstance(content, str)
                and content == row.result
            ):
                legacy_failed.add(call_id)
    facts: dict[str, ToolFact] = {}
    for call_id, (activity_id, row) in matched.items():
        values: dict[str, Any] = {"activity_id": activity_id}
        if (row.status == "failed" and row.is_error) or call_id in legacy_failed:
            values["status"] = "failed"
        target = targets.get(call_id)
        if target is not None and target in approved:
            values["display_detail"] = ToolDisplayDetail(
                kind="mcp_tool", name=target[1]
            )
        if len(values) > 1:
            facts[activity_id] = ToolFact.model_validate(values)
    return [facts[activity_id] for activity_id in activities if activity_id in facts]


def _activity_details(
    scope: tuple[str, str, str, str],
    activities: Mapping[str, Mapping[str, Any]],
    messages: Sequence[BaseMessage],
    journal: Mapping[str, ToolJournalRecord],
    connections: tuple[str, ...],
) -> list[ActivityDetail]:
    codes = {
        "mcp_call": "tool.mcp.call",
        "mcp_describe_tool": "tool.mcp.describe",
        "mcp_list_tools": "tool.mcp.discover",
    }
    calls: dict[str, tuple[str, dict[str, Any]]] = {}
    conflicts: set[str] = set()
    for message in messages:
        if not isinstance(message, AIMessage) or not message.id:
            continue
        for call in message.tool_calls:
            call_id = call.get("id")
            if (
                not call_id
                or call_id not in journal
                or journal[call_id].name != call["name"]
            ):
                continue
            candidate = (call["name"], call["args"])
            if call_id in calls and calls[call_id] != candidate:
                conflicts.add(call_id)
            calls[call_id] = candidate
    details: dict[str, ActivityDetail] = {}
    targets: dict[str, tuple[str, str]] = {}
    descriptions: dict[tuple[str, str], set[str]] = {}
    for call_id, (name, args) in calls.items():
        if (
            call_id in conflicts
            or name not in codes
            or not connections
            or journal[call_id].status not in {"started", "succeeded", "failed"}
        ):
            continue
        activity_id = "act_" + safe_progress_id(
            "tool-activity", *scope, call_id, call_id
        )
        event = activities.get(activity_id)
        if event is None or event.get("display_code") != codes[name]:
            continue
        target: ToolDisplayDetail | None = None
        if name == "mcp_list_tools":
            if args:
                continue
            input_value: object = {}
        else:
            if (
                args.get("server") not in connections
                or type(args.get("tool")) is not str
            ):
                continue
            try:
                target = ToolDisplayDetail(kind="mcp_tool", name=args["tool"])
            except ValueError:
                continue
            input_value = (
                args.get("arguments") if name == "mcp_call" else {"tool": args["tool"]}
            )
            if not isinstance(input_value, dict):
                continue
        values: dict[str, Any] = {"activity_id": activity_id, "tool_name": name}
        if target is not None:
            values["target"] = target
        input_preview = preview(
            TypeAdapter(dict[str, Any]).validate_python(input_value, strict=True),
            limit=2048,
        )
        if input_preview is not None:
            values["input_preview"] = input_preview
        row = journal[call_id]
        if target is not None:
            binding = (args["server"], args["tool"])
            targets[activity_id] = binding
            if (
                name == "mcp_describe_tool"
                and row.status == "succeeded"
                and not row.is_error
            ):
                parsed = described_tool(row.result, server=binding[0], tool=binding[1])
                if parsed is not None:
                    descriptions.setdefault(binding, set()).add(parsed[0])
        if row.status in {"succeeded", "failed"}:
            output = (
                mcp_output(row.result)
                if name == "mcp_call"
                else (
                    preparation_output(
                        row.result,
                        name=name,
                        connections=connections,
                        server=args.get("server"),
                        tool=args.get("tool"),
                    )
                    if row.status == "succeeded"
                    and not row.is_error
                    and not row.result.startswith("error:")
                    else None
                )
            )
            if output is not None:
                values["output_preview"] = output
        details[activity_id] = ActivityDetail.model_validate(values)
    # Capability documentation observed in this Run, not this call's purpose.
    # Compare untruncated originals: a shared prefix is not proof of agreement.
    for activity_id, binding in targets.items():
        observed = descriptions.get(binding, set())
        if len(observed) != 1:
            continue
        description = preview(next(iter(observed)), limit=1024, plain=True)
        if description is not None:
            values = details[activity_id].model_dump(mode="json")
            values["tool_description"] = description.model_dump(mode="json")
            details[activity_id] = ActivityDetail.model_validate(values)
    # File targets remain in original display_detail/tool_paths; category alone
    # does not establish the actual native function name (write_file vs edit_file).
    return [details[key] for key in activities if key in details][:128]


def _fit_display(value: RunDisplay) -> RunDisplay:
    # Leave room for both HTTP envelopes; count actual JSON escapes, not raw text.
    def size() -> int:
        return len(
            json.dumps(value.model_dump(mode="json"), ensure_ascii=False).encode()
        )

    if size() <= 1000000:
        return value
    details = list(value.activity_details)
    for index in range(len(details) - 1, -1, -1):
        for field in ("output_preview", "input_preview", "tool_description"):
            old: ToolPreview | None = getattr(details[index], field)
            if old is None:
                continue
            values = details[index].model_dump(mode="json")
            values[field] = {
                "text": clip_text(old.text, 128),
                "truncated": True,
                "redacted": old.redacted,
            }
            details[index] = ActivityDetail.model_validate(values)
            value = value.model_copy(update={"activity_details": details})
            if size() <= 1000000:
                return value
    return value  # Original HTTP total budget still rejects an oversized base.
