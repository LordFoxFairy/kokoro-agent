"""Private previews of exact native file calls, not filesystem reads or execution authority."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from pydantic import TypeAdapter, ValidationError

from kokoro_agent.domain.chat.projection import ToolDisplayDetail, safe_progress_id
from kokoro_agent.domain.chat.tool_display_preview import ActivityDetail, preview

NATIVE_FILE_CODES = {
    "ls": "tool.execution",
    "glob": "tool.execution",
    "grep": "tool.execution",
    "read_file": "tool.file.read",
    "write_file": "tool.file.write",
    "edit_file": "tool.execution",
}
_ARGUMENTS = {
    "ls": {"path"},
    "glob": {"pattern", "path"},
    "grep": {"pattern", "path", "glob", "output_mode"},
    "read_file": {"file_path", "offset", "limit"},
    "write_file": {"file_path", "content"},
    "edit_file": {"file_path", "old_string", "new_string", "replace_all"},
}


def frozen_native_tools(peers: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    """StateBackend proves virtual files; possible human substitutions stay unknown.

    Root checkpoint messages may belong to any peer. Intersect their authority;
    absent fields never mean permission. Native file names are factory-reserved.
    """
    allowed: set[str] = set(NATIVE_FILE_CODES) if peers else set()
    for peer in peers:
        if peer.get("backend") != {"kind": "state"}:
            return ()
        try:
            permissions = TypeAdapter(dict[str, Any]).validate_python(
                peer.get("permissions"), strict=True
            )
            for value in (
                permissions.get("approval_tools"),
                permissions.get("review_tools"),
                peer.get("pause_tools"),
            ):
                names = TypeAdapter(list[str]).validate_python(value, strict=True)
                allowed.difference_update(names)
        except ValidationError:
            return ()
    return tuple(sorted(allowed))


def native_activity_details(
    scope: tuple[str, str, str, str],
    activities: Mapping[str, Mapping[str, Any]],
    messages: Sequence[BaseMessage],
    allowed: tuple[str, ...],
) -> list[ActivityDetail]:
    """Pair original typed call/result with the existing opaque activity identity.

    The caller proves same-Run checkpoint scope and no approval/review rewrite.
    Ambiguous IDs, revisions or returns never select a convenient first/last value.
    """
    calls: dict[str, tuple[str, str, dict[str, Any]]] = {}
    outputs: dict[str, ToolMessage] = {}
    conflicts: set[str] = set()
    for message in messages:
        if isinstance(message, AIMessage) and message.id:
            ids = [call.get("id") for call in message.tool_calls]
            conflicts.update(
                call_id for call_id in ids if call_id and ids.count(call_id) > 1
            )
            for call in message.tool_calls:
                call_id = call.get("id")
                if not call_id:
                    continue
                value = (message.id, call["name"], call["args"])
                if call_id in calls and calls[call_id] != value:
                    conflicts.add(call_id)
                calls[call_id] = value
        elif isinstance(message, ToolMessage):
            call_id = message.tool_call_id
            old = outputs.get(call_id)
            if old is not None and (
                old.name,
                old.status,
                old.model_dump().get("content"),
            ) != (
                message.name,
                message.status,
                message.model_dump().get("content"),
            ):
                conflicts.add(call_id)
            outputs[call_id] = message
    result: list[ActivityDetail] = []
    for call_id, (_, name, args) in calls.items():
        if call_id in conflicts or name not in allowed or name not in NATIVE_FILE_CODES:
            continue
        activity_id = "act_" + safe_progress_id(
            "tool-activity", *scope, call_id, call_id
        )
        activity = activities.get(activity_id)
        output = outputs.get(call_id)
        if (
            activity is None
            or activity.get("display_code") != NATIVE_FILE_CODES[name]
            or output is None
            or output.name != name
            or (activity.get("status"), output.status)
            not in {("completed", "success"), ("failed", "error")}
        ):
            continue
        values: dict[str, Any] = {"activity_id": activity_id, "tool_name": name}
        # Only the tool's public argument vocabulary; never model-only metadata.
        arguments = {
            key: value for key, value in args.items() if key in _ARGUMENTS[name]
        }
        input_preview = preview(arguments, limit=2048)
        if input_preview is not None:
            values["input_preview"] = input_preview
        # Native text only. Non-text blocks/artifacts are not arbitrary raw dumps.
        content = output.model_dump().get("content")
        if isinstance(content, str):
            output_preview = preview(content, limit=4096, plain=True)
            if output_preview is not None:
                values["output_preview"] = output_preview
        raw_target = activity.get("display_detail")
        if raw_target is not None and name in {"read_file", "write_file", "edit_file"}:
            target = ToolDisplayDetail.model_validate(raw_target)
            if target.kind == "file" and target.virtual_path == args.get("file_path"):
                values["target"] = target
        result.append(ActivityDetail.model_validate(values))
    return result
