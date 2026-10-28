"""Allowlisted projection from execution payloads to user-visible chat facts."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    TypeAdapter,
    field_validator,
    model_validator,
)
from typing import Self

from kokoro_agent.domain.chat.display_text import (
    public_action_title,
    sensitive_display_text,
)
from kokoro_agent.domain.chat.models import (
    ChatEventDraft,
    ChatMessageDraft,
    ChatProjection,
    assistant_message_id,
)
from kokoro_agent.protocol import (
    ArtifactKind,
    ChatFailure,
    DeliveryCreatedPayload,
    MessageCompletedPayload,
    MessageDeltaPayload,
    RunCompletedPayload,
    RunFailedPayload,
    RunStartedPayload,
    SubagentFinishedPayload,
    SubagentStartedPayload,
    ToolAwaitingApprovalPayload,
    ToolInvokedPayload,
    ToolReturnedPayload,
    TodoUpdatedPayload,
    SkillProgressPayload,
)


class _Payload(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class _RunPhase(_Payload):
    status: Literal["running"]


class _AssistantDelta(_Payload):
    delta: str


class _AssistantCompleted(_Payload):
    content: str


ToolDisplayCode = Literal[
    "tool.execution",
    "tool.file.read",
    "tool.mcp.discover",
    "tool.mcp.describe",
    "tool.mcp.call",
    "tool.web.search",
    "tool.web.fetch",
    "tool.file.write",
    "tool.file.deliver",
]


class ToolDisplayDetail(_Payload):
    kind: Literal["file", "mcp_tool"]
    name: str
    virtual_path: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        if not safe_display_name(value):
            raise ValueError("invalid public display name")
        if sensitive_display_text(value):
            raise ValueError("sensitive public display name")
        return value

    @model_validator(mode="after")
    def file_basename(self) -> Self:
        if self.kind == "file" and (
            self.name in {".", ".."} or any(c in self.name for c in "/\\:")
        ):
            raise ValueError("display file must be a basename")
        if "virtual_path" in self.model_fields_set and (
            self.kind != "file"
            or self.virtual_path is None
            or not safe_virtual_path(self.virtual_path)
            or self.virtual_path.rsplit("/", 1)[-1] != self.name
        ):
            raise ValueError("invalid public virtual path")
        return self


def safe_display_name(value: str) -> bool:
    return (
        1 <= len(value) <= 120
        and unicodedata.normalize("NFC", value) == value
        and not any(unicodedata.category(c) in {"Cc", "Cf", "Cs"} for c in value)
        and len(value.encode("utf-8")) <= 480
        and bool(value.strip())
    )


def safe_virtual_path(value: str) -> bool:
    if (
        not value.startswith("/")
        or len(value.encode("utf-8", errors="surrogatepass")) > 4096
        or unicodedata.normalize("NFC", value) != value
        or any(unicodedata.category(c) in {"Cc", "Cf", "Cs"} for c in value)
        or any(c in value for c in "\\:?#%")
        or value.startswith(("//", "~"))
    ):
        return False
    parts = value.removeprefix("/").split("/")
    return (
        not any(part in {"", ".", ".."} or part.startswith("~") for part in parts)
        and parts[0].casefold()
        not in {"users", "home", "private", "etc", "proc", "sys", "var"}
        and not sensitive_display_text(value)
    )


class SkillDisplay(_Payload):
    source_ref: str
    name: str
    virtual_path: str
    revision: str | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def exact_native_source(self) -> Self:
        if (
            re.fullmatch(r"skill:[A-Za-z0-9][A-Za-z0-9._:-]{0,190}", self.source_ref)
            is None
        ):
            raise ValueError("invalid display Skill source")
        segment = (
            base64.urlsafe_b64encode(self.source_ref[6:].encode("ascii"))
            .decode("ascii")
            .rstrip("=")
        )
        if self.virtual_path != f"/.skills/{segment}/SKILL.md" or not safe_virtual_path(
            self.virtual_path
        ):
            raise ValueError("Skill display path differs from source")
        if not safe_display_name(self.name) or sensitive_display_text(self.name):
            raise ValueError("invalid display Skill name")
        if "revision" in self.model_fields_set and (
            self.revision is None
            or re.fullmatch(r"[1-9][0-9]{0,18}", self.revision) is None
            or int(self.revision) > 9223372036854775807
        ):
            raise ValueError("invalid display Skill revision")
        return self


@dataclass(frozen=True, slots=True, kw_only=True)
class ToolDisplaySource:
    """Factory-observed source; never constructed from event metadata."""

    name: str
    code: ToolDisplayCode
    origin: Literal["native", "delivery", "mcp", "web"]
    file_scope: Literal["none", "virtual"] = "none"
    skill_prefixes: tuple[str, ...] = ()
    mcp_names: tuple[tuple[str, str, str], ...] = ()

    def shared_with(self, other: ToolDisplaySource) -> ToolDisplaySource | None:
        """Keep only facts independently proved by every possible native peer."""
        if self == other:
            return self
        if self.origin != "mcp" or replace(self, mcp_names=()) != replace(
            other, mcp_names=()
        ):
            return None
        names = tuple(sorted(set(self.mcp_names).intersection(other.mcp_names)))
        return replace(self, mcp_names=names) if names else None

    def detail(self, args: dict[str, JsonValue]) -> ToolDisplayDetail | None:
        value: object = None
        virtual_path: str | None = None
        kind: Literal["file", "mcp_tool"]
        if self.origin in {"native", "delivery"} and self.code in {
            "tool.file.read",
            "tool.file.write",
            "tool.file.deliver",
        }:
            value = args.get("path" if self.origin == "delivery" else "file_path")
            if (
                type(value) is not str
                or not value
                or len(value) > 4096
                or any(unicodedata.category(c) in {"Cc", "Cf", "Cs"} for c in value)
            ):
                return None
            if (
                len(value.encode("utf-8")) > 4096
                or unicodedata.normalize("NFC", value) != value
            ):
                return None
            if any(c in value for c in "\\:?#%") or value.startswith(("//", "~")):
                return None
            parts = value.removeprefix("/").split("/")
            if any(part in {"", ".", ".."} or part.startswith("~") for part in parts):
                return None
            if parts[0].casefold() in {
                "users",
                "home",
                "private",
                "etc",
                "proc",
                "sys",
                "var",
            } or sensitive_display_text(value):
                return None
            if parts[0] == ".skills":
                if self.code != "tool.file.read" or not any(
                    value.startswith(prefix) for prefix in self.skill_prefixes
                ):
                    return None
            elif self.file_scope != "virtual":
                return None
            virtual_path = value if safe_virtual_path(value) else None
            value, kind = parts[-1], "file"
        elif self.origin == "mcp" and self.code in {
            "tool.mcp.describe",
            "tool.mcp.call",
        }:
            names = [
                label
                for connection, selector, label in self.mcp_names
                if connection == args.get("server") and selector == args.get("tool")
            ]
            if len(names) != 1:
                return None
            value, kind = names[0], "mcp_tool"
        else:
            return None
        try:
            return ToolDisplayDetail(
                kind=kind,
                name=value,
                **({"virtual_path": virtual_path} if virtual_path is not None else {}),
            )
        except ValueError:
            return None


class ToolActivity(_Payload):
    activity: Literal["tool"] = "tool"
    activity_id: str
    segment_id: str
    status: Literal["running", "completed", "failed"]
    display_code: ToolDisplayCode
    display_detail: ToolDisplayDetail | None = None
    action_title: str | None = None

    @model_validator(mode="after")
    def detail_matches_code(self) -> Self:
        if (
            self.action_title is not None
            and public_action_title(self.action_title) is None
        ):
            raise ValueError("invalid public action title")
        if self.display_detail is not None:
            allowed = {
                "file": {"tool.file.read", "tool.file.write", "tool.file.deliver"},
                "mcp_tool": {"tool.mcp.describe", "tool.mcp.call"},
            }
            if self.display_code not in allowed[self.display_detail.kind]:
                raise ValueError("display detail differs from the tool category")
        return self


def read_tool_activity(payload_json: str) -> ToolActivity:
    # Both original opaque-only activities and the new bounded detail fit here.
    if len(payload_json) > 8192 or len(payload_json.encode("utf-8")) > 8192:
        raise ValueError("safe activity history exceeds its bounded shape")

    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        values: dict[str, object] = {}
        for key, value in pairs:
            if key in values:
                raise ValueError("duplicate safe activity member")
            values[key] = value
        return values

    def constant(_value: str) -> object:
        raise ValueError("non-finite safe activity value")

    decoded: object = json.loads(
        payload_json, object_pairs_hook=unique, parse_constant=constant
    )
    value = TypeAdapter(dict[str, object]).validate_python(decoded, strict=True)
    if not {
        "activity",
        "activity_id",
        "segment_id",
        "status",
        "display_code",
    }.issubset(value):
        raise ValueError("safe activity history is missing canonical base fields")
    if any(
        key in value and value[key] is None
        for key in ("display_detail", "action_title")
    ):
        raise ValueError("null safe activity detail")
    return ToolActivity.model_validate(value, strict=True)


class _SubagentActivity(_Payload):
    activity: Literal["subagent"] = "subagent"
    activity_id: str
    segment_id: str
    status: Literal["running", "completed", "failed"]
    display_code: Literal["subagent.execution"] = "subagent.execution"


class _SkillActivity(_Payload):
    activity: Literal["skill"] = "skill"
    activity_id: str
    preflight_id: str
    source_refs: list[str]
    phase: Literal["resolving", "loading", "ready", "failed"]
    error_code: Literal["skill_resolve_failed", "skill_load_failed"] | None = None
    display_skills: list[SkillDisplay] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def exact_displays(self) -> Self:
        if self.display_skills is not None:
            refs = [item.source_ref for item in self.display_skills]
            if (
                self.phase != "ready"
                or len(refs) > 16
                or len(set(refs)) != len(refs)
                or not set(refs).issubset(self.source_refs)
            ):
                raise ValueError("Skill display differs from ready selection")
        return self


class _Delivery(_Payload):
    tool_call_id: str
    artifact_id: str
    asset_id: str
    artifact_kind: ArtifactKind
    path: str
    title: str
    mime: str
    size: int
    content_hash: str
    note: str | None = None


class _Terminal(_Payload):
    status: Literal["completed", "cancelled"]
    token_usage: dict[str, JsonValue] | None = None


ProjectablePayload = (
    RunStartedPayload
    | MessageDeltaPayload
    | MessageCompletedPayload
    | ToolInvokedPayload
    | ToolReturnedPayload
    | ToolAwaitingApprovalPayload
    | SubagentStartedPayload
    | SubagentFinishedPayload
    | DeliveryCreatedPayload
    | RunCompletedPayload
    | RunFailedPayload
    | BaseModel
)


def safe_progress_id(
    tag: str, tenant_id: str, namespace: str, session_id: str, run_id: str, *parts: str
) -> str:
    value = [
        "kokoro-agent.safe-progress.v1",
        tag,
        tenant_id,
        namespace,
        session_id,
        run_id,
        *parts,
    ]
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8", errors="strict")
    return hashlib.sha256(encoded).hexdigest()


def project_chat_fact(
    *,
    tenant_id: str,
    namespace: str,
    session_id: str,
    run_id: str,
    source_index: int,
    created_at: datetime,
    payload: ProjectablePayload,
    tool_display_code: ToolDisplayCode = "tool.execution",
    tool_display_detail: ToolDisplayDetail | None = None,
    tool_action_title: str | None = None,
    skill_display_skills: Sequence[Mapping[str, JsonValue]] | None = None,
) -> ChatProjection | None:
    """Project only allowlisted product semantics; unknown/private payloads disappear."""

    if skill_display_skills is not None and not isinstance(
        payload, SkillProgressPayload
    ):
        raise ValueError("Skill display requires Skill progress")
    event_type: str
    chat_message_id: str | None = None
    safe_payload: _Payload | ChatFailure | TodoUpdatedPayload
    message: ChatMessageDraft | None = None

    def opaque(tag: str, *parts: str) -> str:
        return safe_progress_id(tag, tenant_id, namespace, session_id, run_id, *parts)

    if isinstance(payload, TodoUpdatedPayload):
        event_type = "todo.updated"
        safe_payload = TodoUpdatedPayload.model_validate(
            payload.model_dump(mode="json")
        )
    elif isinstance(payload, SkillProgressPayload):
        event_type = "activity"
        safe_payload = _SkillActivity(
            activity_id="act_" + opaque("skill-activity"),
            preflight_id="spf_"
            + opaque("skill-preflight", str(payload.anchor_source_index)),
            source_refs=payload.source_refs,
            phase=payload.phase,
            error_code=payload.error_code,
            display_skills=[
                SkillDisplay.model_validate(item, strict=True)
                for item in skill_display_skills
            ]
            if skill_display_skills is not None
            else None,
        )
    elif isinstance(payload, RunStartedPayload):
        event_type = "run.started"
        safe_payload = _RunPhase(status="running")
    elif isinstance(payload, MessageDeltaPayload):
        event_type = "assistant.delta"
        chat_message_id = assistant_message_id(namespace, run_id, payload.segment_id)
        safe_payload = _AssistantDelta(delta=payload.delta)
    elif isinstance(payload, MessageCompletedPayload):
        event_type = "assistant.completed"
        chat_message_id = assistant_message_id(namespace, run_id, payload.segment_id)
        safe_payload = _AssistantCompleted(content=payload.content)
        message = ChatMessageDraft(
            chat_message_id=chat_message_id,
            tenant_id=tenant_id,
            namespace=namespace,
            session_id=session_id,
            run_id=run_id,
            role="assistant",
            content=payload.content,
            status="completed",
            created_at=created_at,
            updated_at=created_at,
        )
    elif isinstance(payload, ToolInvokedPayload | ToolReturnedPayload):
        event_type = "activity"
        safe_payload = ToolActivity(
            display_code=tool_display_code,
            display_detail=tool_display_detail,
            action_title=tool_action_title,
            activity_id="act_"
            + opaque("tool-activity", payload.segment_id, payload.tool_id),
            segment_id="seg_" + opaque("segment", payload.segment_id),
            status="running"
            if isinstance(payload, ToolInvokedPayload)
            else "failed"
            if payload.is_error
            else "completed",
        )
    elif isinstance(payload, ToolAwaitingApprovalPayload):
        # Partial execution notifications cannot supply the committed collection,
        # pause identity or revision. Only the Run transaction writes this source.
        raise ValueError("interaction source requires a complete durable pause")
    elif isinstance(payload, SubagentStartedPayload | SubagentFinishedPayload):
        event_type = "activity"
        safe_payload = _SubagentActivity(
            activity_id="act_"
            + opaque("subagent-activity", payload.segment_id, payload.subagent_id),
            segment_id="seg_" + opaque("segment", payload.segment_id),
            status="running"
            if isinstance(payload, SubagentStartedPayload)
            else "failed"
            if payload.failed
            else "completed",
        )
    elif isinstance(payload, DeliveryCreatedPayload):
        event_type = "delivery"
        safe_payload = _Delivery(**payload.model_dump())
    elif isinstance(payload, RunCompletedPayload):
        event_type = "run.completed"
        safe_payload = _Terminal(
            status=payload.status,
            token_usage=None
            if payload.token_usage is None
            else payload.token_usage.model_dump(mode="json"),
        )
    elif isinstance(payload, RunFailedPayload):
        event_type = "run.failed"
        safe_payload = ChatFailure(
            status="failed", code=payload.code, retryable=payload.retryable
        )
    else:
        return None
    return ChatProjection(
        event=ChatEventDraft(
            tenant_id=tenant_id,
            namespace=namespace,
            session_id=session_id,
            run_id=run_id,
            source_index=source_index,
            chat_message_id=chat_message_id,
            event_type=event_type,
            payload_json=safe_payload.canonical_bytes().decode("utf-8")
            if isinstance(safe_payload, TodoUpdatedPayload)
            else safe_payload.model_dump_json(exclude_none=True),
            created_at=created_at,
        ),
        message=message,
    )


__all__ = ["project_chat_fact"]
