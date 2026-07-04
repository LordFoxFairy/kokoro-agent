# GENERATED — DO NOT EDIT. Source: contract/spec/control.yaml
# Regenerate: python3 contract/generate.py
from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StringConstraints, TypeAdapter

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]

McpTransport = Literal["http", "streamable_http"]
SubagentCreate = Literal["deny", "ask", "allow"]
FilesystemPerm = Literal["read_only", "workspace_write"]
Backend = Literal["state", "local_shell", "e2b", "custom"]


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class RunInput(StrictModel):
    message_id: NonEmptyStr
    content: NonEmptyStr


class ModelConfig(StrictModel):
    provider: NonEmptyStr
    name: NonEmptyStr
    effort: NonEmptyStr | None = None


class SkillMount(StrictModel):
    name: NonEmptyStr
    path: NonEmptyStr
    lock: NonEmptyStr


class McpServer(StrictModel):
    name: NonEmptyStr
    transport: McpTransport
    url: NonEmptyStr
    allowed_tools: list[NonEmptyStr]
    timeout_s: int | None = None
    headers: dict[str, str] | None = None


class SubagentDef(StrictModel):
    name: NonEmptyStr
    description: str
    system_prompt: NonEmptyStr
    tools: list[NonEmptyStr]
    model: ModelConfig | None = None


class Permissions(StrictModel):
    approval_tools: list[NonEmptyStr]
    review_tools: list[NonEmptyStr]
    subagent_create: SubagentCreate
    filesystem: FilesystemPerm


class RuntimeConfig(StrictModel):
    model: ModelConfig
    system_prompt: NonEmptyStr | None = None
    tools: list[NonEmptyStr]
    skills: list[SkillMount]
    mcp: list[McpServer]
    subagents: list[SubagentDef]
    backend: Backend
    permissions: Permissions


class RuntimeContext(StrictModel):
    namespace: NonEmptyStr
    session_id: NonEmptyStr


class ApproveDecision(StrictModel):
    type: Literal["approve"]
    tool_id: NonEmptyStr
    args: dict[str, JsonValue] | None = None


class EditDecision(StrictModel):
    type: Literal["edit"]
    tool_id: NonEmptyStr
    args: dict[str, JsonValue]


class RejectDecision(StrictModel):
    type: Literal["reject"]
    tool_id: NonEmptyStr
    reason: str | None = None


class RespondDecision(StrictModel):
    type: Literal["respond"]
    tool_id: NonEmptyStr
    response: NonEmptyStr


ResumeDecision = Annotated[
    Union[ApproveDecision, EditDecision, RejectDecision, RespondDecision],
    Field(discriminator="type"),
]


class RunRequest(StrictModel):
    kind: Literal["run.request"]
    run_id: NonEmptyStr
    thread_id: NonEmptyStr
    input: RunInput
    runtime: RuntimeConfig
    context: RuntimeContext
    trace: dict[str, JsonValue] | None = None


class RunResume(StrictModel):
    kind: Literal["run.resume"]
    run_id: NonEmptyStr
    thread_id: NonEmptyStr
    decisions: Annotated[list[ResumeDecision], Field(min_length=1)]


class RunCancel(StrictModel):
    kind: Literal["run.cancel"]
    run_id: NonEmptyStr
    thread_id: NonEmptyStr


class RunSteer(StrictModel):
    kind: Literal["run.steer"]
    run_id: NonEmptyStr
    thread_id: NonEmptyStr
    message_id: NonEmptyStr
    content: NonEmptyStr


InboundMessage = Annotated[
    Union[RunRequest, RunResume, RunCancel, RunSteer],
    Field(discriminator="kind"),
]

inbound_adapter: TypeAdapter[InboundMessage] = TypeAdapter(InboundMessage)
