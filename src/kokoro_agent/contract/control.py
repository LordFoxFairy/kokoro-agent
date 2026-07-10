# GENERATED — DO NOT EDIT. Source: contract/spec/control.yaml
# Regenerate: python3 contract/generate.py
from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StringConstraints, TypeAdapter

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]

SubagentCreate = Literal["deny", "ask", "allow"]
FilesystemPerm = Literal["read_only", "workspace_write"]
AgentType = Literal["general"]
Backend = Literal["state", "local_shell", "docker", "e2b", "custom"]


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class RunInput(StrictModel):
    message_id: NonEmptyStr
    content: NonEmptyStr


class ModelConfig(StrictModel):
    provider: NonEmptyStr
    name: NonEmptyStr
    effort: NonEmptyStr | None = None
    thinking: bool | None = None


class Permissions(StrictModel):
    approval_tools: list[NonEmptyStr]
    review_tools: list[NonEmptyStr]
    subagent_create: SubagentCreate
    filesystem: FilesystemPerm


class RuntimeConfig(StrictModel):
    agent_type: AgentType
    agent: NonEmptyStr | None = None
    model: ModelConfig
    tools: list[NonEmptyStr]
    skills: list[NonEmptyStr]
    mcp_servers: list[NonEmptyStr]
    subagents: list[NonEmptyStr]
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
