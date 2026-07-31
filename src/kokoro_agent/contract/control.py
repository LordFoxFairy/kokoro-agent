# GENERATED — DO NOT EDIT. Source: contract/spec/control.yaml
# Regenerate: python3 contract/generate.py
from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, JsonValue, StringConstraints, TypeAdapter

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]

def _trimmed_reference(value: str) -> str:
    if value.strip() != value:
        raise ValueError("reference must not have surrounding whitespace")
    return value

Sha256Str = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
Reference = Annotated[str, StringConstraints(min_length=1, max_length=256), AfterValidator(_trimmed_reference)]
OpaqueRuntimeHandle = Annotated[str, StringConstraints(min_length=32, max_length=8192), AfterValidator(_trimmed_reference)]

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
    authorization_handle: Reference
    effort: NonEmptyStr | None = None
    thinking: bool | None = None


class SkillGrant(StrictModel):
    option_ref: Reference
    name: NonEmptyStr
    content_hash: NonEmptyStr
    description: NonEmptyStr
    scope: NonEmptyStr


class McpGrant(StrictModel):
    option_ref: Reference
    scope: NonEmptyStr
    name: NonEmptyStr
    revision: int
    config_hash: NonEmptyStr


class Permissions(StrictModel):
    approval_tools: list[NonEmptyStr]
    review_tools: list[NonEmptyStr]
    subagent_create: SubagentCreate
    filesystem: FilesystemPerm


class MediaRuntimeGrant(StrictModel):
    media_access_handle: OpaqueRuntimeHandle
    media_projection_reservation_handle: OpaqueRuntimeHandle


class RuntimeConfig(StrictModel):
    agent_catalog_ref: Reference
    agent_type: AgentType
    agent: NonEmptyStr | None = None
    model: ModelConfig
    tools: list[NonEmptyStr]
    skills: list[SkillGrant]
    mcp_servers: list[McpGrant]
    subagents: list[NonEmptyStr]
    backend: Backend
    permissions: Permissions
    media: MediaRuntimeGrant | None = None


class RuntimeContext(StrictModel):
    namespace: NonEmptyStr
    session_id: NonEmptyStr


class ExecutionContextIntentRoot(StrictModel):
    mode: Literal["root"]


class ExecutionContextIntentContinue(StrictModel):
    parent_anchor: Reference
    parent_digest: Sha256Str
    mode: Literal["continue"]


class ExecutionContextIntentFork(StrictModel):
    parent_anchor: Reference
    parent_digest: Sha256Str
    mode: Literal["fork"]


ExecutionContextIntent = Annotated[
    Union[ExecutionContextIntentRoot, ExecutionContextIntentContinue, ExecutionContextIntentFork],
    Field(discriminator="mode"),
]


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


class SubmitDecision(StrictModel):
    type: Literal["submit"]
    request_id: NonEmptyStr
    value: dict[str, JsonValue]


ResumeDecision = Annotated[
    Union[ApproveDecision, EditDecision, RejectDecision, RespondDecision, SubmitDecision],
    Field(discriminator="type"),
]


class RunRequest(StrictModel):
    kind: Literal["run.request"]
    run_id: NonEmptyStr
    thread_id: NonEmptyStr
    input: RunInput
    runtime: RuntimeConfig
    context: RuntimeContext
    execution_context: ExecutionContextIntent
    trace: dict[str, JsonValue] | None = None


class RunResume(StrictModel):
    kind: Literal["run.resume"]
    run_id: NonEmptyStr
    thread_id: NonEmptyStr
    decision_id: NonEmptyStr
    decisions: Annotated[list[ResumeDecision], Field(min_length=1)]


class RunCancel(StrictModel):
    kind: Literal["run.cancel"]
    run_id: NonEmptyStr
    thread_id: NonEmptyStr
    decision_id: NonEmptyStr


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
