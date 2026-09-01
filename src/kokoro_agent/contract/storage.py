# GENERATED — DO NOT EDIT. Source: contract/spec/storage.yaml
# Regenerate: python3 contract/generate.py
from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, StringConstraints, TypeAdapter

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]


def _int_from_bson_number(value: object) -> object:
    # BSON 数字模型：JS fixture 写者的整数可能落库为 double；整值 float 视为 int，其余交 strict 报错。
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


# 存储文档专用 int:跨语言写者容忍(仅此镜像;wire JSON 面仍纯 strict int)。
BsonInt = Annotated[int, BeforeValidator(_int_from_bson_number)]

SkillSource = Literal["deploy", "upload", "github"]
McpTransport = Literal["http", "streamable_http"]
ReviewStatus = Literal["pending", "approved", "rejected"]
DispatchStatus = Literal["pending", "claimed", "expired"]
EventReceiptStatus = Literal["persisted", "rejected", "superseded"]


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class SkillCard(StrictModel):
    name: NonEmptyStr
    description: NonEmptyStr
    content_hash: NonEmptyStr


class SkillFileEntry(StrictModel):
    path: NonEmptyStr
    size: BsonInt


class SkillDoc(StrictModel):
    scope: NonEmptyStr
    name: NonEmptyStr
    description: NonEmptyStr
    skill_md: NonEmptyStr
    files_manifest: list[SkillFileEntry]
    file_count: BsonInt
    package_size: BsonInt
    content_hash: NonEmptyStr
    package_ref: NonEmptyStr
    source: SkillSource
    revision: BsonInt
    display_weight: BsonInt | None = None
    pinned: bool | None = None
    category: NonEmptyStr | None = None
    review_status: ReviewStatus | None = None
    official_enabled: bool
    official_required: bool
    updated_at: BsonInt
    deleted_at: BsonInt | None = None


class SkillStateDoc(StrictModel):
    namespace: NonEmptyStr
    name: NonEmptyStr
    enabled: bool
    updated_at: BsonInt


class SkillRevisionDoc(StrictModel):
    scope: NonEmptyStr
    name: NonEmptyStr
    revision: BsonInt
    content_hash: NonEmptyStr
    package_size: BsonInt
    source: SkillSource
    created_at: BsonInt


class McpServerDoc(StrictModel):
    scope: NonEmptyStr
    name: NonEmptyStr
    revision: BsonInt
    transport: McpTransport
    url: NonEmptyStr
    allowed_tools: list[NonEmptyStr]
    secret_ref: NonEmptyStr | None
    enabled: bool
    updated_at: BsonInt
    deleted_at: BsonInt | None


class McpServerRevisionDoc(StrictModel):
    scope: NonEmptyStr
    name: NonEmptyStr
    revision: BsonInt
    config_hash: NonEmptyStr
    transport: McpTransport
    url: NonEmptyStr
    allowed_tools: list[NonEmptyStr]
    secret_ref: NonEmptyStr | None
    created_at: BsonInt


class RunDispatchDoc(StrictModel):
    run_id: NonEmptyStr
    session_id: NonEmptyStr
    namespace: NonEmptyStr
    fence: NonEmptyStr
    status: DispatchStatus
    deadline_at: BsonInt
    claimed_by: NonEmptyStr | None = None
    created_at: BsonInt
    updated_at: BsonInt


class McpSecretDoc(StrictModel):
    scope: NonEmptyStr
    handle: NonEmptyStr
    name: NonEmptyStr
    ciphertext: NonEmptyStr
    key_id: NonEmptyStr
    created_at: BsonInt
    deleted_at: BsonInt | None


class RunEventReceiptDoc(StrictModel):
    run_id: NonEmptyStr
    durable_seq: BsonInt
    event_id: NonEmptyStr
    status: EventReceiptStatus
    reason: NonEmptyStr | None = None
    created_at: BsonInt


class RunReceiptManifestDoc(StrictModel):
    run_id: NonEmptyStr
    persisted_seq: BsonInt
    projected_seq: BsonInt
    consumed_seq: BsonInt
    terminal_fence_seq: BsonInt | None = None
    superseded_from: BsonInt | None = None
    producer_close_requested: bool
    producer_closed: bool
    updated_at: BsonInt


MCP_SECRETS_COLLECTION = "mcp_secrets"
MCP_SECRETS_UNIQUE: tuple[str, ...] = ("scope", "handle",)
MCP_SERVER_REVISIONS_COLLECTION = "mcp_server_revisions"
MCP_SERVER_REVISIONS_UNIQUE: tuple[str, ...] = ("scope", "name", "revision",)
MCP_SERVERS_COLLECTION = "mcp_servers"
MCP_SERVERS_UNIQUE: tuple[str, ...] = ("scope", "name",)
RUN_DISPATCHES_COLLECTION = "run_dispatches"
RUN_DISPATCHES_UNIQUE: tuple[str, ...] = ("run_id",)
RUN_EVENT_RECEIPTS_COLLECTION = "run_event_receipts"
RUN_EVENT_RECEIPTS_UNIQUE: tuple[str, ...] = ("run_id", "durable_seq",)
RUN_RECEIPT_MANIFESTS_COLLECTION = "run_receipt_manifests"
RUN_RECEIPT_MANIFESTS_UNIQUE: tuple[str, ...] = ("run_id",)
SKILL_REVISIONS_COLLECTION = "skill_revisions"
SKILL_REVISIONS_UNIQUE: tuple[str, ...] = ("scope", "name", "content_hash",)
SKILL_STATE_COLLECTION = "skill_state"
SKILL_STATE_UNIQUE: tuple[str, ...] = ("namespace", "name",)
SKILLS_COLLECTION = "skills"
SKILLS_UNIQUE: tuple[str, ...] = ("scope", "name",)

mcp_secrets_doc_adapter: TypeAdapter[McpSecretDoc] = TypeAdapter(McpSecretDoc)
mcp_server_revisions_doc_adapter: TypeAdapter[McpServerRevisionDoc] = TypeAdapter(McpServerRevisionDoc)
mcp_servers_doc_adapter: TypeAdapter[McpServerDoc] = TypeAdapter(McpServerDoc)
run_dispatches_doc_adapter: TypeAdapter[RunDispatchDoc] = TypeAdapter(RunDispatchDoc)
run_event_receipts_doc_adapter: TypeAdapter[RunEventReceiptDoc] = TypeAdapter(RunEventReceiptDoc)
run_receipt_manifests_doc_adapter: TypeAdapter[RunReceiptManifestDoc] = TypeAdapter(RunReceiptManifestDoc)
skill_revisions_doc_adapter: TypeAdapter[SkillRevisionDoc] = TypeAdapter(SkillRevisionDoc)
skill_state_doc_adapter: TypeAdapter[SkillStateDoc] = TypeAdapter(SkillStateDoc)
skills_doc_adapter: TypeAdapter[SkillDoc] = TypeAdapter(SkillDoc)


WORKSPACE_KEY_TEMPLATE = "{namespace}:{session_id}"


def workspace_key(namespace: str, session_id: str) -> str:
    """会话工作区键（本地目录名 / S3 归档前缀）：单源模板，双语言同构，禁手拼。"""
    return WORKSPACE_KEY_TEMPLATE.format(namespace=namespace, session_id=session_id)
