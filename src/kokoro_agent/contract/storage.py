# GENERATED — DO NOT EDIT. Source: contract/spec/storage.yaml
# Regenerate: python3 contract/generate.py
from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, StringConstraints, TypeAdapter

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]

SkillSource = Literal["deploy", "upload", "github"]


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class SkillCard(StrictModel):
    name: NonEmptyStr
    description: NonEmptyStr
    content_hash: NonEmptyStr


class SkillFileEntry(StrictModel):
    path: NonEmptyStr
    size: int


class SkillDoc(StrictModel):
    scope: NonEmptyStr
    name: NonEmptyStr
    description: NonEmptyStr
    skill_md: NonEmptyStr
    files_manifest: list[SkillFileEntry]
    file_count: int
    package_size: int
    content_hash: NonEmptyStr
    package_ref: NonEmptyStr
    source: SkillSource
    revision: int
    official_enabled: bool
    official_required: bool
    updated_at: int
    deleted_at: int | None = None


class SkillStateDoc(StrictModel):
    namespace: NonEmptyStr
    name: NonEmptyStr
    enabled: bool
    updated_at: int


class SkillRevisionDoc(StrictModel):
    scope: NonEmptyStr
    name: NonEmptyStr
    revision: int
    content_hash: NonEmptyStr
    package_size: int
    source: SkillSource
    created_at: int


SKILL_REVISIONS_COLLECTION = "skill_revisions"
SKILL_REVISIONS_UNIQUE: tuple[str, ...] = ("scope", "name", "content_hash",)
SKILL_STATE_COLLECTION = "skill_state"
SKILL_STATE_UNIQUE: tuple[str, ...] = ("namespace", "name",)
SKILLS_COLLECTION = "skills"
SKILLS_UNIQUE: tuple[str, ...] = ("scope", "name",)

skill_revisions_doc_adapter: TypeAdapter[SkillRevisionDoc] = TypeAdapter(SkillRevisionDoc)
skill_state_doc_adapter: TypeAdapter[SkillStateDoc] = TypeAdapter(SkillStateDoc)
skills_doc_adapter: TypeAdapter[SkillDoc] = TypeAdapter(SkillDoc)


WORKSPACE_KEY_TEMPLATE = "{namespace}:{session_id}"


def workspace_key(namespace: str, session_id: str) -> str:
    """会话工作区键（本地目录名 / S3 归档前缀）：单源模板，双语言同构，禁手拼。"""
    return WORKSPACE_KEY_TEMPLATE.format(namespace=namespace, session_id=session_id)
