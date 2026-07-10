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


SKILL_STATE_COLLECTION = "skill_state"
SKILL_STATE_UNIQUE: tuple[str, ...] = ("namespace", "name",)
SKILLS_COLLECTION = "skills"
SKILLS_UNIQUE: tuple[str, ...] = ("scope", "name",)

skill_state_doc_adapter: TypeAdapter[SkillStateDoc] = TypeAdapter(SkillStateDoc)
skills_doc_adapter: TypeAdapter[SkillDoc] = TypeAdapter(SkillDoc)
