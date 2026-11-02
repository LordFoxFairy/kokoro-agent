"""GA control frames.

The public launch frame carries a product ``feature_key``, a trusted execution
identity, and explicit exact Skill and MCP Connection selections. Agent configuration
belongs to the worker-local Feature catalog; it is never supplied by a caller
or persisted as request ``runtime`` data.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Annotated, Literal, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    TypeAdapter,
    field_validator,
    model_validator,
)

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


IdentityKind = Literal["user", "project", "service"]
_SKILL_SOURCE_REF = re.compile(r"skill:[A-Za-z0-9][A-Za-z0-9._:-]{0,190}\Z")
_MAX_SKILL_SOURCE_REFS = 16
_MAX_SKILL_SELECTION_BYTES = 4096
_SKILL_SOURCE_REFS_ADAPTER: TypeAdapter[list[str] | tuple[str, ...]] = TypeAdapter(
    list[str] | tuple[str, ...]
)
_MCP_CONNECTION_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,190}\Z")
_MCP_CONNECTION_IDS_ADAPTER: TypeAdapter[list[str] | tuple[str, ...]] = TypeAdapter(
    list[str] | tuple[str, ...]
)


class IdentityRef(StrictModel):
    """Opaque IAM-owned identity reference; GA does not interpret its value."""

    kind: IdentityKind
    opaque_ref: NonEmptyStr


class ExecutionIdentity(StrictModel):
    """Who acts, for which tenancy, and under which IAM assertion."""

    tenant_ref: NonEmptyStr
    actor: IdentityRef
    subject: IdentityRef
    identity_assertion_ref: NonEmptyStr


class RunInput(StrictModel):
    message_id: NonEmptyStr
    content: NonEmptyStr


_PersonalRevision = Annotated[int, Field(ge=1, le=9007199254740991)]


class PersonalMaterialModel(StrictModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        frozen=True,
        hide_input_in_errors=True,
        revalidate_instances="always",
    )

    @field_validator("*", mode="after")
    @classmethod
    def _scalar_text(cls, value: object) -> object:
        if isinstance(value, str) and (
            "\x00" in value or any(0xD800 <= ord(char) <= 0xDFFF for char in value)
        ):
            raise ValueError("invalid personal material text")
        return value


class PersonalContextOwner(PersonalMaterialModel):
    tenant_ref: NonEmptyStr
    user_ref: NonEmptyStr


class PersonalProfile(PersonalMaterialModel):
    revision: _PersonalRevision
    nickname: Annotated[str, StringConstraints(max_length=128)]
    occupation: Annotated[str, StringConstraints(max_length=256)]
    about: Annotated[str, StringConstraints(max_length=2000)]
    instructions: Annotated[str, StringConstraints(max_length=8000)]


class PersonalKnowledgeItem(PersonalMaterialModel):
    knowledge_id: Annotated[
        str,
        StringConstraints(
            pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        ),
    ]
    revision: _PersonalRevision
    name: Annotated[str, StringConstraints(min_length=1, max_length=500)]
    when_to_use: Annotated[str, StringConstraints(min_length=1, max_length=500)]
    content: Annotated[str, StringConstraints(min_length=1, max_length=2000)]

    @field_validator("knowledge_id")
    @classmethod
    def _id(cls, value: str) -> str:
        if (
            re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                value,
            )
            is None
        ):
            raise ValueError("invalid personal knowledge id")
        return value

    @field_validator("name", "when_to_use", "content")
    @classmethod
    def _nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("personal knowledge text must not be blank")
        return value


_PERSONAL_KNOWLEDGE_ITEMS: TypeAdapter[
    list[PersonalKnowledgeItem] | tuple[PersonalKnowledgeItem, ...]
] = TypeAdapter(list[PersonalKnowledgeItem] | tuple[PersonalKnowledgeItem, ...])


class PersonalKnowledge(PersonalMaterialModel):
    usage_revision: Annotated[int, Field(ge=0, le=9007199254740991)]
    enabled: bool
    items: tuple[PersonalKnowledgeItem, ...] = Field(max_length=100)

    @field_validator("items", mode="before")
    @classmethod
    def _items(cls, value: object) -> tuple[PersonalKnowledgeItem, ...]:
        return tuple(_PERSONAL_KNOWLEDGE_ITEMS.validate_python(value, strict=True))

    @model_validator(mode="after")
    def _state(self) -> PersonalKnowledge:
        if (not self.enabled and self.items) or (
            self.enabled and self.usage_revision == 0
        ):
            raise ValueError("invalid personal knowledge usage snapshot")
        if len({item.knowledge_id for item in self.items}) != len(self.items):
            raise ValueError("duplicate personal knowledge item")
        return self


class PersonalContext(PersonalMaterialModel):
    schema_version: Literal[1]
    owner: PersonalContextOwner
    profile: PersonalProfile | None
    knowledge: PersonalKnowledge

    @field_validator("schema_version", mode="before")
    @classmethod
    def _version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("invalid personal context schema version")
        return value


class CustomApiProfileSelection(StrictModel):
    """Safe exact Platform intent, never descriptions, credentials or authority."""

    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, revalidate_instances="always"
    )
    profile_id: Annotated[str, StringConstraints(min_length=1, max_length=191)]
    expected_state_version: Annotated[int, Field(ge=1, le=2147483647)]

    @field_validator("profile_id")
    @classmethod
    def _validate_profile_id(cls, value: str) -> str:
        if _MCP_CONNECTION_ID.fullmatch(value) is None:
            raise ValueError("invalid CustomAPI profile_id")
        return value


_CUSTOM_API_SELECTIONS_ADAPTER: TypeAdapter[
    list[CustomApiProfileSelection] | tuple[CustomApiProfileSelection, ...]
] = TypeAdapter(list[CustomApiProfileSelection] | tuple[CustomApiProfileSelection, ...])


class RunRequest(StrictModel):
    """Frozen business selections; the trusted Feature still owns the runtime graph."""

    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, hide_input_in_errors=True
    )

    kind: Literal["run.request"]
    # HTTP ingress supplies request_id. Direct worker callers may omit it;
    # run_id remains the execution idempotency key in RunRepository.
    request_id: NonEmptyStr | None = None
    run_id: NonEmptyStr
    session_id: NonEmptyStr
    feature_key: NonEmptyStr
    project_ref: NonEmptyStr | None
    selected_skill_source_refs: tuple[str, ...]
    execution_identity: ExecutionIdentity
    input: RunInput
    requested_model_label: NonEmptyStr | None = None
    trace: dict[str, JsonValue] | None = None
    # Omit only the new empty field: persisted pre-7.4 request JSON and fences
    # must remain byte-identical. Nonempty choices participate in both as usual.
    selected_mcp_connection_ids: tuple[str, ...] = Field(
        default=(), exclude_if=lambda refs: not refs
    )

    # Preserve pre-8.10 persisted bytes/fences when omitted or explicitly empty.
    selected_custom_api_profiles: tuple[CustomApiProfileSelection, ...] = Field(
        default=(), max_length=32, exclude_if=lambda refs: not refs
    )

    # None is omitted so pre-8.11 durable JSON and idempotency fences stay exact.
    personal_context: PersonalContext | None = Field(
        default=None, exclude_if=lambda value: value is None, repr=False
    )

    @model_validator(mode="after")
    def _personal_owner(self) -> RunRequest:
        context = self.personal_context
        identity = self.execution_identity
        if context is not None and (
            identity.actor.kind != "user"
            or context.owner.tenant_ref != identity.tenant_ref
            or context.owner.user_ref != identity.actor.opaque_ref
        ):
            raise ValueError("personal context owner differs from trusted actor")
        return self

    @field_validator("selected_custom_api_profiles", mode="before")
    @classmethod
    def _parse_custom_api_profiles(
        cls, value: object
    ) -> tuple[CustomApiProfileSelection, ...]:
        return tuple(_CUSTOM_API_SELECTIONS_ADAPTER.validate_python(value, strict=True))

    @field_validator("selected_custom_api_profiles")
    @classmethod
    def _validate_custom_api_profiles(
        cls, refs: tuple[CustomApiProfileSelection, ...]
    ) -> tuple[CustomApiProfileSelection, ...]:
        if len({ref.profile_id for ref in refs}) != len(refs):
            raise ValueError("duplicate selected_custom_api_profiles profile_id")
        return refs

    @field_validator("selected_mcp_connection_ids", mode="before")
    @classmethod
    def _parse_mcp_connection_ids(cls, value: object) -> tuple[str, ...]:
        return tuple(_MCP_CONNECTION_IDS_ADAPTER.validate_python(value, strict=True))

    @field_validator("selected_mcp_connection_ids")
    @classmethod
    def _validate_mcp_connection_ids(cls, refs: tuple[str, ...]) -> tuple[str, ...]:
        if len(refs) > 16 or len(set(refs)) != len(refs):
            raise ValueError("invalid selected_mcp_connection_ids")
        if any(_MCP_CONNECTION_ID.fullmatch(ref) is None for ref in refs):
            raise ValueError("invalid selected_mcp_connection_ids")
        if (
            len(json.dumps(refs, ensure_ascii=True, separators=(",", ":")).encode())
            > 4096
        ):
            raise ValueError("selected_mcp_connection_ids exceeds byte limit")
        return refs

    @field_validator("project_ref")
    @classmethod
    def _validate_project_ref(cls, value: str | None) -> str | None:
        if (
            value is not None
            and re.fullmatch(r"[^\s\x00-\x1f\x7f]{1,255}", value) is None
        ):
            raise ValueError("invalid project_ref")
        return value

    @field_validator("selected_skill_source_refs", mode="before")
    @classmethod
    def _parse_skill_source_refs(cls, value: object) -> tuple[str, ...]:
        refs = _SKILL_SOURCE_REFS_ADAPTER.validate_python(value, strict=True)
        return tuple(refs)

    @field_validator("selected_skill_source_refs")
    @classmethod
    def _validate_skill_source_refs(cls, refs: tuple[str, ...]) -> tuple[str, ...]:
        if len(refs) > _MAX_SKILL_SOURCE_REFS:
            raise ValueError("too many selected_skill_source_refs")
        if len(set(refs)) != len(refs) or any(
            _SKILL_SOURCE_REF.fullmatch(ref) is None or ref.startswith("skill:skill:")
            for ref in refs
        ):
            raise ValueError("invalid selected_skill_source_refs")
        encoded = json.dumps(refs, ensure_ascii=True, separators=(",", ":")).encode()
        if len(encoded) > _MAX_SKILL_SELECTION_BYTES:
            raise ValueError("selected_skill_source_refs exceeds byte limit")
        return refs


class ApproveDecision(StrictModel):
    type: Literal["approve"]
    item_id: NonEmptyStr
    args: dict[str, JsonValue] | None = None


class EditDecision(StrictModel):
    type: Literal["edit"]
    item_id: NonEmptyStr
    args: dict[str, JsonValue]


class RejectDecision(StrictModel):
    type: Literal["reject"]
    item_id: NonEmptyStr
    reason: str | None = None


class RespondDecision(StrictModel):
    type: Literal["respond"]
    item_id: NonEmptyStr
    response: NonEmptyStr


class SubmitDecision(StrictModel):
    type: Literal["submit"]
    item_id: NonEmptyStr
    value: dict[str, JsonValue]


ResumeDecision = Annotated[
    Union[
        ApproveDecision, EditDecision, RejectDecision, RespondDecision, SubmitDecision
    ],
    Field(discriminator="type"),
]


class RunResume(StrictModel):
    kind: Literal["run.resume"]
    run_id: NonEmptyStr
    session_id: NonEmptyStr
    command_id: NonEmptyStr
    request_digest: NonEmptyStr | None = None
    expected_pause_revision: Annotated[int, Field(ge=1)]
    pause_ref: NonEmptyStr
    decisions: Annotated[list[ResumeDecision], Field(min_length=1)]

    @field_validator("decisions")
    @classmethod
    def unique_items(cls, values: list[ResumeDecision]) -> list[ResumeDecision]:
        if len({value.item_id for value in values}) != len(values):
            raise ValueError("duplicate resume item")
        return values


class RunCancel(StrictModel):
    kind: Literal["run.cancel"]
    run_id: NonEmptyStr
    session_id: NonEmptyStr
    command_id: NonEmptyStr
    request_digest: NonEmptyStr | None = None


class RunSteer(StrictModel):
    kind: Literal["run.steer"]
    run_id: NonEmptyStr
    session_id: NonEmptyStr
    command_id: NonEmptyStr
    request_digest: NonEmptyStr | None = None
    message_id: NonEmptyStr
    content: NonEmptyStr


InboundMessage = Annotated[
    Union[RunRequest, RunResume, RunCancel, RunSteer],
    Field(discriminator="kind"),
]

inbound_adapter: TypeAdapter[InboundMessage] = TypeAdapter(InboundMessage)


__all__ = [
    "ApproveDecision",
    "CustomApiProfileSelection",
    "EditDecision",
    "ExecutionIdentity",
    "IdentityRef",
    "InboundMessage",
    "NonEmptyStr",
    "RejectDecision",
    "ResumeDecision",
    "RespondDecision",
    "RunCancel",
    "RunInput",
    "RunRequest",
    "RunResume",
    "RunSteer",
    "StrictModel",
    "SubmitDecision",
    "inbound_adapter",
]


def control_request_digest(message: RunResume) -> str:
    """HITL4 digest of the one typed normalized request, excluding delivery IDs.

    Keep the existing sha256 prefix and sorted compact {run_id, ...} format.
    Optional nullable decision values normalize identically; required pause/item
    identity never has a default. Cancel/steer retain their existing ingress path.
    """
    payload = message.model_dump(
        mode="json", exclude={"command_id", "request_digest"}, exclude_none=True
    )
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()
