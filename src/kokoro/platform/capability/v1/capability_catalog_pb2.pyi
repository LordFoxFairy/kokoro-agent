import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from kokoro.common.v1 import receipt_pb2 as _receipt_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SignatureAlgorithm(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SIGNATURE_ALGORITHM_UNSPECIFIED: _ClassVar[SignatureAlgorithm]
    SIGNATURE_ALGORITHM_ED25519_SHA256_V1: _ClassVar[SignatureAlgorithm]

class CatalogProjectionState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_PROJECTION_STATE_UNSPECIFIED: _ClassVar[CatalogProjectionState]
    CATALOG_PROJECTION_STATE_PENDING: _ClassVar[CatalogProjectionState]
    CATALOG_PROJECTION_STATE_COMMITTED: _ClassVar[CatalogProjectionState]
    CATALOG_PROJECTION_STATE_REJECTED: _ClassVar[CatalogProjectionState]
    CATALOG_PROJECTION_STATE_OUTCOME_UNKNOWN: _ClassVar[CatalogProjectionState]
SIGNATURE_ALGORITHM_UNSPECIFIED: SignatureAlgorithm
SIGNATURE_ALGORITHM_ED25519_SHA256_V1: SignatureAlgorithm
CATALOG_PROJECTION_STATE_UNSPECIFIED: CatalogProjectionState
CATALOG_PROJECTION_STATE_PENDING: CatalogProjectionState
CATALOG_PROJECTION_STATE_COMMITTED: CatalogProjectionState
CATALOG_PROJECTION_STATE_REJECTED: CatalogProjectionState
CATALOG_PROJECTION_STATE_OUTCOME_UNKNOWN: CatalogProjectionState

class AgentOption(_message.Message):
    __slots__ = ("option_ref", "agent", "label")
    OPTION_REF_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    option_ref: str
    agent: str
    label: str
    def __init__(self, option_ref: _Optional[str] = ..., agent: _Optional[str] = ..., label: _Optional[str] = ...) -> None: ...

class SkillOption(_message.Message):
    __slots__ = ("option_ref", "label", "name", "content_hash", "description", "scope", "prerequisite_ref")
    OPTION_REF_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    PREREQUISITE_REF_FIELD_NUMBER: _ClassVar[int]
    option_ref: str
    label: str
    name: str
    content_hash: str
    description: str
    scope: str
    prerequisite_ref: str
    def __init__(self, option_ref: _Optional[str] = ..., label: _Optional[str] = ..., name: _Optional[str] = ..., content_hash: _Optional[str] = ..., description: _Optional[str] = ..., scope: _Optional[str] = ..., prerequisite_ref: _Optional[str] = ...) -> None: ...

class McpOption(_message.Message):
    __slots__ = ("option_ref", "label", "scope", "name", "revision", "config_hash", "prerequisite_ref")
    OPTION_REF_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    CONFIG_HASH_FIELD_NUMBER: _ClassVar[int]
    PREREQUISITE_REF_FIELD_NUMBER: _ClassVar[int]
    option_ref: str
    label: str
    scope: str
    name: str
    revision: int
    config_hash: str
    prerequisite_ref: str
    def __init__(self, option_ref: _Optional[str] = ..., label: _Optional[str] = ..., scope: _Optional[str] = ..., name: _Optional[str] = ..., revision: _Optional[int] = ..., config_hash: _Optional[str] = ..., prerequisite_ref: _Optional[str] = ...) -> None: ...

class CapabilityCatalogSnapshot(_message.Message):
    __slots__ = ("schema_version", "agent_options", "default_agent_option_ref", "tools", "skill_options", "mcp_options", "subagents")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    AGENT_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_AGENT_OPTION_REF_FIELD_NUMBER: _ClassVar[int]
    TOOLS_FIELD_NUMBER: _ClassVar[int]
    SKILL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    MCP_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SUBAGENTS_FIELD_NUMBER: _ClassVar[int]
    schema_version: int
    agent_options: _containers.RepeatedCompositeFieldContainer[AgentOption]
    default_agent_option_ref: str
    tools: _containers.RepeatedScalarFieldContainer[str]
    skill_options: _containers.RepeatedCompositeFieldContainer[SkillOption]
    mcp_options: _containers.RepeatedCompositeFieldContainer[McpOption]
    subagents: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, schema_version: _Optional[int] = ..., agent_options: _Optional[_Iterable[_Union[AgentOption, _Mapping]]] = ..., default_agent_option_ref: _Optional[str] = ..., tools: _Optional[_Iterable[str]] = ..., skill_options: _Optional[_Iterable[_Union[SkillOption, _Mapping]]] = ..., mcp_options: _Optional[_Iterable[_Union[McpOption, _Mapping]]] = ..., subagents: _Optional[_Iterable[str]] = ...) -> None: ...

class FrozenCatalogPublication(_message.Message):
    __slots__ = ("site_id", "site_release_ref", "agent_catalog_ref", "snapshot_digest", "snapshot", "frozen_at", "signing_key_ref", "signature_algorithm", "signature_payload_digest", "signature")
    SITE_ID_FIELD_NUMBER: _ClassVar[int]
    SITE_RELEASE_REF_FIELD_NUMBER: _ClassVar[int]
    AGENT_CATALOG_REF_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    FROZEN_AT_FIELD_NUMBER: _ClassVar[int]
    SIGNING_KEY_REF_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_PAYLOAD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    site_id: str
    site_release_ref: str
    agent_catalog_ref: str
    snapshot_digest: str
    snapshot: CapabilityCatalogSnapshot
    frozen_at: _timestamp_pb2.Timestamp
    signing_key_ref: str
    signature_algorithm: SignatureAlgorithm
    signature_payload_digest: str
    signature: bytes
    def __init__(self, site_id: _Optional[str] = ..., site_release_ref: _Optional[str] = ..., agent_catalog_ref: _Optional[str] = ..., snapshot_digest: _Optional[str] = ..., snapshot: _Optional[_Union[CapabilityCatalogSnapshot, _Mapping]] = ..., frozen_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., signing_key_ref: _Optional[str] = ..., signature_algorithm: _Optional[_Union[SignatureAlgorithm, str]] = ..., signature_payload_digest: _Optional[str] = ..., signature: _Optional[bytes] = ...) -> None: ...

class FreezeCatalogEffect(_message.Message):
    __slots__ = ("site_id", "site_release_ref", "snapshot")
    SITE_ID_FIELD_NUMBER: _ClassVar[int]
    SITE_RELEASE_REF_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    site_id: str
    site_release_ref: str
    snapshot: CapabilityCatalogSnapshot
    def __init__(self, site_id: _Optional[str] = ..., site_release_ref: _Optional[str] = ..., snapshot: _Optional[_Union[CapabilityCatalogSnapshot, _Mapping]] = ...) -> None: ...

class FreezeCatalogRequest(_message.Message):
    __slots__ = ("command", "effect")
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    command: _receipt_pb2.CommandIdentity
    effect: FreezeCatalogEffect
    def __init__(self, command: _Optional[_Union[_receipt_pb2.CommandIdentity, _Mapping]] = ..., effect: _Optional[_Union[FreezeCatalogEffect, _Mapping]] = ...) -> None: ...

class FreezeCatalogResponse(_message.Message):
    __slots__ = ("receipt", "publication", "projection_state", "replayed")
    RECEIPT_FIELD_NUMBER: _ClassVar[int]
    PUBLICATION_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_STATE_FIELD_NUMBER: _ClassVar[int]
    REPLAYED_FIELD_NUMBER: _ClassVar[int]
    receipt: _receipt_pb2.CommandReceipt
    publication: FrozenCatalogPublication
    projection_state: CatalogProjectionState
    replayed: bool
    def __init__(self, receipt: _Optional[_Union[_receipt_pb2.CommandReceipt, _Mapping]] = ..., publication: _Optional[_Union[FrozenCatalogPublication, _Mapping]] = ..., projection_state: _Optional[_Union[CatalogProjectionState, str]] = ..., replayed: _Optional[bool] = ...) -> None: ...

class GetCatalogPublicationRequest(_message.Message):
    __slots__ = ("command_id", "idempotency_key", "digest_algorithm", "request_digest", "site_id", "site_release_ref")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    DIGEST_ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    REQUEST_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SITE_ID_FIELD_NUMBER: _ClassVar[int]
    SITE_RELEASE_REF_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    idempotency_key: str
    digest_algorithm: _receipt_pb2.CommandDigestAlgorithm
    request_digest: str
    site_id: str
    site_release_ref: str
    def __init__(self, command_id: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., digest_algorithm: _Optional[_Union[_receipt_pb2.CommandDigestAlgorithm, str]] = ..., request_digest: _Optional[str] = ..., site_id: _Optional[str] = ..., site_release_ref: _Optional[str] = ...) -> None: ...

class GetCatalogPublicationResponse(_message.Message):
    __slots__ = ("receipt", "publication", "projection_state", "last_projection_error_code")
    RECEIPT_FIELD_NUMBER: _ClassVar[int]
    PUBLICATION_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_STATE_FIELD_NUMBER: _ClassVar[int]
    LAST_PROJECTION_ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    receipt: _receipt_pb2.CommandReceipt
    publication: FrozenCatalogPublication
    projection_state: CatalogProjectionState
    last_projection_error_code: str
    def __init__(self, receipt: _Optional[_Union[_receipt_pb2.CommandReceipt, _Mapping]] = ..., publication: _Optional[_Union[FrozenCatalogPublication, _Mapping]] = ..., projection_state: _Optional[_Union[CatalogProjectionState, str]] = ..., last_projection_error_code: _Optional[str] = ...) -> None: ...

class SkillGrantSelection(_message.Message):
    __slots__ = ("option_ref", "scope", "name", "content_hash", "description")
    OPTION_REF_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    option_ref: str
    scope: str
    name: str
    content_hash: str
    description: str
    def __init__(self, option_ref: _Optional[str] = ..., scope: _Optional[str] = ..., name: _Optional[str] = ..., content_hash: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class McpGrantSelection(_message.Message):
    __slots__ = ("option_ref", "scope", "name", "revision", "config_hash")
    OPTION_REF_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    CONFIG_HASH_FIELD_NUMBER: _ClassVar[int]
    option_ref: str
    scope: str
    name: str
    revision: int
    config_hash: str
    def __init__(self, option_ref: _Optional[str] = ..., scope: _Optional[str] = ..., name: _Optional[str] = ..., revision: _Optional[int] = ..., config_hash: _Optional[str] = ...) -> None: ...

class ResolveExecutionAssemblyRequest(_message.Message):
    __slots__ = ("namespace", "agent_catalog_ref", "skill_grants", "mcp_grants")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    AGENT_CATALOG_REF_FIELD_NUMBER: _ClassVar[int]
    SKILL_GRANTS_FIELD_NUMBER: _ClassVar[int]
    MCP_GRANTS_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    agent_catalog_ref: str
    skill_grants: _containers.RepeatedCompositeFieldContainer[SkillGrantSelection]
    mcp_grants: _containers.RepeatedCompositeFieldContainer[McpGrantSelection]
    def __init__(self, namespace: _Optional[str] = ..., agent_catalog_ref: _Optional[str] = ..., skill_grants: _Optional[_Iterable[_Union[SkillGrantSelection, _Mapping]]] = ..., mcp_grants: _Optional[_Iterable[_Union[McpGrantSelection, _Mapping]]] = ...) -> None: ...

class SkillArtifactManifest(_message.Message):
    __slots__ = ("option_ref", "scope", "name", "content_hash", "description", "artifact_ref", "artifact_size", "artifact_sha256")
    OPTION_REF_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_REF_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_SIZE_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_SHA256_FIELD_NUMBER: _ClassVar[int]
    option_ref: str
    scope: str
    name: str
    content_hash: str
    description: str
    artifact_ref: str
    artifact_size: int
    artifact_sha256: str
    def __init__(self, option_ref: _Optional[str] = ..., scope: _Optional[str] = ..., name: _Optional[str] = ..., content_hash: _Optional[str] = ..., description: _Optional[str] = ..., artifact_ref: _Optional[str] = ..., artifact_size: _Optional[int] = ..., artifact_sha256: _Optional[str] = ...) -> None: ...

class McpAssemblyConfig(_message.Message):
    __slots__ = ("option_ref", "scope", "name", "revision", "config_hash", "transport", "url", "allowed_tools", "authorization_value")
    OPTION_REF_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    CONFIG_HASH_FIELD_NUMBER: _ClassVar[int]
    TRANSPORT_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_TOOLS_FIELD_NUMBER: _ClassVar[int]
    AUTHORIZATION_VALUE_FIELD_NUMBER: _ClassVar[int]
    option_ref: str
    scope: str
    name: str
    revision: int
    config_hash: str
    transport: str
    url: str
    allowed_tools: _containers.RepeatedScalarFieldContainer[str]
    authorization_value: str
    def __init__(self, option_ref: _Optional[str] = ..., scope: _Optional[str] = ..., name: _Optional[str] = ..., revision: _Optional[int] = ..., config_hash: _Optional[str] = ..., transport: _Optional[str] = ..., url: _Optional[str] = ..., allowed_tools: _Optional[_Iterable[str]] = ..., authorization_value: _Optional[str] = ...) -> None: ...

class ResolveExecutionAssemblyResponse(_message.Message):
    __slots__ = ("agent_catalog_ref", "assembly_digest", "skills", "mcp_servers")
    AGENT_CATALOG_REF_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLY_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SKILLS_FIELD_NUMBER: _ClassVar[int]
    MCP_SERVERS_FIELD_NUMBER: _ClassVar[int]
    agent_catalog_ref: str
    assembly_digest: str
    skills: _containers.RepeatedCompositeFieldContainer[SkillArtifactManifest]
    mcp_servers: _containers.RepeatedCompositeFieldContainer[McpAssemblyConfig]
    def __init__(self, agent_catalog_ref: _Optional[str] = ..., assembly_digest: _Optional[str] = ..., skills: _Optional[_Iterable[_Union[SkillArtifactManifest, _Mapping]]] = ..., mcp_servers: _Optional[_Iterable[_Union[McpAssemblyConfig, _Mapping]]] = ...) -> None: ...

class FetchSkillArtifactRequest(_message.Message):
    __slots__ = ("namespace", "agent_catalog_ref", "grant", "artifact_ref", "expected_size", "expected_sha256")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    AGENT_CATALOG_REF_FIELD_NUMBER: _ClassVar[int]
    GRANT_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_REF_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_SIZE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_SHA256_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    agent_catalog_ref: str
    grant: SkillGrantSelection
    artifact_ref: str
    expected_size: int
    expected_sha256: str
    def __init__(self, namespace: _Optional[str] = ..., agent_catalog_ref: _Optional[str] = ..., grant: _Optional[_Union[SkillGrantSelection, _Mapping]] = ..., artifact_ref: _Optional[str] = ..., expected_size: _Optional[int] = ..., expected_sha256: _Optional[str] = ...) -> None: ...

class FetchSkillArtifactResponse(_message.Message):
    __slots__ = ("artifact_ref", "offset", "data")
    ARTIFACT_REF_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    artifact_ref: str
    offset: int
    data: bytes
    def __init__(self, artifact_ref: _Optional[str] = ..., offset: _Optional[int] = ..., data: _Optional[bytes] = ...) -> None: ...

class ProjectCatalogRequest(_message.Message):
    __slots__ = ("command", "publication")
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    PUBLICATION_FIELD_NUMBER: _ClassVar[int]
    command: _receipt_pb2.CommandIdentity
    publication: FrozenCatalogPublication
    def __init__(self, command: _Optional[_Union[_receipt_pb2.CommandIdentity, _Mapping]] = ..., publication: _Optional[_Union[FrozenCatalogPublication, _Mapping]] = ...) -> None: ...

class ProjectCatalogResponse(_message.Message):
    __slots__ = ("receipt", "agent_catalog_ref", "projection_state", "replayed")
    RECEIPT_FIELD_NUMBER: _ClassVar[int]
    AGENT_CATALOG_REF_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_STATE_FIELD_NUMBER: _ClassVar[int]
    REPLAYED_FIELD_NUMBER: _ClassVar[int]
    receipt: _receipt_pb2.CommandReceipt
    agent_catalog_ref: str
    projection_state: CatalogProjectionState
    replayed: bool
    def __init__(self, receipt: _Optional[_Union[_receipt_pb2.CommandReceipt, _Mapping]] = ..., agent_catalog_ref: _Optional[str] = ..., projection_state: _Optional[_Union[CatalogProjectionState, str]] = ..., replayed: _Optional[bool] = ...) -> None: ...

class GetProjectionReceiptRequest(_message.Message):
    __slots__ = ("command_id", "idempotency_key", "digest_algorithm", "request_digest", "site_id")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    DIGEST_ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    REQUEST_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SITE_ID_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    idempotency_key: str
    digest_algorithm: _receipt_pb2.CommandDigestAlgorithm
    request_digest: str
    site_id: str
    def __init__(self, command_id: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., digest_algorithm: _Optional[_Union[_receipt_pb2.CommandDigestAlgorithm, str]] = ..., request_digest: _Optional[str] = ..., site_id: _Optional[str] = ...) -> None: ...

class GetProjectionReceiptResponse(_message.Message):
    __slots__ = ("receipt", "agent_catalog_ref", "projection_state")
    RECEIPT_FIELD_NUMBER: _ClassVar[int]
    AGENT_CATALOG_REF_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_STATE_FIELD_NUMBER: _ClassVar[int]
    receipt: _receipt_pb2.CommandReceipt
    agent_catalog_ref: str
    projection_state: CatalogProjectionState
    def __init__(self, receipt: _Optional[_Union[_receipt_pb2.CommandReceipt, _Mapping]] = ..., agent_catalog_ref: _Optional[str] = ..., projection_state: _Optional[_Union[CatalogProjectionState, str]] = ...) -> None: ...
