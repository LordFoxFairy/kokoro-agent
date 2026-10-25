# fmt: off
"""Generated from contract/model-call-bound/v1/schema.json. Do not edit."""
from __future__ import annotations
from typing import Annotated, Literal
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, JsonValue
from kokoro_agent.protocol.model_usage_v2_generated import Opaque, Digest, Uuid, Ordinal, Count, UtcInstant
SOURCE_SHA256 = '9c27caf925c48037a7c185a0e62a08e00a2c202d3da05bc3d085f25d1bd08815'
def _output(value: str) -> str:
    if int(value) > 16384:
        raise ValueError("bound output cap exceeds profile")
    return value
OutputCap = Annotated[str, Field(pattern="^[1-9][0-9]*$", max_length=5), AfterValidator(_output)]
BoundRef = Annotated[str, Field(pattern='^mcb_[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')]
PlannedRef = Annotated[str, Field(pattern='^mpb_[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')]
def _system_provenance(value: dict[str, JsonValue]) -> dict[str, JsonValue]:
    if value != {'owner': 'kokoro-system', 'contract': 'contract/openapi/system.openapi.json', 'version': '4.0.0', 'source_commit': '7f479f654c535077874d9cf63d1b25437e9420b6', 'sha256': 'c7cda9d09db266cd26dc1b80dfe9bb95c3c9f6a91bdbd6c5d7de4f85295d1dc6', 'provenance': {'path': 'contract/provenance.json', 'sha256': '37e338939ebecc4a61cf7aff4d14c2e28b5bc34381e66324a2009988e793d2d0'}, 'source_sha256': {'scripts/generate-system-openapi.ts': '6cbbbfa18ac42511048dc8543605da6dd2087d40fc094212ee51d1cbf43d1dae', 'scripts/system-openapi-operations.ts': 'fb46bea2ec53a09c0f397d2f38aafc77a1c6e7eaef904b11667d56fda919d9f3', 'src/http/protocol.schema.ts': '72dbe6ee57d842b855b337d1f1603a7ddbb8c3a4bb6c4f7bf3fe62821c8160c0', 'src/modules/model-catalog/schemas/connection.schema.ts': '7d9efcaeb0d4d8725b50e2e2da95d37f9c64094ecbe99143aba3076df615ab03', 'src/modules/model-catalog/schemas/definition.schema.ts': 'e2063b9ae62b8e5c4ca7cccce09dd4874f818831fc8966de2cf6d015a99bef64', 'src/modules/model-catalog/schemas/label.schema.ts': '96116b4d390e203be056f73e32a30ea93e40dc0c60966ce60579cdb949f31a6a', 'src/modules/model-catalog/schemas/provider.schema.ts': 'ad07f69888f9cac1a263af81b70648381e19bbbd6f771c107eaa09b429099373', 'src/modules/model-catalog/schemas/resolve.schema.ts': '9d0786377b08c1f1e55c62f5821b85f4a6dc76a52a9ef70fea05e0243231e8b8', 'src/modules/model-catalog/schemas/revision.schema.ts': 'ad96204bc69d4310a0a44d67539921edc57076e9724a160170cb9abdf7ccae5a', 'src/modules/products/products.public.ts': 'ccea3298f5ca5549d7882ce056e8bbb3445d23a9e1703958bd4b0a220882983e', 'src/modules/products/schemas/application.schema.ts': '087d997360add9b9bbb2e244c1b1f9864fc6cf4ba93a7c9e8c14767288ca6a57', 'src/modules/products/schemas/config.schema.ts': '92324f19fd4e39fdf2cef0a4a28f9e43f8b70c09fd5290e8153035a5a2aa2060', 'src/modules/products/schemas/feature.schema.ts': '5222976ca8ccba5669e868386132dca6489d44c26f9a23526aaaeb0d20147014', 'src/modules/products/schemas/presentation.schema.ts': '5cd491fe9788de6fb8e7d575fd303b795251e7f682e89c10506e6ee7601174b5', 'src/modules/products/schemas/product.schema.ts': '90c57e7ecfa71346698ed7f399c6e31a5258451d4eea16023d2c011b4986beb6', 'src/modules/products/schemas/release.schema.ts': '2d21d41ebb4b0a0c8b33dde26783052f1142eaf8f44ab0027e350cb1b6e2a669', 'src/modules/runtime-manifests/schemas/manifest.schema.ts': '8b73a78e98c64174494398bc68c69061a408f53a9e81bf19d5e970d1ef828553', 'src/modules/sites/schemas/policy.schema.ts': 'b9aa0e7d70d9212a668b7c88133fb337acc8248f956240bbf0afe60d12eff7e9', 'src/modules/sites/schemas/site.schema.ts': '98031e74e4badb28d519f9f42e4c29894aa2a7c4b9388b0756764abc9e7b3636', 'src/modules/workspaces/schemas/workspace.schema.ts': 'a52e63e9fc798eb25b2003830ad4fcebb7ce9edf0a06b90a5f3b72ad752bc16d'}, 'operations': ['POST /v1/system/model-catalog/resolve']}:
        raise ValueError("bound provenance differs")
    return value
class ModelCallBound(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid", hide_input_in_errors=True)
    artifact_version: Literal['1.0.0']
    producer: Literal['kokoro-agent']
    profile_id: Literal['openai_chat_completions_capacity_v1']
    revision: Literal['1']
    sealed: Literal[True]
    execution_bound_ref: BoundRef
    planned_binding_ref: PlannedRef
    planned_binding_digest: Digest
    tenant_ref: Opaque
    run_id: Opaque
    call_id: Uuid
    attempt_id: Uuid
    attempt_ordinal: Ordinal
    lease_owner: Opaque
    lease_generation: Ordinal
    final_request_binding_version: Literal[1]
    final_request_digest: Digest
    request_profile: Literal['chat-completions-materialized-json-v1']
    capacity_guarantee_owner: Literal['kokoro-agent']
    capacity_guarantee_id: Literal['openai-gpt4o-20240806-text-standard-v1']
    capacity_guarantee_version: Literal['1.0.0']
    capacity_guarantee_digest: Literal['326f7d12d6cdfecf46f78f66ff7400378dc76ee4386146687a8574cf03af6995']
    unit: Literal['token']
    prompt_tokens_max: Literal['128000']
    output_tokens_max: OutputCap
    total_tokens_max: Count
    joint_constraints: tuple[Literal['0<=P<=128000'], Literal['0<=O<=M<=16384'], Literal['total=P+O'], Literal['K+U=P'], Literal['R+V=O']]
    system_provenance: Annotated[dict[str, JsonValue], AfterValidator(_system_provenance)]
    sealed_at: UtcInstant
    valid_until: UtcInstant
