# fmt: off
"""Generated from the finite capacity guarantee schema; do not edit."""
from typing import Literal
from pydantic import BaseModel, ConfigDict

GUARANTEE_SHA256 = "326f7d12d6cdfecf46f78f66ff7400378dc76ee4386146687a8574cf03af6995"

class ModelCallCapacityEvidence(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid", hide_input_in_errors=True)
    artifact_version: Literal['1.0.0']
    owner: Literal['kokoro-agent']
    profile_id: Literal['openai_chat_completions_capacity_v1']
    checked_on: Literal['2026-10-23']
    trust_scope: Literal['Documented normal provider usage; not arbitrary provider violation or anomalous invoice guarantee']
    official_urls: tuple[Literal['https://developers.openai.com/api/docs/models/gpt-4o'], Literal['https://developers.openai.com/api/docs/guides/conversation-state'], Literal['https://developers.openai.com/api/docs/guides/token-counting'], Literal['https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create'], Literal['https://developers.openai.com/api/docs/guides/prompt-caching'], Literal['https://developers.openai.com/api/docs/deprecations']]
    facts: tuple[Literal['Fixed snapshot gpt-4o-2024-08-06 documents context window 128000 and maximum output 16384 tokens.'], Literal['Context contains input, output and any reasoning; using 128000 input plus the final output cap is a conservative joint bound.'], Literal['max_completion_tokens includes visible output and reasoning tokens, not only displayed text.'], Literal['Cached prompt tokens are a subset of prompt tokens; reasoning tokens are a subset of completion tokens.'], Literal['Only direct text Chat Completions, n=1, nonstream, default tier, store=false and no tools, prediction, multimodal or extra charge categories are admitted.'], Literal['The fixed snapshot is scheduled for removal on 2027-01-14; technical validity ends no later than 2027-01-14T00:00:00Z.'], Literal['These are reviewed documented normal-provider facts, not account availability, provider violation protection or an absolute anomalous-invoice guarantee.']]

class ModelCallCapacityGuarantee(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid", hide_input_in_errors=True)
    artifact_version: Literal['1.0.0']
    owner: Literal['kokoro-agent']
    guarantee_id: Literal['openai-gpt4o-20240806-text-standard-v1']
    profile_id: Literal['openai_chat_completions_capacity_v1']
    trust_class: Literal['provider-documented/Agent-enforced']
    protocol: Literal['openai_chat_completions']
    connection_type: Literal['direct']
    endpoint: Literal['https://api.openai.com/v1/chat/completions']
    request_model: Literal['gpt-4o-2024-08-06']
    context_window: Literal[128000]
    max_output_tokens: Literal[16384]
    unit: Literal['token']
    n: Literal[1]
    stream: Literal[False]
    service_tier: Literal['default']
    store: Literal[False]
    roles: tuple[Literal['system'], Literal['user'], Literal['assistant']]
    request_fields: tuple[Literal['model'], Literal['messages'], Literal['n'], Literal['max_completion_tokens'], Literal['stream'], Literal['service_tier'], Literal['store']]
    joint_constraints: tuple[Literal['0<=P<=128000'], Literal['0<=O<=M<=16384'], Literal['total=P+O'], Literal['K+U=P'], Literal['R+V=O']]
    unsupported_policy: Literal['fail_closed_before_prepare']
    uncertain_result_policy: Literal['unknown_no_retry_no_automatic_release']
    sunset: Literal['2027-01-14T00:00:00Z']
    evidence_checked_on: Literal['2026-10-23']
    enforcement_revision: Literal['agent-final-wire-text-v1']
    usage_semantics: Literal['cached_input_subset_of_prompt;reasoning_subset_of_completion;no_extra_cache_write_fee_for_fixed_4o;unknown_categories_unsupported']
    normalization_owner: Literal['kokoro-agent']
    normalization_version: Literal['1.0.0']
    normalization_manifest_aggregate_sha256: Literal['ecd30d1908f410eed29181c9f52669f824312bf3a2e3874153d990b95fe58b91']
    evidence_version: Literal['1.0.0']
    evidence_sha256: Literal['e8ddcfc7e711d95c5324d319bfe254f7e15008708546616199748c7739d0201f']
