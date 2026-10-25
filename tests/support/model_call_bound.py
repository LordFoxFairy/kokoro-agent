"""Persistence fixtures using the actual supported profile, never a grant."""

from __future__ import annotations
import json
from uuid import NAMESPACE_URL, uuid4, uuid5
from kokoro_agent.clients.system import ModelConnection, ResolvedModel
from kokoro_agent.model.call_models import CapacityRequestMaterial
from kokoro_agent.model.call_artifacts import (
    encode_final_request_digest,
    encode_planned_artifact,
)
from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding


def supported_attempt_id() -> str:
    return str(uuid5(NAMESPACE_URL, str(uuid4())))


def supported_material(
    planned: PlannedBinding, content: str
) -> CapacityRequestMaterial:
    assert planned.label is not None
    route = ResolvedModel(
        model_id=planned.model,
        provider_id=planned.provider,
        revision_id=planned.revision_id,
        revision=int(planned.revision),
        digest=planned.digest,
        generation=planned.generation,
        tenant_generation=planned.tenant_generation,
        provider_model_name=planned.provider_model_name,
        request_model=planned.request_model,
        feature_key=planned.feature_key,
        label_key=planned.label,
        context_window=128000,
        max_output_tokens=128,
        connection=ModelConnection(
            protocol="openai_chat_completions",
            connection_type="direct",
            base_url="https://api.openai.com/v1",
            secret_handle_ref="test-only",
        ),
    )
    raw = json.dumps(
        {
            "model": planned.request_model,
            "n": 1,
            "max_completion_tokens": 128,
            "messages": [{"role": "user", "content": content}],
            "stream": False,
            "store": False,
            "service_tier": "default",
        },
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode()
    return CapacityRequestMaterial(
        route=route,
        raw=raw,
        method="POST",
        endpoint="https://api.openai.com/v1/chat/completions",
        headers=(
            ("content-type", "application/json"),
            ("content-length", str(len(raw))),
            ("host", "api.openai.com"),
            ("accept", "application/json"),
        ),
    )


def supported_final_digest(planned: PlannedBinding, content: str) -> str:
    material = supported_material(planned, content)
    return encode_final_request_digest(
        endpoint=material.endpoint,
        planned_digest=encode_planned_artifact(planned)[1],
        raw=material.raw,
    )
