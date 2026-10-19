"""SDK/loopback mechanisms only; never a production authorization fixture."""

from __future__ import annotations

import json

import httpx

from kokoro_agent.model.call_artifacts import (
    encode_final_request_digest,
    encode_planned_artifact,
)
from kokoro_agent.model.factory import ModelHttpClients
from kokoro_agent.model.usage_observation import claim_usage_request
from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding


class ModelHttpMechanismClients(ModelHttpClients):
    """Real owned HTTPX pools sharing production profile/cap/usage algorithms.

    Isolated SDK protocol tests exercise these clients, not the production gate.
    Native/mandatory-gate tests must continue constructing ModelHttpClients.
    """

    def _validate_sync(self, request: httpx.Request) -> None:
        self._validate(request, request.content)
        claim_usage_request(request, self.route)

    async def _validate_async(self, request: httpx.Request) -> None:
        self._validate(request, request.content)
        claim_usage_request(request, self.route)


def synthetic_final_digest(planned: PlannedBinding, content: str) -> str:
    """A named synthetic final-body capsule for persistence-only fixtures."""
    raw = json.dumps(
        {
            "model": planned.request_model,
            "n": 1,
            "max_completion_tokens": 1,
            "messages": [{"role": "user", "content": content}],
        },
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return encode_final_request_digest(
        endpoint="http://synthetic.invalid/v1/chat/completions",
        planned_digest=encode_planned_artifact(planned)[1],
        raw=raw,
    )
