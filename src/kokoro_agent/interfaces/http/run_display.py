"""The named, identity-scoped read-only Run display resource."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping

from kokoro_agent.application.run_display import RunDisplay
from typing import Protocol

from kokoro_agent.domain.run.scope import runtime_namespace
from kokoro_agent.interfaces.http.ingress import IngressError
from kokoro_agent.protocol import ExecutionIdentity, RunRequest


class _ScopedRequests(Protocol):
    async def get_request_scoped(
        self, run_id: str, tenant_ref: str, namespace: str
    ) -> RunRequest | None: ...


async def display_resource(
    run_id: str,
    identity: ExecutionIdentity,
    headers: Mapping[str, str],
    query: Mapping[str, list[str]],
    body: object,
    repository: _ScopedRequests,
    read: Callable[[RunRequest], Awaitable[RunDisplay]],
) -> dict[str, object]:
    validate_display_transport(headers, query, body)
    deadline = asyncio.get_running_loop().time() + 5
    try:
        async with asyncio.timeout_at(deadline):
            request = await repository.get_request_scoped(
                run_id, identity.tenant_ref, runtime_namespace(identity)
            )
            if request is None:
                raise IngressError(404, "run_not_found", "Run was not found")
            result = await read(request)
            payload = result.model_dump(mode="json")
            if len(json.dumps(payload, ensure_ascii=False).encode()) > 1024 * 1024:
                raise ValueError("display response exceeds its bound")
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError
            return payload
    except IngressError:
        raise
    except (TimeoutError, ValueError) as error:
        raise IngressError(
            503, "agent_unavailable", "Run display facts are unavailable"
        ) from error


def validate_display_transport(
    headers: Mapping[str, str], query: Mapping[str, list[str]], body: object
) -> None:
    if (
        query
        or body is not None
        or any(
            name in headers
            for name in ("content-length", "transfer-encoding", "idempotency-key")
        )
    ):
        raise IngressError(
            400, "invalid_run_display_request", "Run display read has no query or body"
        )
