from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from kokoro_agent.protocol import ExecutionIdentity, IdentityRef


def identity() -> ExecutionIdentity:
    return ExecutionIdentity(
        tenant_ref="tenant",
        subject=IdentityRef(kind="user", opaque_ref="user"),
        actor=IdentityRef(kind="user", opaque_ref="user"),
        identity_assertion_ref="assertion",
    )


@pytest.mark.asyncio
async def test_display_read_rejects_foreign_run_and_invalid_transport_before_checkpoint_io() -> (
    None
):
    from kokoro_agent.interfaces.http.run_display import display_resource

    repository: Any = SimpleNamespace(get_request_scoped=AsyncMock(return_value=None))
    reader = AsyncMock()
    from kokoro_agent.interfaces.http.ingress import IngressError

    with pytest.raises(IngressError) as caught:
        await display_resource("run", identity(), {}, {}, None, repository, reader)
    assert caught.value.status == 404
    reader.assert_not_called()
    assert repository.get_request_scoped.await_args.args[:2] == ("run", "tenant")
    for headers, query in [
        ({"content-length": "0"}, {}),
        ({"idempotency-key": "key"}, {}),
        ({}, {"x": ["1"]}),
        ({"transfer-encoding": "chunked"}, {}),
    ]:
        with pytest.raises(IngressError) as caught:
            await display_resource(
                "run", identity(), headers, query, None, repository, reader
            )
        assert caught.value.status == 400
    assert repository.get_request_scoped.await_count == 1


@pytest.mark.asyncio
async def test_real_display_dispatch_checks_framing_before_any_dependency_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.interfaces.http import server
    from pydantic import SecretStr

    config: Any = SimpleNamespace(
        internal_secret_agent=SecretStr("fixture"), run_repository=None
    )
    calls: list[str] = []

    def dependency(*_args: object, **_kwargs: object) -> Any:
        calls.append("dependency")
        raise AssertionError("invalid display request must not open dependencies")

    monkeypatch.setattr(server, "make_run_repository", dependency)
    headers = {
        "authorization": "Bearer fixture",
        "x-kokoro-tenant-ref": "tenant",
        "x-kokoro-subject-ref": "user",
        "x-kokoro-subject-kind": "user",
        "x-kokoro-actor-ref": "user",
        "x-kokoro-actor-kind": "user",
        "x-kokoro-identity-assertion-ref": "assertion",
        "content-length": "0",
    }
    status, payload = await server.dispatch_request(
        config, "GET", "/v1/runs/run/display", {}, headers, None
    )
    assert status == 400
    error: Any = payload["error"]
    assert error["code"] == "invalid_run_display_request"
    assert calls == []
