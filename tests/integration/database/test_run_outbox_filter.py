"""The PostgreSQL outbox filter preserves queued-only replay behavior."""

from __future__ import annotations

from kokoro_agent.domain.run.repository import RunRepository
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest


def _request(run_id: str) -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id=run_id,
        session_id="session-outbox-filter",
        feature_key="chat",
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant-outbox-filter",
            actor=IdentityRef(kind="user", opaque_ref="actor-outbox-filter"),
            subject=IdentityRef(kind="user", opaque_ref="subject-outbox-filter"),
            identity_assertion_ref="assertion-outbox-filter",
        ),
        input=RunInput(message_id=f"message-{run_id}", content="outbox filter"),
    )


async def test_list_unpublished_outbox_returns_only_queued_rows(
    run_repository: RunRepository,
) -> None:
    request = _request("outbox-filter")
    lease = await run_repository.try_claim(request, "outbox-filter-worker")
    assert lease is not None

    first = await run_repository.stage_critical_frame(
        request.run_id,
        lease,
        "message.delta",
        1_700_000_000_000,
        '{"delta":"first"}',
        terminal=False,
    )
    second = await run_repository.stage_critical_frame(
        request.run_id,
        lease,
        "message.delta",
        1_700_000_000_001,
        '{"delta":"second"}',
        terminal=False,
    )
    assert first is not None
    assert second is not None

    await run_repository.mark_critical_published(request.run_id, first.durable_seq)

    unpublished = await run_repository.list_unpublished_outbox()
    assert [
        (frame.run_id, frame.durable_seq, frame.payload_json) for frame in unpublished
    ] == [(request.run_id, second.durable_seq, '{"delta":"second"}')]
