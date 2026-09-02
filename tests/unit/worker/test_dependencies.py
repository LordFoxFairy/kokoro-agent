"""Worker owner-client composition remains optional and outside Run input."""

from kokoro_agent.contract import ExecutionIdentity, IdentityRef
from kokoro_agent.worker.dependencies import WorkerClients


async def test_default_worker_clients_leave_agent_core_available() -> None:
    clients = WorkerClients()
    identity = ExecutionIdentity(
        tenant_ref="tenant",
        actor=IdentityRef(kind="user", opaque_ref="actor"),
        subject=IdentityRef(kind="user", opaque_ref="subject"),
        identity_assertion_ref="assertion",
    )

    assert await clients.skill_client.resolve(("music",), identity, "namespace") == ()
    assert clients.mcp is None
    assert clients.delivery is None
