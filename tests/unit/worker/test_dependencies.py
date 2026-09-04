"""Worker owner-client composition remains optional and outside Run input."""

from kokoro_agent.worker.dependencies import WorkerClients


async def test_default_worker_clients_leave_agent_core_available() -> None:
    clients = WorkerClients()
    assert clients.skill_client is None
    assert clients.skill_reader is None
    assert clients.mcp is None
    assert clients.delivery is None
