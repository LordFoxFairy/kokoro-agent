"""Runtime namespace follows stable tenancy/subject ownership."""

from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest
from kokoro_agent.domain.run.scope import RunScope, runtime_namespace


def _identity(
    *,
    tenant: str = "tenant-a",
    actor: str = "actor-a",
    subject: str = "subject-a",
    assertion: str = "assertion-a",
) -> ExecutionIdentity:
    return ExecutionIdentity(
        tenant_ref=tenant,
        actor=IdentityRef(kind="user", opaque_ref=actor),
        subject=IdentityRef(kind="project", opaque_ref=subject),
        identity_assertion_ref=assertion,
    )


def test_actor_or_assertion_rotation_does_not_fork_namespace() -> None:
    baseline = runtime_namespace(_identity())

    assert runtime_namespace(_identity(actor="service-b")) == baseline
    assert runtime_namespace(_identity(assertion="assertion-b")) == baseline


def test_tenant_or_subject_change_produces_another_namespace() -> None:
    baseline = runtime_namespace(_identity())

    assert runtime_namespace(_identity(tenant="tenant-b")) != baseline
    assert runtime_namespace(_identity(subject="subject-b")) != baseline


def test_checkpoint_locator_scopes_same_session_by_trusted_tenant() -> None:
    def request(tenant: str) -> RunRequest:
        return RunRequest(
            kind="run.request",
            run_id=f"run-{tenant}",
            session_id="shared-session-id",
            feature_key="chat",
            execution_identity=_identity(tenant=tenant),
            input=RunInput(message_id=f"message-{tenant}", content="hello"),
        )

    assert RunScope.of(request("tenant-a")).scoped_thread_id != RunScope.of(
        request("tenant-b")
    ).scoped_thread_id
