"""Runtime namespace follows stable tenancy/subject ownership."""

from kokoro_agent.contract import ExecutionIdentity, IdentityRef
from kokoro_agent.execution.scope import runtime_namespace


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
