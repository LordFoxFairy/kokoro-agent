"""Native signed consumption proofs do not depend on browser or Platform credentials."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import jwt
import pytest

from kokoro_agent.domain.run.models import LeaseFence, LeasedRun
from kokoro_agent.execution.execution_proof_signer import (
    ExecutionProofSigner,
    ExecutionProofSignerConfig,
)
from kokoro_agent.execution.execution_proof_supplier import (
    CurrentLeaseObservation,
    ExecutionProofUnavailableError,
    create_execution_proof_supplier,
)
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
BINDING = "ab" * 32


class _LeaseReader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, LeaseFence]] = []

    async def observe_current_lease(
        self, *, run_id: str, fence: LeaseFence
    ) -> CurrentLeaseObservation | None:
        self.calls.append((run_id, fence))
        return (
            CurrentLeaseObservation(
                database_now=EPOCH + timedelta(seconds=100),
                lease_expires_at=EPOCH + timedelta(seconds=161),
            )
            if len(self.calls) <= 2
            else None
        )


@pytest.mark.asyncio
async def test_budget_proof_uses_real_signer_without_browser_or_platform_tokens() -> (
    None
):
    """A Run proof binds consumption; workload auth and IAM permission remain separate."""
    user = IdentityRef(kind="user", opaque_ref="actor-run-1")
    pair = LeasedRun(
        request=RunRequest(
            kind="run.request",
            run_id="run-1",
            session_id="session-1",
            feature_key="chat",
            project_ref="project-1",
            selected_skill_source_refs=("skill:budget-sample",),
            execution_identity=ExecutionIdentity(
                tenant_ref="tenant-1",
                actor=user,
                subject=user,
                identity_assertion_ref="ASSERTION_SENTINEL",
            ),
            input=RunInput(message_id="message-1", content="budget proof acceptance"),
        ),
        lease=LeaseFence(owner="owner-1", generation=7),
    )
    private_key = Ed25519PrivateKey.generate()
    issuer = "https://agent.example.test"
    signer = ExecutionProofSigner(
        ExecutionProofSignerConfig(
            issuer=issuer, kid="budget-key", private_key=private_key
        )
    )
    reader = _LeaseReader()
    nonces = iter([b"\x01" * 16, b"\x02" * 16])
    supplier = create_execution_proof_supplier(
        leased_run=pair,
        lease_reader=reader,
        signer=signer,
        clock=lambda: EPOCH + timedelta(seconds=100),
        nonce_provider=lambda size: next(nonces),
    )
    claims: list[dict[str, Any]] = []
    for digest in (BINDING, "cd" * 32):
        compact = await supplier.issue("billing.credit.consume", digest)
        claims.append(
            jwt.decode(
                compact,
                key=private_key.public_key(),
                algorithms=["EdDSA"],
                issuer=issuer,
                audience="https://kokoro.dev/resources/iam-execution-authorization",
                options={"verify_exp": False},
            )
        )
        assert jwt.get_unverified_header(compact) == {
            "alg": "EdDSA",
            "kid": "budget-key",
            "typ": "kokoro-agent-execution+jwt",
        }
    for claim, digest in zip(claims, (BINDING, "cd" * 32), strict=True):
        assert claim["operation"] == "billing.credit.consume"
        assert claim["tenant_ref"] == "tenant-1"
        assert (
            claim["actor"]
            == claim["subject"]
            == {"kind": "user", "opaque_ref": "actor-run-1"}
        )
        assert claim["run_id"] == "run-1"
        assert claim["execution_session_id"] == "session-1"
        assert claim["lease_generation"] == 7
        assert claim["request_binding_sha256"] == digest
        assert claim["execution_binding"] == {
            "identity_assertion_ref": "ASSERTION_SENTINEL",
            "project_ref": "project-1",
            "selection_digest": hashlib.sha256(b'["skill:budget-sample"]').hexdigest(),
        }
        assert claim["exp"] - claim["iat"] == 60
    assert claims[0]["jti"] != claims[1]["jti"]
    with pytest.raises(ExecutionProofUnavailableError):
        await supplier.issue("billing.credit.consume", BINDING)
    assert len(reader.calls) == 3
