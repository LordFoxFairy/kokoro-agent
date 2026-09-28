"""Worker process resource lifetime and immutable per-lease Platform composition."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx

from kokoro_agent.clients.platform_credentials import CredentialFile
from kokoro_agent.clients.platform_tokens import PlatformTokenProvider
from kokoro_agent.clients.platform_transport import PlatformTransport, RunPlatformClient
from kokoro_agent.config import AppConfig
from kokoro_agent.domain.run.models import LeasedRun
from kokoro_agent.execution.execution_proof_keys import (
    WorkerExecutionProofConfig,
    load_execution_proof_signer,
)
from kokoro_agent.execution.execution_proof_signer import ExecutionProofSigner
from kokoro_agent.execution.execution_proof_supplier import (
    ExecutionProofLeaseReadPort,
    create_execution_proof_supplier,
)
from kokoro_agent.infrastructure.postgres_execution_proof_lease import (
    PostgresExecutionProofLeaseConfig,
    PostgresExecutionProofLeaseReader,
)


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class WorkerPlatformRuntime:
    tokens: PlatformTokenProvider
    transport: PlatformTransport
    signer: ExecutionProofSigner
    lease_reader: ExecutionProofLeaseReadPort

    def for_run(self, leased_run: LeasedRun) -> RunPlatformClient:
        supplier = create_execution_proof_supplier(
            leased_run=leased_run, lease_reader=self.lease_reader, signer=self.signer
        )
        return RunPlatformClient(
            tenant_id=leased_run.request.execution_identity.tenant_ref,
            tokens=self.tokens,
            proofs=supplier,
            transport=self.transport,
        )


@asynccontextmanager
async def worker_platform_runtime(
    config: AppConfig,
) -> AsyncGenerator[WorkerPlatformRuntime | None, None]:
    if config.platform_base_url is None:
        yield None
        return
    # AppConfig enforces all-or-none; these checks also narrow the private descriptor types.
    if (
        config.iam_base_url is None
        or config.platform_credentials_file is None
        or config.execution_proof_issuer is None
        or config.execution_proof_private_key_file is None
        or config.execution_proof_active_kid is None
        or config.execution_proof_thumbprint is None
    ):
        raise ValueError("incomplete worker Platform configuration")
    credentials = CredentialFile(config.platform_credentials_file)
    credentials.validate()
    signer = load_execution_proof_signer(
        WorkerExecutionProofConfig(
            issuer=config.execution_proof_issuer,
            private_key_file=config.execution_proof_private_key_file,
            active_kid=config.execution_proof_active_kid,
            active_jwk_thumbprint_sha256=config.execution_proof_thumbprint,
        )
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url=config.database_url, schema_name=config.database_schema
        )
    )
    async with (
        httpx.AsyncClient(
            follow_redirects=False,
            trust_env=False,
            timeout=5,
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=8),
        ) as http,
        PlatformTransport(config.platform_base_url) as transport,
    ):
        tokens = PlatformTokenProvider(config.iam_base_url, credentials, http=http)
        try:
            yield WorkerPlatformRuntime(
                tokens=tokens, transport=transport, signer=signer, lease_reader=reader
            )
        finally:
            await tokens.aclose()
