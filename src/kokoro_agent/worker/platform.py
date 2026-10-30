"""Worker process resource lifetime and immutable per-lease Platform composition."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

from kokoro_agent.clients.mcp_runtime import PlatformMcpClient
from kokoro_agent.clients.custom_api_runtime import PlatformCustomApiClient
from kokoro_agent.clients.mcp_package_transport import McpPackageTransport
from kokoro_agent.mcp.egress import current_egress_mode
from kokoro_agent.clients.skills import PlatformSkillClient, SkillClientError
from kokoro_agent.clients.skill_package_transport import SkillPackageTransport
from kokoro_agent.clients.platform_credentials import CredentialFile
from kokoro_agent.clients.platform_transport import PlatformTransport, RunPlatformClient
from kokoro_agent.config import AppConfig
from kokoro_agent.domain.run.models import LeasedRun
from kokoro_agent.execution.execution_proof_signer import ExecutionProofSigner
from kokoro_agent.execution.execution_proof_supplier import (
    ExecutionProofLeaseReadPort,
    create_execution_proof_supplier,
)


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class WorkerPlatformRuntime:
    credentials: CredentialFile
    transport: PlatformTransport
    signer: ExecutionProofSigner
    lease_reader: ExecutionProofLeaseReadPort

    packages: SkillPackageTransport | None = None
    mcp_packages: McpPackageTransport | None = None

    def custom_api_for_run(self, leased_run: LeasedRun) -> PlatformCustomApiClient:
        return PlatformCustomApiClient(
            sender=self.for_run(leased_run),
            tenant_id=leased_run.request.execution_identity.tenant_ref,
            run_id=leased_run.request.run_id,
            execution_session_id=leased_run.request.session_id,
        )

    def mcp_for_run(self, leased_run: LeasedRun) -> PlatformMcpClient:
        return PlatformMcpClient(
            self.for_run(leased_run),
            packages=self.mcp_packages,
            run_id=leased_run.request.run_id,
            egress_mode=current_egress_mode(),
        )

    def skills_for_run(self, leased_run: LeasedRun) -> PlatformSkillClient:
        if self.packages is None:
            raise SkillClientError("SKILL_PACKAGE_TRANSPORT_UNAVAILABLE")
        return PlatformSkillClient(self.for_run(leased_run), self.packages)

    def for_run(self, leased_run: LeasedRun) -> RunPlatformClient:
        supplier = create_execution_proof_supplier(
            leased_run=leased_run, lease_reader=self.lease_reader, signer=self.signer
        )
        return RunPlatformClient(
            tenant_id=leased_run.request.execution_identity.tenant_ref,
            credentials=self.credentials,
            proofs=supplier,
            transport=self.transport,
            frozen_custom_api_profiles=tuple(
                (ref.profile_id, ref.expected_state_version)
                for ref in leased_run.request.selected_custom_api_profiles
            ),
        )


@asynccontextmanager
async def worker_platform_runtime(
    config: AppConfig,
    *,
    signer: ExecutionProofSigner,
    lease_reader: ExecutionProofLeaseReadPort,
) -> AsyncGenerator[WorkerPlatformRuntime | None, None]:
    if config.platform_base_url is None:
        yield None
        return
    # AppConfig enforces all-or-none; these checks also narrow the private descriptor types.
    if config.platform_service_credentials_file is None:
        raise ValueError("incomplete worker Platform configuration")
    credentials = CredentialFile(config.platform_service_credentials_file)
    credentials.validate()
    async with (
        AsyncExitStack() as stack,
        PlatformTransport(config.platform_base_url) as transport,
    ):
        packages = None
        mcp_packages = None
        if config.storage_object_origin is not None:
            packages = await stack.enter_async_context(
                SkillPackageTransport(config.storage_object_origin)
            )
            mcp_packages = await stack.enter_async_context(
                McpPackageTransport(config.storage_object_origin)
            )
        yield WorkerPlatformRuntime(
            credentials=credentials,
            transport=transport,
            signer=signer,
            lease_reader=lease_reader,
            packages=packages,
            mcp_packages=mcp_packages,
        )
