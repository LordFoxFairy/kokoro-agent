# Hypercorn's public serve annotation includes an unparameterized WSGI fallback.
# This process passes an ASGI application and keeps that upstream gap at the boundary.
# pyright: reportUnknownVariableType=false

"""Standalone mTLS/HTTP2 process entry for execution-evidence reads."""

from __future__ import annotations

import asyncio
import logging
import os

from dotenv import load_dotenv
from hypercorn.asyncio import serve as serve_asgi

from kokoro.agent.execution.v1.agent_execution_evidence_connect import (
    AgentExecutionEvidenceServiceASGIApplication,
)
from kokoro_agent.config import AppConfig
from kokoro_agent.evidence.server import EvidenceServerSettings, build_hypercorn_config
from kokoro_agent.evidence.service import AgentExecutionEvidenceConnectService
from kokoro_agent.storage.ledger import make_ledger

LOGGER = logging.getLogger(__name__)
_MAX_REQUEST_BYTES = 128 * 1024


async def _serve(config: AppConfig, settings: EvidenceServerSettings) -> None:
    async with make_ledger(config.ledger) as reader:
        service = AgentExecutionEvidenceConnectService(reader)
        app = AgentExecutionEvidenceServiceASGIApplication(
            service,
            read_max_bytes=_MAX_REQUEST_BYTES,
        )
        LOGGER.info(
            "execution evidence provider listening on %s:%d; callers=%s",
            settings.host,
            settings.port,
            ",".join(sorted(settings.allowed_callers)),
        )
        await serve_asgi(app, build_hypercorn_config(settings))


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    config = AppConfig.from_env(os.environ)
    settings = EvidenceServerSettings.from_values(
        host=config.evidence_host,
        port=config.evidence_port,
        tls_cert=config.evidence_tls_cert,
        tls_key=config.evidence_tls_key,
        caller_ca_bundle=config.evidence_caller_ca_bundle,
        allowed_callers=config.evidence_allowed_callers,
    )
    asyncio.run(_serve(config, settings))


if __name__ == "__main__":
    main()
