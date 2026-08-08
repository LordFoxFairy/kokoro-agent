# Hypercorn's public serve annotation includes an unparameterized WSGI fallback.
# This process passes an ASGI application and keeps that upstream gap at the boundary.
# pyright: reportUnknownVariableType=false

"""Standalone mTLS/HTTP2 process entry for PresentationService."""

from __future__ import annotations

import asyncio
import logging
import os

from dotenv import load_dotenv
from hypercorn.asyncio import serve as serve_asgi

from kokoro.agent.presentation.v1.presentation_connect import (
    PresentationServiceASGIApplication,
)
from kokoro_agent.config import AppConfig
from kokoro_agent.presentation.adapters.connect import PresentationConnectService
from kokoro_agent.presentation.delivery import PresentationProviderStore
from kokoro_agent.presentation.server import (
    PresentationServerSettings,
    build_hypercorn_config,
)
from kokoro_agent.storage.ledger import make_ledger

LOGGER = logging.getLogger(__name__)
_MAX_REQUEST_BYTES = 1024 * 1024


def build_presentation_app(
    store: PresentationProviderStore,
) -> PresentationServiceASGIApplication:
    return PresentationServiceASGIApplication(
        PresentationConnectService(store),
        read_max_bytes=_MAX_REQUEST_BYTES,
    )


async def _serve(config: AppConfig, settings: PresentationServerSettings) -> None:
    async with make_ledger(config.ledger) as store:
        app = build_presentation_app(store)
        LOGGER.info(
            "presentation provider listening on %s:%d; callers=%s",
            settings.host,
            settings.port,
            ",".join(sorted(settings.allowed_callers)),
        )
        await serve_asgi(app, build_hypercorn_config(settings))


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    config = AppConfig.from_env(os.environ)
    settings = PresentationServerSettings.from_values(
        host=config.presentation_host,
        port=config.presentation_port,
        tls_cert=config.presentation_tls_cert,
        tls_key=config.presentation_tls_key,
        caller_ca_bundle=config.presentation_caller_ca_bundle,
        allowed_callers=config.presentation_allowed_callers,
    )
    asyncio.run(_serve(config, settings))


if __name__ == "__main__":
    main()


__all__ = ["build_presentation_app", "main"]
