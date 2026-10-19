"""Skill reference policy over the shared isolated ObjectStore byte boundary."""

from __future__ import annotations

import httpx
from kokoro_agent.clients.package_transport import (
    PackageTransport,
    PackageTransferError,
)
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as pb


class SkillTransferError(RuntimeError):
    """Stable transfer error without URL/header/response disclosure."""


class SkillPackageTransport:
    def __init__(
        self, origin: str, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self._packages = PackageTransport(origin, transport=transport)

    async def __aenter__(self) -> SkillPackageTransport:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._packages.aclose()

    async def get(
        self, reference: pb.PackageTransferReference, content_digest: str
    ) -> bytes:
        failure = "SKILL_TRANSFER_INVALID"
        try:
            if (
                reference.method != "GET"
                or reference.required_headers
                or reference.expires_at is None
            ):
                raise ValueError("invalid Skill reference")
            return await self._packages.get(
                url=reference.url,
                headers={},
                expiry=reference.expires_at.to_datetime(),
                digest=content_digest,
            )
        except PackageTransferError as error:
            failure = "SKILL_" + str(error)
        except (ValueError, OverflowError):
            pass
        raise SkillTransferError(failure) from None
