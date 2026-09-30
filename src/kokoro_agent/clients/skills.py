"""Run-bound typed Skill source reader; no package bytes or authorization are cached."""

from __future__ import annotations

import base64
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol
from uuid import uuid4

from kokoro_agent.clients.platform_transport import (
    PlatformCallError,
    PlatformRequest,
    PlatformResponse,
)
from kokoro_agent.clients.skill_package_transport import SkillTransferError
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as pb
from kokoro_agent.skills.package import SkillPackageError, validate_package


class SkillClientError(RuntimeError):
    """Stable Skill failure propagated to the existing Run failure boundary."""


@dataclass(frozen=True, slots=True, kw_only=True)
class ResolvedSkill:
    source_ref: str
    skill_id: str
    revision: int
    asset_ref: str
    content_digest: str
    manifest_identity: str

    @property
    def path_segment(self) -> str:
        return (
            base64.urlsafe_b64encode(self.skill_id.encode("ascii"))
            .decode("ascii")
            .rstrip("=")
        )


class SkillClient(Protocol):
    async def resolve(
        self, source_refs: Sequence[str]
    ) -> tuple[ResolvedSkill, ...]: ...
    async def load_package(self, skill: ResolvedSkill) -> Mapping[str, bytes]: ...


class SkillSender(Protocol):
    async def send(
        self, request: PlatformRequest, *, timeout_s: float = 10
    ) -> PlatformResponse: ...


class PackageTransfer(Protocol):
    async def get(
        self, reference: pb.PackageTransferReference, content_digest: str
    ) -> bytes: ...


class PlatformSkillClient:
    def __init__(self, sender: SkillSender, transfer: PackageTransfer) -> None:
        self._sender = sender
        self._transfer = transfer
        self._resolved: dict[str, ResolvedSkill] = {}

    async def resolve(self, source_refs: Sequence[str]) -> tuple[ResolvedSkill, ...]:
        resolved: list[ResolvedSkill] = []
        for ref in source_refs:
            if (
                re.fullmatch(r"skill:[A-Za-z0-9][A-Za-z0-9._:-]{0,190}", ref) is None
                or ref in self._resolved
            ):
                raise SkillClientError("SKILL_SOURCE_INVALID")
            response = await self._send(
                pb.ResolveVisibleSkillRequest(
                    request_id=str(uuid4()), source_ref=pb.SkillSourceRef(value=ref)
                )
            )
            if (
                not isinstance(response, pb.ResolveVisibleSkillResponse)
                or response.source is None
            ):
                raise SkillClientError("SKILL_SOURCE_INVALID")
            source = response.source
            if (
                source.source_ref is None
                or source.source_ref.value != ref
                or source.skill_id is None
                or source.skill_id.value != ref[6:]
                or type(source.revision) is not int
                or not 1 <= source.revision <= 9007199254740991
                or source.scope_kind
                not in (
                    pb.SkillScopeKind.PERSONAL,
                    pb.SkillScopeKind.PROJECT,
                    pb.SkillScopeKind.ORGANIZATION,
                    pb.SkillScopeKind.SESSION,
                )
                or source.series_id is None
                or re.fullmatch(
                    r"[A-Za-z0-9][A-Za-z0-9._:-]{0,190}", source.series_id.value
                )
                is None
                or not source.package_asset_ref
                or len(source.package_asset_ref) > 2048
                or re.fullmatch(r"[0-9a-f]{64}", source.content_digest) is None
                or re.fullmatch(r"zip-v1:sha256:[0-9a-f]{64}", source.manifest_identity)
                is None
            ):
                raise SkillClientError("SKILL_SOURCE_INVALID")
            skill = ResolvedSkill(
                source_ref=ref,
                skill_id=source.skill_id.value,
                revision=source.revision,
                asset_ref=source.package_asset_ref,
                content_digest=source.content_digest,
                manifest_identity=source.manifest_identity,
            )
            self._resolved[ref] = skill
            resolved.append(skill)
        return tuple(resolved)

    async def _send(self, request: PlatformRequest) -> PlatformResponse:
        try:
            return await self._sender.send(request)
        except PlatformCallError:
            pass
        raise SkillClientError("SKILL_AUTHORIZATION_UNAVAILABLE") from None

    async def load_package(self, skill: ResolvedSkill) -> Mapping[str, bytes]:
        if self._resolved.get(skill.source_ref) != skill:
            raise SkillClientError("SKILL_SOURCE_INVALID")
        response = await self._send(
            pb.GetApprovedSkillPackageReferenceRequest(
                request_id=str(uuid4()),
                source_ref=pb.SkillSourceRef(value=skill.source_ref),
            )
        )
        if (
            not isinstance(response, pb.GetApprovedSkillPackageReferenceResponse)
            or response.asset_ref != skill.asset_ref
            or response.content_digest != skill.content_digest
            or response.manifest_identity != skill.manifest_identity
            or response.transfer_reference is None
        ):
            raise SkillClientError("SKILL_PACKAGE_IDENTITY_MISMATCH")
        failure = "SKILL_PACKAGE_INVALID"
        try:
            payload = await self._transfer.get(
                response.transfer_reference, skill.content_digest
            )
            package = validate_package(
                payload,
                skill_id=skill.skill_id,
                revision=skill.revision,
                manifest_identity=skill.manifest_identity,
            )
            return package
        except (SkillTransferError, SkillPackageError):
            pass
        raise SkillClientError(failure) from None


__all__ = ["ResolvedSkill", "SkillClient", "SkillClientError", "PlatformSkillClient"]
