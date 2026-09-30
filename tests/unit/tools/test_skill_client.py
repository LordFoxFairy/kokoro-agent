from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from kokoro_agent.clients.skills import SkillClientError
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as pb

VECTOR = json.loads(
    (
        Path(__file__).parents[3]
        / "contract/platform/v1/execution-operations/v4/vectors/zip-v1.json"
    ).read_bytes()
)["vectors"][0]


class Sender:
    def __init__(self) -> None:
        self.calls: list[object] = []
        self.deny = False
        self.source = pb.SkillSource(
            source_ref=pb.SkillSourceRef(value="skill:skill-1"),
            skill_id=pb.SkillId(value="skill-1"),
            series_id=pb.SkillSeriesId(value="series-1"),
            revision=1,
            scope_kind=pb.SkillScopeKind.PERSONAL,
            package_asset_ref="asset-1",
            content_digest=VECTOR["zipSha256"],
            manifest_identity=VECTOR["manifestIdentity"],
        )

    async def send(
        self, request: object, *, timeout_s: float = 10
    ) -> pb.ResolveVisibleSkillResponse | pb.GetApprovedSkillPackageReferenceResponse:
        self.calls.append(request)
        if self.deny:
            raise SkillClientError("denied")
        if isinstance(request, pb.ResolveVisibleSkillRequest):
            return pb.ResolveVisibleSkillResponse(source=self.source)
        return pb.GetApprovedSkillPackageReferenceResponse(
            asset_ref="asset-1",
            content_digest=VECTOR["zipSha256"],
            manifest_identity=VECTOR["manifestIdentity"],
            transfer_reference=pb.PackageTransferReference(method="GET"),
        )


class Transfer:
    def __init__(self) -> None:
        self.calls = 0

    async def get(
        self, reference: pb.PackageTransferReference, content_digest: str
    ) -> bytes:
        self.calls += 1
        return base64.b64decode(VECTOR["zipBase64"])


@pytest.mark.asyncio
async def test_typed_resolve_and_each_read_authorizes_without_byte_cache() -> None:
    from kokoro_agent.clients.skills import PlatformSkillClient

    sender, transfer = Sender(), Transfer()
    client = PlatformSkillClient(sender, transfer)
    skills = await client.resolve(("skill:skill-1",))
    first = await client.load_package(skills[0])
    assert first["SKILL.md"] == b"# Skill"
    assert await client.load_package(skills[0]) == first
    assert len(sender.calls) == 3 and transfer.calls == 2
    sender.deny = True
    with pytest.raises(SkillClientError):
        await client.load_package(skills[0])
    assert transfer.calls == 2


@pytest.mark.asyncio
async def test_resolve_wrong_identity_rejected() -> None:
    from kokoro_agent.clients.skills import PlatformSkillClient

    sender = Sender()
    sender.source.skill_id = pb.SkillId(value="other")
    with pytest.raises(SkillClientError):
        await PlatformSkillClient(sender, Transfer()).resolve(("skill:skill-1",))


@pytest.mark.asyncio
async def test_backend_reauthorizes_all_read_surfaces_and_preserves_binary() -> None:
    from collections.abc import Mapping
    from kokoro_agent.clients.skills import ResolvedSkill
    from kokoro_agent.skills.backend import TypedSkillBackend

    class Reader:
        calls = 0
        deny = False

        async def load_package(self, skill: ResolvedSkill) -> Mapping[str, bytes]:
            self.calls += 1
            if self.deny:
                raise SkillClientError("revoked")
            return {"SKILL.md": b"hello", "binary.dat": b"\xff\x00"}

    skill = ResolvedSkill(
        source_ref="skill:Version.A",
        skill_id="Version.A",
        revision=1,
        asset_ref="asset",
        content_digest="a" * 64,
        manifest_identity="zip-v1:sha256:" + "b" * 64,
    )
    reader = Reader()
    backend = TypedSkillBackend((skill,), reader)
    path = "/" + skill.path_segment
    assert (await backend.als("/")).entries
    assert (await backend.als(path)).entries
    assert (await backend.aread(path + "/SKILL.md")).error is None
    assert (await backend.aglob("*")).matches
    assert (await backend.agrep("hello")).matches
    assert (await backend.adownload_files([path + "/binary.dat"]))[
        0
    ].content == b"\xff\x00"
    assert (
        await backend.aread(path + "/binary.dat")
    ).error == f"File '{path}/binary.dat' is not UTF-8 text"
    assert reader.calls == 7
    reader.deny = True
    for operation in (
        backend.als("/"),
        backend.aread(path + "/SKILL.md"),
        backend.aglob("*"),
        backend.agrep("hello"),
        backend.adownload_files([path + "/binary.dat"]),
    ):
        with pytest.raises(SkillClientError, match="revoked"):
            await operation


@pytest.mark.asyncio
async def test_backend_canonical_id_paths_no_alias_and_all_writes_denied() -> None:
    from collections.abc import Mapping
    from kokoro_agent.clients.skills import ResolvedSkill
    from kokoro_agent.skills.backend import TypedSkillBackend
    from deepagents.backends.protocol import PERMISSION_DENIED

    class Reader:
        async def load_package(self, skill: ResolvedSkill) -> Mapping[str, bytes]:
            return {"SKILL.md": skill.skill_id.encode()}

    skills = tuple(
        ResolvedSkill(
            source_ref="skill:" + key,
            skill_id=key,
            revision=1,
            asset_ref="asset",
            content_digest="a" * 64,
            manifest_identity="zip-v1:sha256:" + "b" * 64,
        )
        for key in ("Same.Name", "same.Name", "x" * 191)
    )
    assert len(skills[2].path_segment) == 255
    backend = TypedSkillBackend(skills, Reader())
    assert len((await backend.als("/")).entries or []) == 3
    path = "/" + skills[0].path_segment + "/SKILL.md"
    assert (await backend.adownload_files([path]))[0].content == b"Same.Name"
    for alias in (
        path.replace("/SKILL", "//SKILL"),
        path.replace("/SKILL", "/./SKILL"),
        path.replace("/SKILL", "/../SKILL"),
        path.replace("/SKILL", "=/SKILL"),
        "/Same.Name/SKILL.md",
        path.replace("/SKILL", "%2fSKILL"),
    ):
        assert (await backend.adownload_files([alias]))[0].content is None
    assert (await backend.awrite(path, "x")).error == PERMISSION_DENIED
    assert (await backend.aedit(path, "x", "y")).error == PERMISSION_DENIED
    assert (await backend.aupload_files([(path, b"x")]))[0].error == PERMISSION_DENIED


@pytest.mark.asyncio
async def test_backend_download_and_grep_have_aggregate_budgets() -> None:
    from collections.abc import Mapping
    from kokoro_agent.clients.skills import ResolvedSkill
    from kokoro_agent.skills.backend import TypedSkillBackend

    class Reader:
        async def load_package(self, skill: ResolvedSkill) -> Mapping[str, bytes]:
            return {"SKILL.md": b"x" * 16777216}

    skill = ResolvedSkill(
        source_ref="skill:x",
        skill_id="x",
        revision=1,
        asset_ref="asset",
        content_digest="a" * 64,
        manifest_identity="zip-v1:sha256:" + "b" * 64,
    )
    backend = TypedSkillBackend((skill,), Reader())
    path = "/" + skill.path_segment + "/SKILL.md"
    with pytest.raises(SkillClientError, match="SKILL_READ_LIMIT"):
        await backend.adownload_files([path] * 9)
    assert (await backend.agrep("x")).error == "SKILL_READ_LIMIT"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("source_ref", pb.SkillSourceRef(value="skill:other")),
        ("revision", 0),
        ("revision", 9007199254740992),
        ("scope_kind", pb.SkillScopeKind.UNSPECIFIED),
        ("content_digest", "wrong"),
        ("manifest_identity", "wrong"),
        ("package_asset_ref", ""),
        ("skill_id", None),
        ("series_id", None),
    ],
)
async def test_resolve_rejects_bad_owner_identity_without_get(
    field: str, value: object
) -> None:
    from kokoro_agent.clients.skills import PlatformSkillClient

    sender, transfer = Sender(), Transfer()
    setattr(sender.source, field, value)
    with pytest.raises(SkillClientError):
        await PlatformSkillClient(sender, transfer).resolve(("skill:skill-1",))
    assert transfer.calls == 0


@pytest.mark.asyncio
async def test_source_cancellation_propagates_without_transfer() -> None:
    import asyncio
    from kokoro_agent.clients.skills import PlatformSkillClient

    class Cancelled(Sender):
        async def send(
            self, request: object, *, timeout_s: float = 10
        ) -> (
            pb.ResolveVisibleSkillResponse | pb.GetApprovedSkillPackageReferenceResponse
        ):
            raise asyncio.CancelledError

    transfer = Transfer()
    with pytest.raises(asyncio.CancelledError):
        await PlatformSkillClient(Cancelled(), transfer).resolve(("skill:skill-1",))
    assert transfer.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["asset_ref", "content_digest", "manifest_identity", "transfer_reference"]
)
async def test_approved_mismatch_refuses_any_package(field: str) -> None:
    from kokoro_agent.clients.skills import PlatformSkillClient

    class Invalid(Sender):
        async def send(
            self, request: object, *, timeout_s: float = 10
        ) -> (
            pb.ResolveVisibleSkillResponse | pb.GetApprovedSkillPackageReferenceResponse
        ):
            response = await super().send(request, timeout_s=timeout_s)
            if isinstance(response, pb.GetApprovedSkillPackageReferenceResponse):
                setattr(
                    response, field, None if field == "transfer_reference" else "wrong"
                )
            return response

    transfer = Transfer()
    client = PlatformSkillClient(Invalid(), transfer)
    skills = await client.resolve(("skill:skill-1",))
    with pytest.raises(SkillClientError):
        await client.load_package(skills[0])
    assert transfer.calls == 0
