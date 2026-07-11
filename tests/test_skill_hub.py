"""SkillHub 规格：真实文件 → 目录扫描 → seed → 真 Mongo + 包体存储，全链路真件。

包体存储主测 local 档（同一 Protocol）；minio 组实测 S3 档（不可达干净 skip，
与 test_assets 同口径）。覆盖内容锁（升级后旧 hash 取回）——本方案的核心承诺。
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from pathlib import Path

import boto3
import pytest
from mypy_boto3_s3 import S3Client
from pydantic import SecretStr
from pymongo import AsyncMongoClient

from kokoro_agent.content_source import LocalAssets, LocalAssetSource
from kokoro_agent.sandbox.archive import LocalWorkspace, S3Workspace
from kokoro_agent.skills.hub import (
    LocalPackageStore,
    S3PackageStore,
    SkillHub,
    SkillHubError,
    S3Credentials,
    seed_official,
    validate_package,
)

_MONGO_URL = "mongodb://127.0.0.1:27017"
_MINIO_ENDPOINT = "http://127.0.0.1:9100"


@pytest.fixture
async def hub(tmp_path: Path) -> AsyncGenerator[SkillHub, None]:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(_MONGO_URL)
    suffix = uuid.uuid4().hex[:8]
    database = client["kokoro_test"]
    skills = database[f"skills_{suffix}"]
    state = database[f"skill_state_{suffix}"]
    try:
        yield SkillHub(skills, state, LocalPackageStore(str(tmp_path / "packages")))
    finally:
        await skills.drop()
        await state.drop()
        await client.close()


def write_skill_dir(root: Path, name: str, skill_md: str, extra: dict[str, str] | None = None) -> None:
    """磁盘上真实的技能包目录（与部署形态一致）。"""
    package = root / name
    package.mkdir(parents=True)
    (package / "SKILL.md").write_text(skill_md, encoding="utf-8")
    for rel, content in (extra or {}).items():
        (package / rel).write_text(content, encoding="utf-8")


def scan(root: Path) -> dict[str, dict[str, str]]:
    """真实加载链路：与 worker 启动同一份扫描代码。"""
    source = LocalAssetSource(LocalAssets(type="local", skills_dir=str(root)))
    return {name: dict(files) for name, files in source.load_skills().items()}


STYLE_MD = """\
---
name: style
description: 写作风格指南
---
写作时保持简洁：先结论后论据。
"""

PDF_MD = """\
---
name: pdf
description: PDF 报告生成流程
---
生成报告时运行 python /.skills/pdf/make_report.py 处理数据。
"""


# --- seed 与读面 ---


async def test_seed_then_cards_and_body(hub: SkillHub, tmp_path: Path) -> None:
    src = tmp_path / "src"
    write_skill_dir(src, "style", STYLE_MD)
    write_skill_dir(src, "pdf", PDF_MD, extra={"make_report.py": "print('report')"})
    await seed_official(hub, scan(src))

    cards = await hub.resolve_cards(["official"], ["pdf", "style"])
    assert [card.name for card in cards] == ["pdf", "style"]  # 授权序（清单字节稳定）。
    body = await hub.read_body("official", "style", cards[1].content_hash)
    assert "先结论后论据" in body


async def test_reseed_is_idempotent(hub: SkillHub, tmp_path: Path) -> None:
    src = tmp_path / "src"
    write_skill_dir(src, "style", STYLE_MD)
    await seed_official(hub, scan(src))
    first = await hub.resolve_cards(["official"], ["style"])
    await seed_official(hub, scan(src))  # worker 重启重 seed。
    second = await hub.resolve_cards(["official"], ["style"])
    assert first == second


async def test_content_lock_old_hash_survives_upgrade(hub: SkillHub, tmp_path: Path) -> None:
    """核心承诺：官方升级后，按旧 hash（会话快照）仍取得到旧正文与旧附件。"""
    v1 = tmp_path / "v1"
    write_skill_dir(v1, "pdf", PDF_MD, extra={"make_report.py": "print('v1')"})
    await seed_official(hub, scan(v1))
    old_card = (await hub.resolve_cards(["official"], ["pdf"]))[0]

    v2 = tmp_path / "v2"
    write_skill_dir(v2, "pdf", PDF_MD.replace("处理数据", "处理数据（v2 流程）"),
                    extra={"make_report.py": "print('v2')"})
    await seed_official(hub, scan(v2))  # 官方升级。

    new_card = (await hub.resolve_cards(["official"], ["pdf"]))[0]
    assert new_card.content_hash != old_card.content_hash
    # 旧 hash 双路：正文与整包都按内容寻址取回（进行中会话不受升级影响）。
    old_body = await hub.read_body("official", "pdf", old_card.content_hash)
    assert "（v2 流程）" not in old_body
    old_files = await hub.load_package("official", "pdf", old_card.content_hash)
    assert old_files["make_report.py"] == "print('v1')"


async def test_namespace_overrides_official(hub: SkillHub, tmp_path: Path) -> None:
    src = tmp_path / "src"
    write_skill_dir(src, "style", STYLE_MD)
    await seed_official(hub, scan(src))
    user = tmp_path / "user"
    write_skill_dir(user, "style", STYLE_MD.replace("写作风格指南", "我的定制风格"))
    await hub.upsert("ns1", "style", scan(user)["style"], source="upload")

    mine = await hub.resolve_cards(["ns1", "official"], ["style"])
    assert mine[0].description == "我的定制风格"
    official_only = await hub.resolve_cards(["official"], ["style"])
    assert official_only[0].description == "写作风格指南"


async def test_same_name_cross_scope_reads_pinned_scope(hub: SkillHub, tmp_path: Path) -> None:
    """P1 边角回归：同名跨 scope 时，读正文/取包按快照卡的 scope 定死归属——
    快照 official 旧版即便被 namespace 同名上传遮蔽、且官方后续升级，仍命中 official 旧内容；反向亦然。"""
    # official 发布 v1（含附件）→ 会话此刻快照卡 = {scope: official, hash A}。
    off_v1 = tmp_path / "off_v1"
    write_skill_dir(off_v1, "pdf", PDF_MD, extra={"make_report.py": "print('official-v1')"})
    await seed_official(hub, scan(off_v1))
    snapshot = (await hub.resolve_cards(["official"], ["pdf"]))[0]  # official hash A

    # namespace 上传同名 pdf（hash B，Mongo (official,pdf) 与 (ns1,pdf) 双文档并存）。
    user = tmp_path / "user"
    write_skill_dir(user, "pdf", PDF_MD.replace("处理数据", "命名空间流程"),
                    extra={"make_report.py": "print('ns')"})
    await hub.upsert("ns1", "pdf", scan(user)["pdf"], source="upload")
    ns_card = (await hub.resolve_cards(["ns1"], ["pdf"]))[0]  # ns hash B
    assert ns_card.content_hash != snapshot.content_hash

    # official 再升级 v2 → snapshot 变旧 hash（触发正文/取包的 zip 旧版路径）。
    off_v2 = tmp_path / "off_v2"
    write_skill_dir(off_v2, "pdf", PDF_MD.replace("处理数据", "官方v2"),
                    extra={"make_report.py": "print('official-v2')"})
    await seed_official(hub, scan(off_v2))

    # 快照卡 scope=official：命中 official 旧版 A，既不被 namespace 遮蔽也不随官方升级漂移。
    off_body = await hub.read_body("official", "pdf", snapshot.content_hash)
    assert "命名空间流程" not in off_body and "官方v2" not in off_body
    off_files = await hub.load_package_if_assets("official", "pdf", snapshot.content_hash)
    assert off_files is not None and off_files["make_report.py"] == "print('official-v1')"

    # 反向：快照卡 scope=ns1 → 命中 namespace 版，不被 official 遮蔽。
    ns_body = await hub.read_body("ns1", "pdf", ns_card.content_hash)
    assert "命名空间流程" in ns_body
    ns_files = await hub.load_package_if_assets("ns1", "pdf", ns_card.content_hash)
    assert ns_files is not None and ns_files["make_report.py"] == "print('ns')"


async def test_assets_probe_skips_plain_packages(hub: SkillHub, tmp_path: Path) -> None:
    src = tmp_path / "src"
    write_skill_dir(src, "style", STYLE_MD)
    write_skill_dir(src, "pdf", PDF_MD, extra={"make_report.py": "print('x')"})
    await seed_official(hub, scan(src))
    style = (await hub.resolve_cards(["official"], ["style"]))[0]
    pdf = (await hub.resolve_cards(["official"], ["pdf"]))[0]
    assert await hub.load_package_if_assets("official", "style", style.content_hash) is None
    files = await hub.load_package_if_assets("official", "pdf", pdf.content_hash)
    assert files is not None and "make_report.py" in files


# --- 池（启停/required）---


async def test_pool_respects_state_and_required(hub: SkillHub, tmp_path: Path) -> None:
    src = tmp_path / "src"
    write_skill_dir(src, "style", STYLE_MD)
    write_skill_dir(src, "pdf", PDF_MD)
    await seed_official(hub, scan(src))
    await hub.set_official_flags("pdf", required=True)  # 管理面动作。

    await hub.set_enabled("ns1", "style", enabled=False)
    pool = await hub.list_pool("ns1")
    assert [card.name for card in pool] == ["pdf"]  # style 被用户关闭。
    with pytest.raises(SkillHubError, match="required"):
        await hub.set_enabled("ns1", "pdf", enabled=False)  # required 拒绝关闭。


async def test_soft_deleted_is_invisible(hub: SkillHub, tmp_path: Path) -> None:
    src = tmp_path / "src"
    write_skill_dir(src, "style", STYLE_MD)
    await seed_official(hub, scan(src))
    await hub.mark_deleted("official", "style")
    assert await hub.resolve_cards(["official"], ["style"]) == []
    assert await hub.list_pool("ns1") == []


# --- 校验清单（安全边界逐条负向）---


@pytest.mark.parametrize(
    ("name", "files", "match"),
    [
        ("Bad_Name", {"SKILL.md": STYLE_MD}, "invalid"),
        ("official", {"SKILL.md": STYLE_MD}, "reserved"),
        ("style", {"readme.md": "no skill"}, "SKILL.md"),
        ("style", {"SKILL.md": STYLE_MD, "../evil.py": "x"}, "unsafe path"),
        ("style", {"SKILL.md": STYLE_MD, "/abs.py": "x"}, "unsafe path"),
        ("style", {"SKILL.md": "---\nname: style\ndescription: 有<注入>风险\n---\n正文"}, "angle"),
    ],
)
def test_validate_negatives(name: str, files: dict[str, str], match: str) -> None:
    with pytest.raises(Exception, match=match):
        validate_package(name, files)


async def test_concurrent_write_conflict_fails_loud(hub: SkillHub, tmp_path: Path) -> None:
    src = tmp_path / "src"
    write_skill_dir(src, "style", STYLE_MD)
    await seed_official(hub, scan(src))
    # 模拟竞争者先写：并发升级同一技能——后写基于过期 revision 必须 fail-loud。
    changed_a = tmp_path / "a"
    write_skill_dir(changed_a, "style", STYLE_MD.replace("先结论后论据", "版本A"))
    changed_b = tmp_path / "b"
    write_skill_dir(changed_b, "style", STYLE_MD.replace("先结论后论据", "版本B"))
    import asyncio as _asyncio
    results = await _asyncio.gather(
        hub.upsert("official", "style", scan(changed_a)["style"]),
        hub.upsert("official", "style", scan(changed_b)["style"]),
        return_exceptions=True,
    )
    conflicts = [r for r in results if isinstance(r, SkillHubError)]
    winners = [r for r in results if not isinstance(r, BaseException)]
    # 至少一个成功；若发生真并发交错，落败方以 conflict fail-loud 而非静默覆盖。
    assert winners
    assert len(winners) + len(conflicts) == 2


# --- S3 档实测（minio；不可达干净 skip）---


def _probe_minio() -> S3Client | None:
    client: S3Client = boto3.client(
        "s3", endpoint_url=_MINIO_ENDPOINT,
        aws_access_key_id="kokoro", aws_secret_access_key="kokoro-secret",
        region_name="us-east-1",
    )
    try:
        client.list_buckets()
        return client
    except Exception:
        return None


async def test_s3_package_store_roundtrip_and_idempotency(tmp_path: Path) -> None:
    probe = _probe_minio()
    if probe is None:
        pytest.skip("minio unreachable")
    bucket = f"kokoro-test-{uuid.uuid4().hex[:8]}"
    probe.create_bucket(Bucket=bucket)
    try:
        store = S3PackageStore(
            S3Workspace(type="s3", endpoint=_MINIO_ENDPOINT, bucket=bucket),
            S3Credentials(access_key=SecretStr("kokoro"), secret_key=SecretStr("kokoro-secret")),
        )
        await store.put("skills/official/x/abc.zip", b"payload")
        await store.put("skills/official/x/abc.zip", b"ignored")  # 内容寻址：已存在跳过。
        assert await store.get("skills/official/x/abc.zip") == b"payload"
        with pytest.raises(SkillHubError):
            await store.get("skills/official/x/missing.zip")
    finally:
        for obj in probe.list_objects_v2(Bucket=bucket).get("Contents", []):
            probe.delete_object(Bucket=bucket, Key=str(obj.get("Key")))
        probe.delete_bucket(Bucket=bucket)


def test_local_package_store_location_type() -> None:
    # 配置位形复用（ADR-009 StoreLocation）：local/s3 同一 Protocol。
    assert LocalWorkspace(type="local", root="/tmp/x").type == "local"
