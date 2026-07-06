"""资产域规格（Skills V2）：frontmatter 契约 + 整包装载（local/s3）+ 渐进披露真图 + 配置矩阵。

minio 不可达时 s3 实测组干净 skip；其余恒跑。
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.outputs import ChatResult
from mypy_boto3_s3 import S3Client
from pydantic import SecretStr, ValidationError

from deepagents.backends.utils import create_file_data

from fakes import usage_recorder
from kokoro_agent.content_source import (
    AssetSettings,
    AssetSourceError,
    LocalAssets,
    LocalAssetSource,
    S3Assets,
    S3AssetSource,
    load_asset_libraries,
    load_assets_config,
)
from kokoro_agent.skills import (
    MAIN_SKILLS_SOURCE,
    SkillAssetError,
    SkillLibrary,
    build_packages,
)
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.streams.memory import MemoryStream

MINIO_URL = "http://127.0.0.1:9100"
BUCKET = f"kokoro-assets-test-{int(time.time())}"


def fm(name: str, description: str = "技能描述") -> str:
    return f"---\nname: {name}\ndescription: {description}\n---\n\n"


def _write_skill(root: Path, name: str, body: str, description: str = "技能描述") -> None:
    d = root / name
    d.mkdir()
    (d / "SKILL.md").write_text(fm(name, description) + body)


def _local_library(tmp_path: Path, skills: dict[str, str]) -> SkillLibrary:
    for name, body in skills.items():
        _write_skill(tmp_path, name, body)
    source = LocalAssetSource(LocalAssets(type="local", skills_dir=str(tmp_path)))
    return SkillLibrary(build_packages(source.load_skills()))


# --- frontmatter 契约（S1） ---


def test_package_load_with_frontmatter_and_helper_files(tmp_path: Path) -> None:
    _write_skill(tmp_path, "style", "正文指引", description="风格技能")
    (tmp_path / "style" / "helper.md").write_text("辅助文件")
    source = LocalAssetSource(LocalAssets(type="local", skills_dir=str(tmp_path)))
    library = SkillLibrary(build_packages(source.load_skills()))
    package = library.get("style")
    assert package.description == "风格技能"
    assert "正文指引" in package.files["SKILL.md"]
    assert package.files["helper.md"] == "辅助文件"


@pytest.mark.parametrize(
    ("content", "match"),
    [
        ("没有头的裸文本", "missing YAML frontmatter"),
        ("---\nname: style\ndescription: d", "unterminated"),
        ("---\nname: other\ndescription: d\n---\n正文", "must match directory"),
        ("---\nname: style\ndescription: '  '\n---\n正文", "non-empty"),
        ("---\ndescription: d\n---\n正文", "validation error"),
    ],
)
def test_frontmatter_negatives_fail_loud(tmp_path: Path, content: str, match: str) -> None:
    d = tmp_path / "style"
    d.mkdir()
    (d / "SKILL.md").write_text(content)
    source = LocalAssetSource(LocalAssets(type="local", skills_dir=str(tmp_path)))
    with pytest.raises((SkillAssetError, ValidationError)):
        build_packages(source.load_skills())


def test_unknown_name_fails_loud(tmp_path: Path) -> None:
    library = _local_library(tmp_path, {"alpha": "A"})
    with pytest.raises(SkillAssetError, match="ghost"):
        library.get("ghost")


def test_snapshot_ignores_post_start_edits(tmp_path: Path) -> None:
    # 快照语义：装载后盘上被改，取用仍是装载时内容——改资产=滚动重启。
    library = _local_library(tmp_path, {"alpha": "原文"})
    (tmp_path / "alpha" / "SKILL.md").write_text(fm("alpha") + "被篡改")
    assert "原文" in library.get("alpha").files["SKILL.md"]


def test_dir_without_skill_md_fails_loud(tmp_path: Path) -> None:
    (tmp_path / "broken").mkdir()
    (tmp_path / "broken" / "notes.md").write_text("无 SKILL.md")
    source = LocalAssetSource(LocalAssets(type="local", skills_dir=str(tmp_path)))
    with pytest.raises(AssetSourceError, match="no SKILL.md"):
        source.load_skills()


def test_configured_missing_dir_fails_loud(tmp_path: Path) -> None:
    source = LocalAssetSource(
        LocalAssets(
            type="local",
            skills_dir=str(tmp_path / "nope"),
            personas_dir=str(tmp_path / "nope"),
        )
    )
    with pytest.raises(AssetSourceError, match="not a directory"):
        source.load_skills()
    with pytest.raises(AssetSourceError, match="not a directory"):
        source.load_personas()


def test_unconfigured_source_is_empty_library() -> None:
    skills, prompts = load_asset_libraries(
        AssetSettings(source=LocalAssets(type="local"), s3_access_key=None, s3_secret_key=None)
    )
    assert skills.names() == frozenset()
    assert prompts.get("ghost") is None


# --- 配置矩阵（type 判别 yaml + 凭据 env-only） ---


def test_assets_config_local_yaml(tmp_path: Path) -> None:
    path = tmp_path / "assets.yaml"
    path.write_text("assets:\n  type: local\n  skills_dir: /data/skills\n")
    config = load_assets_config(str(path))
    assert isinstance(config, LocalAssets)
    assert config.skills_dir == "/data/skills"


def test_assets_config_s3_yaml_with_defaults(tmp_path: Path) -> None:
    path = tmp_path / "assets.yaml"
    path.write_text(
        "assets:\n  type: s3\n  endpoint: http://127.0.0.1:9100\n  bucket: kokoro-assets\n"
    )
    config = load_assets_config(str(path))
    assert isinstance(config, S3Assets)
    assert config.region == "us-east-1"
    assert config.prefix == ""


def test_assets_config_unset_means_none() -> None:
    assert load_assets_config(None) is None
    assert load_assets_config("") is None


@pytest.mark.parametrize(
    "body",
    [
        "assets:\n  type: s3\n  endpoint: e\n  bucket: b\n  access_key: leaked\n",
        "assets:\n  type: local\n  unknown_key: x\n",
        "assets:\n  type: ftp\n  endpoint: e\n",
    ],
)
def test_assets_config_rejects_bad_yaml(tmp_path: Path, body: str) -> None:
    path = tmp_path / "assets.yaml"
    path.write_text(body)
    with pytest.raises(ValidationError):
        load_assets_config(str(path))


def test_s3_source_requires_credentials() -> None:
    with pytest.raises(ValidationError, match="KOKORO_ASSETS_S3_ACCESS_KEY"):
        AssetSettings(
            source=S3Assets(type="s3", endpoint="http://e", bucket="b"),
            s3_access_key=None,
            s3_secret_key=None,
        )


# --- s3 源实测（minio） ---


def _probe_minio() -> S3Client | None:
    client: S3Client = boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        region_name="us-east-1",
        aws_access_key_id="kokoro",
        aws_secret_access_key="kokoro-secret",
        config=BotoConfig(
            s3={"addressing_style": "path"},
            connect_timeout=1,
            read_timeout=2,
            retries={"max_attempts": 1},
        ),
    )
    try:
        client.create_bucket(Bucket=BUCKET)
        return client
    except Exception:
        return None


_MINIO = _probe_minio()


@pytest.mark.skipif(_MINIO is None, reason=f"minio {MINIO_URL} 不可达（启动见 docs/test-cases.md）")
class TestS3AssetSource:
    def _put(self, key: str, body: str) -> None:
        assert _MINIO is not None
        _MINIO.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))

    def _source(self, prefix: str) -> S3AssetSource:
        return S3AssetSource(
            S3Assets(type="s3", endpoint=MINIO_URL, bucket=BUCKET, prefix=prefix),
            access_key=SecretStr("kokoro"),
            secret_key=SecretStr("kokoro-secret"),
        )

    def test_loads_full_packages_under_prefix(self) -> None:
        self._put("deploy/skills/style/SKILL.md", fm("style", "s3 技能") + "s3 正文")
        self._put("deploy/skills/style/helper.md", "辅助")
        self._put("deploy/personas/poet.md", "s3 诗人 prompt\n")
        source = self._source("deploy")
        library = SkillLibrary(build_packages(source.load_skills()))
        package = library.get("style")
        assert package.description == "s3 技能"
        assert package.files["helper.md"] == "辅助"
        assert dict(source.load_personas()) == {"poet": "s3 诗人 prompt"}

    def test_skill_without_skill_md_fails_loud(self) -> None:
        self._put("broken/skills/ghost/notes.txt", "无 SKILL.md")
        with pytest.raises(AssetSourceError, match="no SKILL.md"):
            self._source("broken").load_skills()

    def test_load_asset_libraries_end_to_end(self) -> None:
        self._put("e2e/skills/tone/SKILL.md", fm("tone", "语气技能") + "via-s3-skill 正文")
        skills, _prompts = load_asset_libraries(
            AssetSettings(
                source=S3Assets(type="s3", endpoint=MINIO_URL, bucket=BUCKET, prefix="e2e"),
                s3_access_key=SecretStr("kokoro"),
                s3_secret_key=SecretStr("kokoro-secret"),
            )
        )
        assert "via-s3-skill" in skills.get("tone").files["SKILL.md"]


# --- 渐进披露真图（Skills V2 的核心行为断言） ---


@pytest.mark.asyncio
async def test_progressive_disclosure_prompt_has_description_not_body(tmp_path: Path) -> None:
    # 原生 SkillsMiddleware：system prompt 只挂 name+description，正文不进 prompt——
    # 与 V1 全文注入相反的行为钉（正文经 read_file 渐进获取）。
    captured: list[list[BaseMessage]] = []

    class Recorder(LocalFakeChatModel):
        def _generate(
            self,
            messages: list[BaseMessage],
            stop: list[str] | None = None,
            run_manager: CallbackManagerForLLMRun | None = None,
            **kwargs: Any,
        ) -> ChatResult:
            captured.append(list(messages))
            return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)

    agent = build_agent(
        model=Recorder.with_script([AIMessage(content="ok")]),
        tools=[],
        system_prompt="base prompt",
        subagents=[],
        checkpointer=None,
        permissions=[],
        interrupt_on={},
        skills=[MAIN_SKILLS_SOURCE],
    )

    async def claim() -> bool:
        return True

    terminal = await invoke_once(
        RunEmitter(MemoryStream(), "rn"),
        agent,
        "t1",
        {
            "messages": [HumanMessage(content="hi")],
            # state 档官方口径：授权包随首 invoke files 注入。
            "files": {
                f"{MAIN_SKILLS_SOURCE}style/SKILL.md": create_file_data(
                    fm("style", "输出末尾带 via-skill 标记")
                    + "全文正文只在按需读取时可见 BODY-SENTINEL"
                ),
            },
        },
        approval_tool_names=frozenset(),
        source_for=lambda _name: "built-in",
        claim_terminal=claim,
        record_usage=usage_recorder()[0],
    )
    assert terminal is True
    system_text = "\n".join(m.text for m in captured[-1] if m.type == "system")
    assert "输出末尾带 via-skill 标记" in system_text  # description 上 prompt
    assert "BODY-SENTINEL" not in system_text  # 正文绝不进 prompt（渐进披露）
    assert "base prompt" in system_text
