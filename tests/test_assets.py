"""资产域规格：local/s3 资产源快照装载 + skills 渲染 + 配置矩阵（凭据 env-only）。

minio 不可达时 s3 实测组干净 skip；配置矩阵与 local 源不依赖外部服务，恒跑。
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

from fakes import usage_recorder
from kokoro_agent.assets import (
    AssetSettings,
    AssetSourceError,
    LocalAssets,
    LocalAssetSource,
    S3Assets,
    S3AssetSource,
    SkillAssetError,
    SkillLibrary,
    load_asset_libraries,
    load_assets_config,
)
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.streams.memory import MemoryStream

MINIO_URL = "http://127.0.0.1:9100"
BUCKET = f"kokoro-assets-test-{int(time.time())}"


def _local_skills(tmp_path: Path, skills: dict[str, str]) -> SkillLibrary:
    for name, content in skills.items():
        d = tmp_path / name
        d.mkdir()
        (d / "SKILL.md").write_text(content)
    source = LocalAssetSource(LocalAssets(type="local", skills_dir=str(tmp_path)))
    return SkillLibrary(source.load_skills())


# --- local 源与库语义 ---


def test_unconfigured_source_is_empty_library() -> None:
    skills, personas = load_asset_libraries(
        AssetSettings(
            source=LocalAssets(type="local"), s3_access_key=None, s3_secret_key=None
        )
    )
    assert skills.names() == frozenset()
    assert skills.render_prompt([]) is None
    assert personas.get("ghost") is None


def test_renders_full_content_in_order(tmp_path: Path) -> None:
    library = _local_skills(tmp_path, {"alpha": "A 内容", "beta": "B 内容"})
    prompt = library.render_prompt(["beta", "alpha"])
    assert prompt is not None
    assert prompt.index("### Skill: beta") < prompt.index("### Skill: alpha")
    assert "A 内容" in prompt and "B 内容" in prompt


def test_dedupe_by_name_preserves_first(tmp_path: Path) -> None:
    library = _local_skills(tmp_path, {"alpha": "A"})
    prompt = library.render_prompt(["alpha", "alpha"])
    assert prompt is not None
    assert prompt.count("### Skill: alpha") == 1


def test_unknown_name_fails_loud(tmp_path: Path) -> None:
    library = _local_skills(tmp_path, {"alpha": "A"})
    with pytest.raises(SkillAssetError, match="ghost"):
        library.render_prompt(["ghost"])


def test_snapshot_ignores_post_start_edits(tmp_path: Path) -> None:
    # 快照语义：装载后盘上被改，渲染仍是装载时内容——运行期内容恒定，改资产=滚动重启。
    library = _local_skills(tmp_path, {"alpha": "原文"})
    (tmp_path / "alpha" / "SKILL.md").write_text("被篡改")
    prompt = library.render_prompt(["alpha"])
    assert prompt is not None
    assert "原文" in prompt and "被篡改" not in prompt


def test_dir_without_skill_md_fails_loud(tmp_path: Path) -> None:
    (tmp_path / "broken").mkdir()
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


def test_oversized_skill_fails_loud(tmp_path: Path) -> None:
    library = _local_skills(tmp_path, {"big": "x" * 33_000})
    with pytest.raises(SkillAssetError, match="exceeds"):
        library.render_prompt(["big"])


# --- 配置矩阵（type 判别 yaml + 凭据 env-only） ---


def test_assets_config_local_yaml(tmp_path: Path) -> None:
    path = tmp_path / "assets.yaml"
    path.write_text("assets:\n  type: local\n  skills_dir: /data/skills\n")
    config = load_assets_config(str(path))
    assert isinstance(config, LocalAssets)
    assert config.skills_dir == "/data/skills"
    assert config.personas_dir is None


def test_assets_config_s3_yaml_with_defaults(tmp_path: Path) -> None:
    path = tmp_path / "assets.yaml"
    path.write_text(
        "assets:\n  type: s3\n  endpoint: http://127.0.0.1:9100\n  bucket: kokoro-assets\n"
    )
    config = load_assets_config(str(path))
    assert isinstance(config, S3Assets)
    assert config.region == "us-east-1"
    assert config.force_path_style is True
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
    # 凭据键/未知键/未知 type 一律拦截：凭据 env-only 是设计红线。
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

    def test_loads_skills_and_personas_under_prefix(self) -> None:
        self._put("deploy/skills/style/SKILL.md", "s3 技能全文")
        self._put("deploy/skills/style/extra.md", "附属文件不入库")
        self._put("deploy/personas/poet.md", "s3 诗人人格\n")
        source = S3AssetSource(
            S3Assets(
                type="s3", endpoint=MINIO_URL, bucket=BUCKET, prefix="deploy"
            ),
            access_key=SecretStr("kokoro"),
            secret_key=SecretStr("kokoro-secret"),
        )
        assert dict(source.load_skills()) == {"style": "s3 技能全文"}
        assert dict(source.load_personas()) == {"poet": "s3 诗人人格"}

    def test_skill_without_skill_md_fails_loud(self) -> None:
        self._put("broken/skills/ghost/notes.txt", "无 SKILL.md")
        source = S3AssetSource(
            S3Assets(
                type="s3", endpoint=MINIO_URL, bucket=BUCKET, prefix="broken"
            ),
            access_key=SecretStr("kokoro"),
            secret_key=SecretStr("kokoro-secret"),
        )
        with pytest.raises(AssetSourceError, match="no SKILL.md"):
            source.load_skills()

    def test_load_asset_libraries_end_to_end(self) -> None:
        self._put("e2e/skills/tone/SKILL.md", "结尾输出 via-s3-skill")
        self._put("e2e/personas/muse.md", "缪斯人格")
        skills, personas = load_asset_libraries(
            AssetSettings(
                source=S3Assets(
                    type="s3", endpoint=MINIO_URL, bucket=BUCKET, prefix="e2e"
                ),
                s3_access_key=SecretStr("kokoro"),
                s3_secret_key=SecretStr("kokoro-secret"),
            )
        )
        prompt = skills.render_prompt(["tone"])
        assert prompt is not None and "via-s3-skill" in prompt
        assert personas.get("muse") == "缪斯人格"


# --- skills 全文注入真图（backend 无关的 V1 正解） ---


@pytest.mark.asyncio
async def test_skill_body_reaches_model_system_prompt(tmp_path: Path) -> None:
    # state backend 下 deepagents 渐进披露读不到宿主 SKILL.md（实证），全文注入是 V1 正解：
    # 断言真图里模型收到的 system prompt 含 skill 全文。
    library = _local_skills(tmp_path, {"style": "自我介绍末尾输出 via-skill:v1"})
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

    prompt = library.render_prompt(["style"])
    assert prompt is not None
    agent = build_agent(
        model=Recorder.with_script([AIMessage(content="ok")]),
        tools=[],
        system_prompt=f"base\n\n{prompt}",
        subagents=[],
        checkpointer=None,
        permissions=[],
        interrupt_on={},
    )

    async def claim() -> bool:
        return True

    terminal = await invoke_once(
        RunEmitter(MemoryStream(), "rn"),
        agent,
        "t1",
        {"messages": [HumanMessage(content="hi")]},
        approval_tool_names=frozenset(),
        source_for=lambda _name: "built-in",
        claim_terminal=claim,
        record_usage=usage_recorder()[0],
    )
    assert terminal is True
    # .text 是框架的文本收窄口（content 联合 → str），不自拆 content 块。
    system_text = "\n".join(
        message.text for message in captured[-1] if message.type == "system"
    )
    assert "via-skill:v1" in system_text
