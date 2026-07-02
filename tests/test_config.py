"""AppConfig 单一配置入口：默认值、env 解析、approval env 迁移、边界校验。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from kokoro_agent.tools.names import EXECUTE_TOOL_NAME
from kokoro_agent.config import AppConfig, ApprovalPolicy

_DEFAULT_APPROVAL_TOOLS = {EXECUTE_TOOL_NAME}


def test_empty_env_yields_defaults() -> None:
    config = AppConfig.from_env({})

    assert config.model.provider == "anthropic"
    assert config.model.model_name == "claude-sonnet-4-6"
    assert config.model.disable_streaming is False
    assert config.stream.backend == "memory"
    assert config.stream.redis_url == "redis://127.0.0.1:6379/0"
    assert config.observability.langfuse_configured is False
    assert config.approval.requires_approval_tools == frozenset(_DEFAULT_APPROVAL_TOOLS)
    assert config.local_fake_model is False
    assert config.checkpoint.backend == "sqlite"
    assert config.checkpoint.db_path == "kokoro_checkpoints.db"
    assert config.run_state.backend == "sqlite"
    assert config.run_state.db_path == "kokoro_run_state.db"
    assert config.mongo.url == "mongodb://127.0.0.1:27017"
    assert config.mongo.db == "kokoro"
    assert config.runtime.backend == "state"
    assert config.runtime.skills == ()
    assert config.runtime.memory == ()
    assert config.runtime.local_shell_root is None
    assert config.runtime.local_shell_inherit_env is False
    assert config.runtime.local_shell_timeout == 120
    assert config.runtime.local_shell_max_output_bytes == 100000


def test_runtime_from_env() -> None:
    config = AppConfig.from_env(
        {
            "KOKORO_AGENT_BACKEND": "LOCAL_SHELL",
            "KOKORO_AGENT_SKILLS": "/skills/user,/skills/project",
            "KOKORO_AGENT_MEMORY": "/memory/AGENTS.md",
            "KOKORO_AGENT_LOCAL_SHELL_ROOT": "/tmp/kokoro-agent",
            "KOKORO_AGENT_LOCAL_SHELL_INHERIT_ENV": "1",
            "KOKORO_AGENT_LOCAL_SHELL_TIMEOUT": "30",
            "KOKORO_AGENT_LOCAL_SHELL_MAX_OUTPUT_BYTES": "4096",
        }
    )

    assert config.runtime.backend == "local_shell"
    assert config.runtime.skills == ("/skills/user", "/skills/project")
    assert config.runtime.memory == ("/memory/AGENTS.md",)
    assert config.runtime.local_shell_root == "/tmp/kokoro-agent"
    assert config.runtime.local_shell_inherit_env is True
    assert config.runtime.local_shell_timeout == 30
    assert config.runtime.local_shell_max_output_bytes == 4096


def test_runtime_rejects_unknown_backend() -> None:
    with pytest.raises(ValueError, match="unknown KOKORO_AGENT_BACKEND"):
        AppConfig.from_env({"KOKORO_AGENT_BACKEND": "e2b"})


def test_mongo_from_env() -> None:
    config = AppConfig.from_env(
        {"KOKORO_MONGO_URL": "mongodb://h:1", "KOKORO_MONGO_DB": "prod"}
    )
    assert config.mongo.url == "mongodb://h:1"
    assert config.mongo.db == "prod"


def test_run_state_from_env() -> None:
    config = AppConfig.from_env(
        {"KOKORO_RUN_STATE_BACKEND": "MONGO", "KOKORO_RUN_STATE_DB": "/tmp/rs.db"}
    )
    assert config.run_state.backend == "mongo"
    assert config.run_state.db_path == "/tmp/rs.db"


def test_checkpoint_from_env() -> None:
    config = AppConfig.from_env(
        {"KOKORO_CHECKPOINT_BACKEND": "MEMORY", "KOKORO_CHECKPOINT_DB": "/tmp/x.db"}
    )
    assert config.checkpoint.backend == "memory"
    assert config.checkpoint.db_path == "/tmp/x.db"


def test_stream_redis_from_env() -> None:
    config = AppConfig.from_env(
        {"KOKORO_STREAM_BACKEND": "REDIS", "KOKORO_REDIS_URL": "redis://h:1/2"}
    )
    assert config.stream.backend == "redis"
    assert config.stream.redis_url == "redis://h:1/2"


def test_observability_configured_requires_both_keys() -> None:
    assert AppConfig.from_env({"LANGFUSE_PUBLIC_KEY": "pk"}).observability.langfuse_configured is False
    both = AppConfig.from_env({"LANGFUSE_PUBLIC_KEY": "pk", "LANGFUSE_SECRET_KEY": "sk"})
    assert both.observability.langfuse_configured is True
    assert both.observability.langfuse_secret_key is not None
    assert both.observability.langfuse_secret_key.get_secret_value() == "sk"


def test_local_fake_flag() -> None:
    assert AppConfig.from_env({"KOKORO_LOCAL_FAKE_MODEL": "1"}).local_fake_model is True
    assert AppConfig.from_env({"KOKORO_LOCAL_FAKE_MODEL": "0"}).local_fake_model is False


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("external_action", {"external_action"}),
        ("a,b,c", {"a", "b", "c"}),
        (" a , b ,, c ", {"a", "b", "c"}),  # 去空白 + 丢空段
        ("", _DEFAULT_APPROVAL_TOOLS),  # 空串视同未设 → 回退默认
    ],
)
def test_approval_tools_from_env(raw: str, expected: set[str]) -> None:
    config = AppConfig.from_env({"KOKORO_REQUIRES_APPROVAL_TOOLS": raw})
    assert config.approval.requires_approval_tools == frozenset(expected)


def test_approval_policy_rejects_blank_tool() -> None:
    with pytest.raises(ValidationError):
        ApprovalPolicy.model_validate({"requires_approval_tools": [""]})


def test_approval_policy_rejects_unknown_keys() -> None:
    with pytest.raises(ValidationError):
        ApprovalPolicy.model_validate({"requires_approval_tools": ["x"], "rogue": True})


def test_app_config_rejects_unknown_keys() -> None:
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"rogue": 1})
