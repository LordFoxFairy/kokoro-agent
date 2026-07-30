"""统一配置树规格（ADR-010）：yaml 域分组生效、env 覆盖、凭据禁入、未知键 fail-loud。"""

from __future__ import annotations

from pathlib import Path

import pytest

from kokoro_agent.config import AppConfig
from kokoro_agent.config_file import load_config_file

FULL_TREE = """\
model:
  local_fake: true
  local_fake_script: hitl
stream:
  redis_url: redis://127.0.0.1:6379/5
mongo:
  url: mongodb://127.0.0.1:27017
  db: kokoro_tree
ledger:
  lease_ttl_s: 90
sandbox:
  local_shell:
    root: /data/ws
    timeout: 60
  docker:
    image: python:3.12-slim
    ttl: 900
  custom:
    factory: my_pkg.sandbox:make_backend
web_tools:
  fetch_allow_private: true
  search:
    provider: searxng
    url: http://127.0.0.1:8080
subagents:
  builtin: [researcher, coder]
limits:
  recursion_limit: 50
retention:
  run_ttl_s: 0
"""


def _config_from(tmp_path: Path, tree: str, extra_env: dict[str, str] | None = None) -> AppConfig:
    file = tmp_path / "kokoro-agent.yaml"
    file.write_text(tree)
    return AppConfig.from_env({"KOKORO_AGENT_CONFIG": str(file), **(extra_env or {})})


class TestConfigTree:
    def test_full_tree_lands_in_all_domains(self, tmp_path: Path) -> None:
        config = _config_from(tmp_path, FULL_TREE)
        assert config.model.local_fake is True
        assert config.model.local_fake_script == "hitl"
        assert config.stream.redis_url == "redis://127.0.0.1:6379/5"
        assert config.ledger.lease_ttl_ms == 90_000
        assert config.ledger.mongo_db == "kokoro_tree"
        assert config.sandbox.local_shell_root == "/data/ws"
        assert config.sandbox.local_shell_timeout == 60
        assert (config.sandbox.docker.image, config.sandbox.docker.ttl) == ("python:3.12-slim", 900)
        assert config.sandbox.custom.factory_ref == "my_pkg.sandbox:make_backend"
        assert config.web_tools.fetch_allow_private is True
        assert config.web_tools.search_provider == "searxng"
        assert config.enabled_builtin_subagents == frozenset({"researcher", "coder"})
        assert config.recursion_limit == 50
        assert config.retention_run_ttl_s == 0

    def test_env_overrides_yaml(self, tmp_path: Path) -> None:
        config = _config_from(
            tmp_path, FULL_TREE,
            {"KOKORO_RECURSION_LIMIT": "77", "KOKORO_DOCKER_IMAGE": "busybox"},
        )
        assert config.recursion_limit == 77
        assert config.sandbox.docker.image == "busybox"
        assert config.sandbox.docker.ttl == 900  # 未覆盖键保持 yaml 值

    def test_no_file_means_pure_env_behaviour(self) -> None:
        assert AppConfig.from_env({}) == AppConfig.from_env({"KOKORO_AGENT_CONFIG": ""})

    def test_destructive_durable_output_retention_requires_consumer_ack(self) -> None:
        with pytest.raises(
            ValueError, match="DURABLE_OUTPUT_RETENTION_REQUIRES_CONSUMER_ACK"
        ):
            AppConfig.from_env({"KOKORO_RETENTION_RUN_TTL_S": "1"})

    @pytest.mark.parametrize(
        ("tree", "match"),
        [
            ("model:\n  openai_api_key: sk-leak\n", "credentials are env-only"),
            ("unknown_domain:\n  x: 1\n", "credentials are env-only"),
            ("sandbox:\n  local_shell:\n    typo_key: 1\n", "typo_key"),
            ("- a list\n", "mapping"),
        ],
    )
    def test_fail_loud_matrix(self, tmp_path: Path, tree: str, match: str) -> None:
        file = tmp_path / "kokoro-agent.yaml"
        file.write_text(tree)
        with pytest.raises((KeyError, TypeError), match=match):
            AppConfig.from_env({"KOKORO_AGENT_CONFIG": str(file)})

    def test_missing_file_fail_loud(self) -> None:
        with pytest.raises(OSError):
            AppConfig.from_env({"KOKORO_AGENT_CONFIG": "/nonexistent/agent.yaml"})

    def test_bool_false_and_zero_preserved(self, tmp_path: Path) -> None:
        # 原生值保留：false/0 不被 falsy 丢弃，交 AppConfig 的 pydantic 统一 coerce。
        flat = load_config_file(None)
        assert flat == {}
        file = tmp_path / "a.yaml"
        file.write_text("web_tools:\n  fetch_allow_private: false\nlimits:\n  run_token_budget: 0\n")
        flat = load_config_file(str(file))
        assert flat == {"KOKORO_WEB_FETCH_ALLOW_PRIVATE": False, "KOKORO_RUN_TOKEN_BUDGET": 0}


class TestExamplesStayValid:
    """config/examples 防漂移：example 键漂出映射表即此处爆炸。"""

    EXAMPLES = Path(__file__).parents[2] / "config" / "examples"

    @pytest.mark.skipif(not EXAMPLES.exists(), reason="parent-repo examples not present")
    def test_agent_full_example_loads(self) -> None:
        flat = load_config_file(str(self.EXAMPLES / "agent.example.full.yaml"))
        assert flat["KOKORO_DOCKER_IMAGE"] == "python:3.12-slim"
        assert flat["KOKORO_CUSTOM_BACKEND"] == "my_pkg.sandbox:make_backend"
        # 原生 yaml 列表直接落座（不再 CSV stringify）；AppConfig 再收窄成 frozenset。
        assert flat["KOKORO_BUILTIN_SUBAGENTS"] == ["researcher"]
