from __future__ import annotations

from pathlib import Path

import pytest

from kokoro_agent.infrastructure.permission import (
    ApprovalPolicy,
    approval_policy,
    load_approval_policy,
)


def test_load_approval_policy_from_yaml_file() -> None:
    policy = load_approval_policy(
        Path("src/kokoro_agent/config/approval_policy.yaml")
    )

    assert policy == ApprovalPolicy(
        requires_approval_tools=frozenset({"fetch_url"}),
    )


def test_approval_policy_is_cached_singleton() -> None:
    assert approval_policy() is approval_policy()


def test_load_approval_policy_rejects_unknown_keys(tmp_path: Path) -> None:
    path = tmp_path / "approval-policy.yaml"
    path.write_text(
        "requires_approval_tools:\n  - fetch_url\nrogue: true\n",
        encoding="utf-8",
    )

    with pytest.raises(Exception):
        load_approval_policy(path)
