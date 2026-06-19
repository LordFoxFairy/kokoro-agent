"""权限策略：从 config/approval_policy.yaml 加载需审批/计划态禁用的工具集。"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import BaseModel, ConfigDict, StringConstraints

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class _ApprovalPolicyPayload(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    requires_approval_tools: list[_NonEmpty]
    plan_only_blocked_tools: list[_NonEmpty]


class ApprovalPolicy(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    requires_approval_tools: frozenset[str]
    plan_only_blocked_tools: frozenset[str]


def load_approval_policy(path: Path) -> ApprovalPolicy:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload = _ApprovalPolicyPayload.model_validate(raw)
    return ApprovalPolicy(
        requires_approval_tools=frozenset(payload.requires_approval_tools),
        plan_only_blocked_tools=frozenset(payload.plan_only_blocked_tools),
    )


@lru_cache(maxsize=1)
def approval_policy() -> ApprovalPolicy:
    # 配置位于包根：kokoro_agent/config/approval_policy.yaml。
    path = Path(__file__).resolve().parents[2] / "config" / "approval_policy.yaml"
    return load_approval_policy(path)
