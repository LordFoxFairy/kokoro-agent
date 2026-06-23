"""权限策略：从 config/approval_policy.yaml 加载需审批/计划态禁用的工具集。"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class ApprovalPolicy(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    # Field(strict=False)：只在此字段允许 list → frozenset 的 yaml 解析 coercion；
    # 元素类型 _NonEmpty 由 pydantic 在字段级校验空工具名。
    requires_approval_tools: Annotated[frozenset[_NonEmpty], Field(strict=False)]


def load_approval_policy(path: Path) -> ApprovalPolicy:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return ApprovalPolicy.model_validate(raw)


@lru_cache(maxsize=1)
def approval_policy() -> ApprovalPolicy:
    # 配置位于包根：kokoro_agent/config/approval_policy.yaml。
    path = Path(__file__).resolve().parents[2] / "config" / "approval_policy.yaml"
    return load_approval_policy(path)
