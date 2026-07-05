"""skills 资产库：目录扫描建清单（启动即锁）+ 按名渲染全文进 system prompt。

库形态与 CC plugins 同构：{KOKORO_SKILLS_DIR}/<name>/SKILL.md。启动扫描计算内容
sha256 入清单；装配期按名取用并复核内容锁——运行期文件被篡改即 fail-loud。
配置与 wire 只传名称，路径/锁全归库内部。

state/e2b 等虚拟 backend 下 deepagents 渐进披露（模型 read_file 宿主路径）读不到
SKILL.md（实证），V1 采用 backend 无关的全文注入；沙箱供给落地后可回归原生机制。
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

SKILL_MAX_CHARS = 32_000


class SkillMountError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class SkillAsset:
    name: str
    path: Path
    # 启动时锁定的内容摘要：装配期复核，篡改即 fail-loud。
    lock: str


class SkillLibrary:
    """不可变清单：worker 启动扫描一次；空目录/未配置=空库（用到未知名才报错）。"""

    def __init__(self, assets: dict[str, SkillAsset]) -> None:
        self._assets = assets

    def names(self) -> frozenset[str]:
        return frozenset(self._assets)

    def render_prompt(self, names: Sequence[str]) -> str | None:
        """按名渲染 skills 段（首见去重保序）；未知名/锁不符/超限 fail-loud；空单返回 None。"""
        sections: list[str] = []
        seen: set[str] = set()
        for name in names:
            if name in seen:
                continue
            seen.add(name)
            asset = self._assets.get(name)
            if asset is None:
                raise SkillMountError(
                    f"skill {name!r} not in library (available: {sorted(self._assets)})"
                )
            raw = (asset.path / "SKILL.md").read_bytes()
            digest = hashlib.sha256(raw).hexdigest()
            if digest != asset.lock:
                raise SkillMountError(
                    f"skill {name!r}: content changed since worker start "
                    f"(expected {asset.lock}, got {digest})"
                )
            content = raw.decode("utf-8")
            if len(content) > SKILL_MAX_CHARS:
                raise SkillMountError(
                    f"skill {name!r}: SKILL.md exceeds {SKILL_MAX_CHARS} chars ({len(content)})"
                )
            sections.append(f"### Skill: {name}\n\n{content.strip()}")
        if not sections:
            return None
        return "## Skills\n\n遵循以下技能的指引完成对应任务。\n\n" + "\n\n".join(sections)


def load_skill_library(root: str | None) -> SkillLibrary:
    """扫描 {root}/<name>/SKILL.md 建清单；缺 SKILL.md 的目录 fail-loud（半成品即配置错误）。"""
    if root is None or root == "":
        return SkillLibrary({})
    base = Path(root)
    if not base.is_dir():
        raise SkillMountError(f"KOKORO_SKILLS_DIR {root!r} is not a directory")
    assets: dict[str, SkillAsset] = {}
    for child in sorted(base.iterdir()):
        if not child.is_dir() or child.name.startswith("."):
            continue
        skill_md = child / "SKILL.md"
        if not skill_md.is_file():
            raise SkillMountError(f"skill dir {child} has no SKILL.md")
        lock = hashlib.sha256(skill_md.read_bytes()).hexdigest()
        assets[child.name] = SkillAsset(name=child.name, path=child, lock=lock)
    return SkillLibrary(assets)
