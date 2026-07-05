"""skills 资产库：启动快照（name → SKILL.md 全文），按名渲染进 system prompt。

库形态与 CC plugins 同构（<name>/SKILL.md）。进程期内容恒定：更新资产=滚动重启，
运行期渲染零文件/网络依赖——多 pod 一致性由共享资产源（s3 档）在装载侧保证。

state/e2b 等虚拟 backend 下 deepagents 渐进披露（模型 read_file 宿主路径）读不到
SKILL.md（实证），V1 采用 backend 无关的全文注入；沙箱供给落地后可回归原生机制。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

SKILL_MAX_CHARS = 32_000


class SkillAssetError(Exception):
    pass


class SkillLibrary:
    """不可变快照：空源=空库（用到未知名才报错）。"""

    def __init__(self, contents: Mapping[str, str]) -> None:
        self._contents = dict(contents)

    def names(self) -> frozenset[str]:
        return frozenset(self._contents)

    def render_prompt(self, names: Sequence[str]) -> str | None:
        """按名渲染 skills 段（首见去重保序）；未知名/超限 fail-loud；空单返回 None。"""
        sections: list[str] = []
        seen: set[str] = set()
        for name in names:
            if name in seen:
                continue
            seen.add(name)
            content = self._contents.get(name)
            if content is None:
                raise SkillAssetError(
                    f"skill {name!r} not in library (available: {sorted(self._contents)})"
                )
            if len(content) > SKILL_MAX_CHARS:
                raise SkillAssetError(
                    f"skill {name!r}: SKILL.md exceeds {SKILL_MAX_CHARS} chars ({len(content)})"
                )
            sections.append(f"### Skill: {name}\n\n{content.strip()}")
        if not sections:
            return None
        return "## Skills\n\n遵循以下技能的指引完成对应任务。\n\n" + "\n\n".join(sections)
