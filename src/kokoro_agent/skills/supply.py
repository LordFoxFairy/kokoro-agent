"""skills 供给布局（backend 内虚拟路径）：主 agent 与各子代理分前缀——授权面互不越界。"""

from __future__ import annotations

# 点前缀：skills 是能力供给不是用户产物——session 文件清单与 S3 归档按隐藏目录跳过。
MAIN_SKILLS_SOURCE = "/.skills/main/"


def subagent_skills_source(name: str) -> str:
    return f"/.skills/sub-{name}/"
