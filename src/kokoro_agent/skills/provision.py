"""skills 供给（Skills V2）：把本次授权的 skill 包物化进 run 的 backend。

消费在图内由 deepagents 原生 SkillsMiddleware 承担（渐进披露：prompt 只挂
name+description，agent 用到才 read_file 全文）。wire subagents 已 names 化，
per-subagent 技能包随 wire 定义路径退役；供给面只剩主 agent 的 MAIN_SKILLS_SOURCE。
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass

from typing import Protocol

from deepagents.backends.protocol import FileData, FileUploadResponse
from deepagents.backends.utils import create_file_data

from kokoro_agent.skills.package import SkillLibrary
from kokoro_agent.skills.supply import MAIN_SKILLS_SOURCE
from kokoro_agent.contract import RuntimeConfig


@dataclass(frozen=True, slots=True)
class ProvisionedSkills:
    # 主 agent 的 SkillsMiddleware 源（无授权=空，不挂空中间件面）。
    sources: tuple[str, ...]
    # state 档（backend=None）：deepagents 官方口径经 invoke files 注入初始状态
    # （值为 FileData 结构）；真实 backend 已直接 upload，此处恒空。
    initial_files: Mapping[str, FileData]


def _granted_files(runtime: RuntimeConfig, skills: SkillLibrary) -> dict[str, str]:
    grants: dict[str, tuple[str, ...]] = {}
    if runtime.skills:
        grants[MAIN_SKILLS_SOURCE] = tuple(dict.fromkeys(runtime.skills))
    files: dict[str, str] = {}
    for prefix, names in grants.items():
        for name in names:
            package = skills.get(name)  # 未知名 fail-loud（库快照即授权目录）
            for rel, content in package.files.items():
                files[f"{prefix}{name}/{rel}"] = content
    return files


class UploadCapableBackend(Protocol):
    """供给只依赖 upload_files 能力面（BackendProtocol 全家桶结构化满足）。"""

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]: ...


async def provision_skills(
    runtime: RuntimeConfig, skills: SkillLibrary, backend: UploadCapableBackend | None
) -> ProvisionedSkills:
    """物化授权包：state 档走 invoke files；真实 backend 走其 upload_files（幂等覆盖，
    resume/重拾重供无害）。"""
    files = _granted_files(runtime, skills)
    sources = (MAIN_SKILLS_SOURCE,) if runtime.skills else ()
    if not files:
        return ProvisionedSkills(sources=(), initial_files={})
    if backend is None:
        return ProvisionedSkills(
            sources=sources,
            initial_files={path: create_file_data(content) for path, content in files.items()},
        )
    payload = [(path, content.encode("utf-8")) for path, content in sorted(files.items())]
    await asyncio.to_thread(backend.upload_files, payload)
    return ProvisionedSkills(sources=sources, initial_files={})
