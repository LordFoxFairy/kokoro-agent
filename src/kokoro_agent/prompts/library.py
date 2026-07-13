"""agent prompt 库：部署快照优先于内置包资源（kokoro_agent/prompts/<name>.md 随包出厂）。"""

from __future__ import annotations

from collections.abc import Mapping
from importlib.resources import files


class PromptLibrary:
    """按名取 agent prompt 全文；未知名返回 None（调用方决定兜底/报错）。"""

    def __init__(self, extra: Mapping[str, str]) -> None:
        self._extra = dict(extra)

    def get(self, name: str) -> str | None:
        hit = self._extra.get(name)
        if hit is not None:
            return hit
        resource = files("kokoro_agent.prompts").joinpath(f"{name}.md")
        if resource.is_file():
            return resource.read_text(encoding="utf-8").strip()
        return None

    def names(self) -> tuple[str, ...]:
        """部署配置的 persona 名全集（handoff 候选源，与 AGENT-PRESET 同源=目录即配置）。

        只数部署快照（personas_dir/S3 装载进的 _extra）：内置包缺省（general 等）是类型末级
        回退，不算 swarm 候选——默认部署无 personas_dir 即零候选，单人格链路透明不挂 handoff。
        名序稳定（排序）：候选清单/handoff 描述字节恒定，前缀可测。
        """
        return tuple(sorted(self._extra))
