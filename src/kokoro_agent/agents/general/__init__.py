"""通用 agent 成品：Kokoro 缺省主 agent（session 入口表的内建 general 引用此身份）。

成品包结构约定：每个对外 agent 一个子包，人格资源（persona.md）与定义同居本包；
未来的成品资产（默认技能清单等）也归各自包内。
"""

from __future__ import annotations

from importlib.resources import files

from kokoro_agent.agents.product import AgentProduct


def _load_persona() -> str:
    return files("kokoro_agent.agents.general").joinpath("persona.md").read_text(encoding="utf-8").strip()


GENERAL_AGENT = AgentProduct(
    name="general",
    description="通用协调 agent：组装并调度本空间全部能力。",
    persona=_load_persona(),
)
