"""通用 agent 成品：Kokoro 缺省主 agent 的人格（session 入口表的内建 general 引用之）。

成品包结构约定：每个对外 agent 一个子包，人格资源（persona.md）随包分发；
名字/描述等入口元数据活在 session 入口表（wire 数据），不在本仓重复维护。
"""

from __future__ import annotations

from importlib.resources import files

GENERAL_PERSONA: str = (
    files("kokoro_agent.agents.general").joinpath("persona.md").read_text(encoding="utf-8").strip()
)
