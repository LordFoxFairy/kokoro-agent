"""装配域（assembly）：把 wire 的 RunRequest + 进程级依赖装配成可运行 agent。

命名注记：不叫 orchestration——那个词留给未来的多 agent 运行时调度（swarm 域）；
这里做的是装配（parts.py 共享装配件 + <type>.py 各类型配方，现有 general）。
"""

from kokoro_agent.assembly.general import assemble_general
from kokoro_agent.assembly.parts import (
    AssembleDeps,
    AssembledAgent,
    approval_names,
    build_web_tools,
)

__all__ = [
    "AssembleDeps",
    "AssembledAgent",
    "approval_names",
    "assemble_general",
    "build_web_tools",
]
