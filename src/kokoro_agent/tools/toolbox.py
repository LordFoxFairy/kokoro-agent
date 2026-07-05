"""进程级内置工具箱：恒挂底座工具的唯一出口。

内置工具按"参数来自哪根轴"分三类，各归各位：
  无参常量（ask_user）           → registry 常量，类型工厂作 core_tools 恒挂
  进程配置态（web_search/web_fetch）→ 启动构建一次入箱（配置缺失即缺席，不挂空壳）
  租户态（memory：namespace 隔离）  → 装配期由箱按 run 实例化
wire 点名的注册表工具与 MCP 外接工具不在箱内——它们由 wire 逐 run 决定。
"""

from __future__ import annotations

from dataclasses import dataclass

from langchain_core.tools import BaseTool

from kokoro_agent.tools.memory import make_memory_tools
from kokoro_agent.tools.web_fetch import make_web_fetch_tool
from kokoro_agent.tools.web_search import (
    SearchProviderSettings,
    make_search_provider,
    make_web_search_tool,
)


@dataclass(frozen=True, slots=True)
class ProcessToolbox:
    """worker 启动构建一次，逐请求出恒挂底座。"""

    # 进程配置态工具（当前=联网工具 web_search/web_fetch；web=互联网，非 kokoro-web）。
    configured: tuple[BaseTool, ...]

    def tools_for(self, namespace: str) -> tuple[BaseTool, ...]:
        """恒挂底座 = 租户态记忆工具（隔离在此注入，工具体不含租户概念）+ 配置态工具。"""
        return (*make_memory_tools(namespace), *self.configured)


def build_toolbox(
    *, fetch_allow_private: bool, search: SearchProviderSettings | None
) -> ProcessToolbox:
    tools: list[BaseTool] = [make_web_fetch_tool(allow_private=fetch_allow_private)]
    # search 配置即挂载：无 provider 不挂空壳。
    if search is not None:
        tools.append(make_web_search_tool(make_search_provider(search)))
    return ProcessToolbox(configured=tuple(tools))
